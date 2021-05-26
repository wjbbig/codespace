package fs

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"log"
	pb "my-fs/proto"
	"my-fs/utils"
	"os"
	"path/filepath"
)

const (
	maxFileSize = 64 * 1024 * 1024
	filePrefix  = "file_"
)

type FS interface {
	Write([]byte) (string, error)
	Read(string) ([]byte, error)
	Close() error
}

var _ FS = &FileManager{}

type FileManager struct {
	rootDir    string
	checkpoint *checkpoint
	writer     *fileWriter
	fSeq       int        // 文件的序号: file_000001
	offset     uint64     // 当前文件的偏移量
	indexStore IndexStore // 文件索引数据库
}

func NewFileManager(fileStorePath string, indexStorePath string) (*FileManager, error) {
	// 不管存不存在，都创建存储用文件夹
	if _, err := createDirIfMissing(fileStorePath); err != nil {
		return nil, err
	}
	// 创建索引数据库
	indexStore := &indexStore{}
	if err := indexStore.Open(indexStorePath); err != nil {
		return nil, err
	}

	fs := &FileManager{
		rootDir:    fileStorePath,
		indexStore: indexStore,
	}
	// TODO 实现恢复功能
	// 读取最后保存的checkpoint
	cp, err := fs.loadCheckpoint()
	if err != nil {
		return nil, err
	}
	// checkpoint不存在，初始化
	if cp == nil {
		log.Println("construct checkpoint from file storage")
		if cp, err = constructCheckpointFromFiles(fileStorePath); err != nil {

		}
	} else {
		// 否则读取
	}
	// 保存checkpoint
	err = fs.saveCheckpoint(cp)
	if err != nil {
		return nil, err
	}
	fs.checkpoint = cp
	// 利用checkpoint中的数据生成writer

	// 修剪不完整的数据

	// 通过打开文件来初始化其余的值
	if err := fs.openFile(); err != nil {
		return nil, err
	}
	return fs, nil
}

func constructCheckpointFromFiles(rootDir string) (*checkpoint, error) {
	var offsetAfterLastChunk, chunkNums int64
	lastFileSeq, err := retrieveLastFileSuffix(rootDir)
	if err != nil {
		return nil, err
	}
	// 文件夹中没有任何数据文件
	if lastFileSeq == -1 {
		checkpoint := &checkpoint{0, 0}
		return checkpoint, nil
	}

	_, offsetAfterLastChunk, chunkNums, err = scanForLastCompleteChunk(rootDir, lastFileSeq, 0)
	if err != nil {
		return nil, err
	}
	// 最后一个文件中没有任何数据，需要读取倒数第二个文件
	if chunkNums == 0 && lastFileSeq > 0 {
		secondLastFileSeq := lastFileSeq - 1
		_, offsetAfterLastChunk, chunkNums, err = scanForLastCompleteChunk(rootDir, secondLastFileSeq, 0)
		if err != nil {
			return nil, err
		}
	}

	cp := &checkpoint{
		lastFileSeq:  lastFileSeq,
		lastFileSize: int(offsetAfterLastChunk),
	}

	return cp, nil
}

func scanForLastCompleteChunk(rootDir string, seq int, startOffset int64) ([]byte, int64, int64, error) {
	var lastChunkBytes []byte
	var chunkNums int64
	stream, err := newFileStream(rootDir, seq, startOffset)
	if err != nil {
		return nil, 0, 0, err
	}
	defer stream.close()
	var chunkBytes []byte
	for {
		chunkBytes, err = stream.scanForNextChunk()
		if chunkBytes == nil || err != nil {
			break
		}
		lastChunkBytes = chunkBytes
		chunkNums++
	}
	// 如果是ErrUnexpectedEndOfFile，可以不处理，反正之后会裁剪的
	if err == utils.ErrUnexpectedEndOfFile {
		err = nil
	}
	return lastChunkBytes, stream.currentOffset, chunkNums, err
}

// openFile 打开一个文件
func (fm *FileManager) openFile() error {
	fileName := filepath.Join(fm.rootDir, fm.newFileName())
	_, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		// 报错了，fSeq-1
		fm.fSeq--
		return errors.Wrapf(err, "open file failed, file name=%s", fileName)
	}
	// TODO:修复bug
	fm.writer, err = newFileWriter(fileName)

	// 打开了一个新的文件，偏移量置为0
	fm.offset = 0
	return nil
}

// loadCheckpoint 从索引数据库中获取保存的checkpoint
func (fm *FileManager) loadCheckpoint() (*checkpoint, error) {
	if fm.indexStore == nil {
		return nil, errors.New("index store is nil")
	}
	return fm.indexStore.FetchCheckpoint()
}

// saveCheckpoint 持久化checkpoint
func (fm *FileManager) saveCheckpoint(cp *checkpoint) error {
	if fm.indexStore == nil {
		return errors.New("index store is nil")
	}
	return fm.indexStore.SaveCheckpoint(cp)
}

// newFileName 构建一个新的文件名，名字格式为file_000001
func (fm *FileManager) newFileName() string {
	fm.fSeq++
	return buildFileName(fm.fSeq)
}

func buildFileName(seq int) string {
	return filePrefix + fmt.Sprintf("%06d", seq)
}

func buildFilePath(rootDir string, seq int) string {
	return rootDir + "/" + buildFileName(seq)
}

func (fm *FileManager) Read(blockId string) ([]byte, error) {
	index, err := fm.indexStore.FetchIndex(blockId)
	if err != nil {
		return nil, err
	}

	fileName := filepath.Join(fm.rootDir, buildFileName(index.FSeq))
	file, err := os.Open(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	defer file.Close()
	// TODO:修改
	data := make([]byte, 10)
	n, err := file.ReadAt(data, int64(index.Offset))
	if err != nil {
		return nil, errors.Wrap(err, "read data from file failed")
	}
	if n != int(10) {
		return nil, errors.Wrap(err, "data length is not equal to index.datalen")
	}
	if index.BlockId != blockId {
		return nil, fmt.Errorf("blockId is not equal to index, should be %s, but got %s", blockId, index.BlockId)
	}

	block := new(pb.Chunk)
	if err := proto.Unmarshal(data, block); err != nil {
		return nil, errors.Wrap(err, "proto unmarshal block failed")
	}

	return block.Payload, nil
}

func (fm *FileManager) Write(data []byte) (string, error) {
	// 序列化数据
	id := uuid.New().String()
	block := &pb.Chunk{
		Id:      id,
		Payload: data,
	}
	data, err := proto.Marshal(block)
	if err != nil {
		return "", errors.Wrap(err, "marshal block failed")
	}
	// 写入文件
	// 判断文件是否已经超过最大大小
	dataLen := len(data)
	if int(fm.offset)+dataLen > maxFileSize {
		// 超过大小，重新创建一个文件，并写入数据
		if err := fm.moveToNextFile(); err != nil {
			return "", errors.Wrap(err, "move to next file failed")
		}
	}
	// 没有超过，正常写入
	err = fm.writer.write(data, true)
	if err != nil {
		// 写入失败，需要销毁这段数据
		fm.truncate(int(fm.offset))
		return "", errors.Wrap(err, "write data into file failed")
	}
	// 更新索引
	if err = fm.indexStore.SaveIndex(&BlockIndex{
		FSeq:    fm.fSeq,
		BlockId: id,
		Offset:  fm.offset,
	}); err != nil {
		fm.truncate(int(fm.offset))
		return "", err
	}
	fm.offset += uint64(len(data))
	return id, nil
}

func (fm *FileManager) moveToNextFile() error {
	return fm.openFile()
}

// truncate 清理损坏的数据
func (fm *FileManager) truncate(offset int) error {
	return fm.writer.truncate(offset)
}

func (fm *FileManager) Close() error {
	if err := fm.indexStore.Close(); err != nil {
		return err
	}
	return fm.writer.close()
}
