package fs

import (
	"fmt"
	"log"
	pb "my-fs/proto"
	"my-fs/utils"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
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
	mutx       sync.Mutex // 用于保护fileManager维护的cp
	writer     *fileWriter
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
	// 读取最后保存的checkpoint
	cp, err := fs.loadCheckpoint()
	if err != nil {
		return nil, err
	}
	// checkpoint不存在，初始化
	if cp == nil {
		log.Println("construct checkpoint from file storage")
		if cp, err = constructCheckpointFromFiles(fileStorePath); err != nil {
			return nil, err
		}
	} else {
		// 否则读取
		if err := syncCheckpointFromFS(fileStorePath, cp); err != nil {
			return nil, err
		}
	}
	// 保存checkpoint
	err = fs.saveCheckpoint(cp, true)
	if err != nil {
		return nil, err
	}
	fs.checkpoint = cp
	// 利用checkpoint中的数据生成writer
	fWriter, err := newFileWriter(buildFilePath(fileStorePath, cp.lastFileSeq))
	if err != nil {
		return nil, err
	}
	fs.writer = fWriter
	// 修剪不完整的数据
	if err = fs.writer.truncate(cp.lastFileSize); err != nil {
		return nil, err
	}

	return fs, nil
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

// loadCheckpoint 从索引数据库中获取保存的checkpoint
func (fm *FileManager) loadCheckpoint() (*checkpoint, error) {
	if fm.indexStore == nil {
		return nil, errors.New("index store is nil")
	}
	return fm.indexStore.FetchCheckpoint()
}

// saveCheckpoint 持久化checkpoint
func (fm *FileManager) saveCheckpoint(cp *checkpoint, sync bool) error {
	if fm.indexStore == nil {
		return errors.New("index store is nil")
	}
	return fm.indexStore.SaveCheckpoint(cp, sync)
}

func (fm *FileManager) updateCheckpoint(cp *checkpoint) {
	fm.mutx.Lock()
	defer fm.mutx.Unlock()

	fm.checkpoint = cp
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

	stream, err := newFileStream(fm.rootDir, index.FSeq, int64(index.Offset))
	if err != nil {
		return nil, err
	}
	defer stream.close()
	chunkBytes, err := stream.scanForNextChunk()
	if err != nil {
		return nil, err
	}
	chunk := new(pb.Chunk)
	if err := proto.Unmarshal(chunkBytes, chunk); err != nil {
		return nil, errors.Wrap(err, "proto unmarshal chunk failed")
	}
	return chunk.Payload, nil
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
	dataLen := len(data)
	encodedDataLen := proto.EncodeVarint(uint64(dataLen))
	totalLenToAppend := dataLen+len(encodedDataLen)
	currentOffset := fm.checkpoint.lastFileSize
	// 写入文件
	// 判断文件是否已经超过最大大小
	if fm.checkpoint.lastFileSize + totalLenToAppend > maxFileSize {
		// 超过大小，重新创建一个文件，并写入数据
		fm.moveToNextFile()
		currentOffset = 0
	}
	// 没有超过，正常写入
	err = fm.writer.write(encodedDataLen, false)
	if err == nil {
		err = fm.writer.write(data, true)
	}
	if err != nil {
		// 出错了，修剪文件
		err1 := fm.truncate(currentOffset)
		if err1 != nil {
			panic(fmt.Sprintf("truncate file failed, err=%s", err1))
		}
		return "", errors.Wrap(err, "write data into file failed")
	}

	// 更新checkpoint
	newCP := &checkpoint{
		lastFileSeq: fm.checkpoint.lastFileSeq,
		lastFileSize: currentOffset + totalLenToAppend,
	}
	if err = fm.saveCheckpoint(newCP, false); err != nil {
		err1 := fm.truncate(currentOffset)
		if err1 != nil {
			panic(fmt.Sprintf("truncate file failed, err=%s", err1))
		}
		return "", errors.Wrap(err, "save checkpoint failed")
	}
	
	// 更新索引
	if err = fm.indexStore.SaveIndex(&BlockIndex{
		FSeq:    fm.checkpoint.lastFileSeq,
		BlockId: id,
		Offset:  uint64(currentOffset),
	}, true); err != nil {
		return "", err
	}
	fm.updateCheckpoint(newCP)
	return id, nil
}

func (fm *FileManager) moveToNextFile() {
	// 更新checkpoint
	newCheckpoint := &checkpoint{
		lastFileSeq:  fm.checkpoint.lastFileSeq + 1,
		lastFileSize: 0,
	}
	// 更新writer
	nextWriter, err := newFileWriter(buildFilePath(fm.rootDir, newCheckpoint.lastFileSeq))
	if err != nil {
		panic(fmt.Sprintf("could not open writer for next file, err=%s", err))
	}
	fm.writer.close()
	fm.writer = nextWriter
	// 保存checkpoint
	fm.saveCheckpoint(newCheckpoint, true)
	fm.updateCheckpoint(newCheckpoint)
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
