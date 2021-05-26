package fs

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	pb "my-fs/proto"
	"os"
	"path/filepath"
)

const (
	maxFileSize = 64 * 1024 * 1024
)

type FS interface {
	Write([]byte) (string, error)
	Read(string) ([]byte, error)
	Close() error
}

var _ FS = &FileManager{}

type FileManager struct {
	// 当前文件
	file      *os.File
	fileStore string
	reader    *bufio.Reader
	writer    *bufio.Writer
	// 文件的序号: file_000001
	fSeq uint64
	// 当前文件的偏移量
	offset uint64
	// 文件索引数据库
	indexStore IndexStore
}

func NewFileManager(fileStorePath string, indexStorePath string) (*FileManager, error) {
	fs := &FileManager{
		fileStore: fileStorePath,
	}

	indexStore := &indexStore{}
	if err := indexStore.Open(indexStorePath); err != nil {
		return nil, err
	}
	fs.indexStore = indexStore
	// TODO 实现恢复功能
	// 通过打开文件来初始化其余的值
	if err := fs.openFile(); err != nil {
		return nil, err
	}
	return fs, nil
}

// openFile 打开一个文件
func (fm *FileManager) openFile() error {
	fileName := filepath.Join(fm.fileStore, fm.newFileName())
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		// 报错了，fSeq-1
		fm.fSeq--
		return errors.Wrapf(err, "open file failed, file name=%s", fileName)
	}
	fm.file = file
	fm.writer = bufio.NewWriter(file)
	// 打开了一个新的文件，偏移量置为0
	fm.offset = 0
	return nil
}

// newFileName 构建一个新的文件名，名字格式为file_000001
func (fm *FileManager) newFileName() string {
	fm.fSeq++
	return buildFileName(fm.fSeq)
}

func buildFileName(seq uint64) string {
	return "file_" + fmt.Sprintf("%06d", seq)
}

func (fm *FileManager) Read(blockId string) ([]byte, error) {
	index, err := fm.indexStore.FetchIndex(blockId)
	if err != nil {
		return nil, err
	}

	fileName := filepath.Join(fm.fileStore, buildFileName(index.FSeq))
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
	_, err = fm.writer.Write(data)
	if err != nil {
		// 写入失败，需要销毁这段数据
		fm.truncate(int64(fm.offset))
		return "", errors.Wrap(err, "write data into file failed")
	}
	// 更新索引
	if err = fm.indexStore.SaveIndex(&BlockIndex{
		FSeq:    fm.fSeq,
		BlockId: id,
		Offset:  fm.offset,
	}); err != nil {
		fm.truncate(int64(fm.offset))
		return "", err
	}
	fm.offset += uint64(len(data))
	fm.writer.Flush()
	return id, nil
}

func (fm *FileManager) moveToNextFile() error {
	return fm.openFile()
}

// truncate 清理损坏的数据
func (fm *FileManager) truncate(offset int64) error {
	return fm.file.Truncate(offset)
}

func (fm *FileManager) Close() error {
	fm.indexStore.Close()
	return fm.file.Close()
}
