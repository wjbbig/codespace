package fs

import (
	"bufio"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"io"
	"my-fs/utils"
	"os"
)

type fileStream struct {
	fSeq          int
	file          *os.File
	reader        *bufio.Reader
	currentOffset int64
}

type chunkPlacement struct {
	fSeq             int
	chunkStartOffset int64
	chunkBytesOffset int64 // 这里的offset是chunkStartOffset+n(chunk长度)
}

func newFileStream(rootDir string, fileSeq int, startOffset int64) (*fileStream, error) {
	filePath := buildFilePath(rootDir, fileSeq)
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "open file [%s] failed", filePath)
	}

	var newPosition int64
	if newPosition, err = file.Seek(startOffset, 0); err != nil {
		return nil, errors.Wrapf(err, "seek file [%s] to startOffset %d failed", filePath, startOffset)
	}
	if newPosition != startOffset {
		return nil, errors.Wrapf(err, "Could not seek block file [%s] to startOffset [%d]. New position = [%d]",
			filePath, startOffset, newPosition)
	}

	return &fileStream{
		fSeq:          fileSeq,
		file:          file,
		reader:        bufio.NewReader(file),
		currentOffset: startOffset,
	}, nil
}

func (s *fileStream) scanForNextChunk() ([]byte, error) {
	chunkBytes, _, err := s.nextChunkBytesAndPlacement()
	return chunkBytes, err
}

func (s *fileStream) nextChunkBytesAndPlacement() ([]byte, *chunkPlacement, error) {
	var fileInfo os.FileInfo
	var err error
	var moreContentAvailable = true

	if fileInfo, err = s.file.Stat(); err != nil {
		return nil, nil, errors.Wrapf(err, "get file stat failed")
	}
	// 读完了
	if s.currentOffset == fileInfo.Size() {
		return nil, nil, nil
	}

	remainingBytes := fileInfo.Size() - s.currentOffset
	// 读取代表chunk长度的字节，默认读8字节，如果不够，就读剩余的字节
	peekLen := 8
	if remainingBytes < int64(peekLen) {
		peekLen = int(remainingBytes)
		moreContentAvailable = false
	}
	peekBytes, err := s.reader.Peek(peekLen)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "peek %d bytes from file failed", peekLen)
	}
	chunkLen, n := proto.DecodeVarint(peekBytes)
	if n == 0 {
		// 没有读取任何字节，这表示这些字节并不能解析出一个chunk大小
		if !moreContentAvailable {
			return nil, nil, utils.ErrUnexpectedEndOfFile
		}
		return nil, nil, errors.Wrapf(err, "decode varint bytes %v failed", peekBytes)
	}
	// 跳过n个字节，开始读取chunk数据
	if _, err = s.reader.Discard(n); err != nil {
		return nil, nil, errors.Wrapf(err, "discard %d bytes", n)
	}
	chunkBytes := make([]byte, chunkLen)
	if _, err = io.ReadAtLeast(s.reader, chunkBytes, int(chunkLen)); err != nil {
		return nil, nil, utils.ErrUnexpectedEndOfFile
	}
	chunkPlacement := &chunkPlacement{
		fSeq:             s.fSeq,
		chunkStartOffset: s.currentOffset,
		chunkBytesOffset: s.currentOffset + int64(n),
	}
	s.currentOffset += int64(n) + int64(chunkLen)
	return chunkBytes, chunkPlacement, nil
}

func (s *fileStream) close() error {
	return s.file.Close()
}
