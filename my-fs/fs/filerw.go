package fs

import (
	"os"

	"github.com/pkg/errors"
)

type fileWriter struct {
	file *os.File
	filePath string
}

func newFileWriter(filePath string) (*fileWriter, error) {
	writer := &fileWriter{filePath: filePath}
	return writer, writer.open()
}

func (f *fileWriter) open() error {
	file, err := os.OpenFile(f.filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return errors.Wrap(err, "open file failed")
	}
	f.file = file
	return nil
}

// write 将数据写入文件，如果sync为true，则直接文件中，否则会先保存在缓冲区
func (f *fileWriter) write(data []byte, sync bool) error {
	if _, err := f.file.Write(data); err != nil {
		return err
	}
	if sync {
		return f.file.Sync()
	}
	return nil
}

func (f *fileWriter) truncate(targetSize int) error {
	stat, err := f.file.Stat()
	if err != nil {
		return errors.Wrapf(err, "error truncating the file [%s] to size [%d]", f.filePath, targetSize)
	}
	if stat.Size() < int64(targetSize) {
		return nil
	}

	return f.file.Truncate(int64(targetSize))
}

func (f *fileWriter) close() error {
	return f.file.Close()
}

type fileReader struct {
	file *os.File
}

func newFileReader(filePath string) (*fileReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return &fileReader{file: file}, nil
}

func (f *fileReader) read(offset int, length int) ([]byte, error) {
	buffer := make([]byte, length)
	if _, err := f.file.ReadAt(buffer, int64(offset)); err != nil {
		return nil, err
	}

	return buffer, nil
}

func (f *fileReader) close() error {
	return f.file.Close()
}