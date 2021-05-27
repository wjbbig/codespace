package fs

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// checkpoint 用于记录最后记录的文件号与文件大小
type checkpoint struct {
	lastFileSeq  int
	lastFileSize int
}

func constructCheckpointFromFiles(rootDir string) (*checkpoint, error) {
	var offsetAfterLastChunk, chunkNums int64
	lastFileSeq, err := retrieveLastFileSuffix(rootDir)
	if err != nil {
		return nil, err
	}
	// 文件夹中没有任何数据文件
	if lastFileSeq == -1 {
		checkpoint := &checkpoint{1, 0}
		return checkpoint, nil
	}

	_, offsetAfterLastChunk, chunkNums, err = scanForLastCompleteChunk(rootDir, lastFileSeq, 0)
	if err != nil {
		return nil, err
	}
	// 最后一个文件中没有任何数据，需要读取倒数第二个文件
	if chunkNums == 0 && lastFileSeq > 1 {
		secondLastFileSeq := lastFileSeq - 1
		_, offsetAfterLastChunk, _, err = scanForLastCompleteChunk(rootDir, secondLastFileSeq, 0)
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

// syncCheckpointFromFS 从已有的存储文件中，重新同步checkpoint
func syncCheckpointFromFS(rootDir string, cp *checkpoint) error {
	filePath := buildFilePath(rootDir, cp.lastFileSeq)
	exists, size, err := fileExists(filePath)
	if err != nil {
		return err
	}
	if !exists || int(size) == cp.lastFileSize {
		return nil
	}
	_, offsetAfterLastChunk, _, err := scanForLastCompleteChunk(rootDir, cp.lastFileSeq, int64(cp.lastFileSize))
	if err != nil {
		return err
	}
	cp.lastFileSize = int(offsetAfterLastChunk)
	return nil
}

func (cp *checkpoint) marshal() ([]byte, error) {
	buffer := proto.NewBuffer([]byte{})
	if err := buffer.EncodeVarint(uint64(cp.lastFileSeq)); err != nil {
		return nil, errors.Wrapf(err, "error encoding the lastFileSeq [%d]", cp.lastFileSeq)
	}
	if err := buffer.EncodeVarint(uint64(cp.lastFileSize)); err != nil {
		return nil, errors.Wrapf(err, "error encoding the lastFileSize [%d]", cp.lastFileSize)
	}
	return buffer.Bytes(), nil
}

func (cp *checkpoint) unmarshal(data []byte) error {
	buffer := proto.NewBuffer(data)
	var err error
	var val uint64
	if val, err = buffer.DecodeVarint(); err != nil {
		return errors.Wrap(err, "error decoding the lastFileSeq")
	}
	cp.lastFileSeq = int(val)
	if val, err = buffer.DecodeVarint(); err != nil {
		return errors.Wrap(err, "error decoding the lastFileSize")
	}
	cp.lastFileSize = int(val)
	return nil
}
