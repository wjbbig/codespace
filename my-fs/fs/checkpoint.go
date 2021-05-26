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
