package fs

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)
// checkpoint 用于记录最后记录的文件号与文件大小
type checkpoint struct {
	lastFileSeq uint64
	lastFileSize uint64
}

func (cp *checkpoint) marshal() ([]byte, error) {
	buffer := proto.NewBuffer([]byte{})
	if err := buffer.EncodeVarint(cp.lastFileSeq); err != nil {
		return nil, errors.Wrapf(err, "error encoding the lastFileSeq [%d]", cp.lastFileSeq)
	}
	if err := buffer.EncodeVarint(cp.lastFileSize); err != nil {
		return nil, errors.Wrapf(err, "error encoding the lastFileSize [%d]", cp.lastFileSize)
	}
	return buffer.Bytes(), nil	
}

func (cp *checkpoint) unmarshal(data []byte) error {
	buffer := proto.NewBuffer(data)
	var err error
	if cp.lastFileSeq, err = buffer.DecodeVarint(); err != nil {
		return errors.Wrap(err, "error decoding the lastFileSeq")
	}
	if cp.lastFileSize, err = buffer.DecodeVarint(); err != nil {
		return errors.Wrap(err, "error decoding the lastFileSize")
	}
	return nil
}