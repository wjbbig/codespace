package fs

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"my-fs/utils"
)

type IndexStore interface {
	Open(string) error
	SaveIndex(*BlockIndex, bool) error
	FetchIndex(string) (*BlockIndex, error)
	SaveCheckpoint(*checkpoint, bool) error
	FetchCheckpoint() (*checkpoint, error)
	Close() error
}

const (
	checkpointKey = "checkpoint"
)

type BlockIndex struct {
	// 文件的序号
	FSeq int
	// block的唯一Id
	BlockId string
	// 偏移量
	Offset uint64
}

var _ IndexStore = &indexStore{}

type indexStore struct {
	db *leveldb.DB
}

func (i *indexStore) Open(dbPath string) error {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return errors.Wrap(err, "open index store failed")
	}
	i.db = db
	return nil
}

func (i *indexStore) SaveIndex(index *BlockIndex, sync bool) error {
	indexBytes, err := json.Marshal(index)
	if err != nil {
		return errors.Wrap(err, "save block index failed")
	}
	opts := &opt.WriteOptions{}
	if sync {
		opts.Sync = true
	}
	return i.db.Put([]byte(index.BlockId), indexBytes, opts)
}

func (i *indexStore) FetchIndex(id string) (*BlockIndex, error) {
	if i.db == nil {
		return nil, errors.New("index store is empty")
	}
	indexBytes, err := i.db.Get([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return nil, utils.ErrIndexNotFound
	}
	if err != nil {
		return nil, errors.Wrap(err, "get index from store failed")
	}

	index := &BlockIndex{}
	if err = json.Unmarshal(indexBytes, index); err != nil {
		return nil, errors.Wrap(err, "unmarshal blockIndex failed")
	}
	return index, nil
}

func (i *indexStore) SaveCheckpoint(c *checkpoint, sync bool) error {
	cpBytes, err := c.marshal()
	if err != nil {
		return err
	}
	opts := &opt.WriteOptions{}
	if sync {
		opts.Sync = true
	}
	return i.db.Put([]byte(checkpointKey), cpBytes, opts)
}

func (i *indexStore) FetchCheckpoint() (*checkpoint, error) {
	cpBytes, err := i.db.Get([]byte(checkpointKey), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "fetch checkpoint from index store failed")
	}

	cp := &checkpoint{}
	err = cp.unmarshal(cpBytes)
	return cp, err
}

func (i indexStore) Close() error {
	return i.db.Close()
}
