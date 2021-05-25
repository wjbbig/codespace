package fs

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"my-fs/utils"
)

type IndexStore interface {
	Open(string) error
	SaveIndex(*BlockIndex) error
	FetchIndex(string) (*BlockIndex, error)
	Close() error
}

type BlockIndex struct {
	// 文件的序号
	FSeq uint64
	// block的唯一Id
	BlockId string
	// 数据长度
	DataLen uint64
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

func (i *indexStore) SaveIndex(index *BlockIndex) error {
	indexBytes, err := json.Marshal(index)
	if err != nil {
		return errors.Wrap(err, "save block index failed")
	}
	return i.db.Put([]byte(index.BlockId), indexBytes, nil)
}

func (i *indexStore) FetchIndex(id string) (*BlockIndex, error) {
	if i.db == nil {
		return nil, errors.New("index store is empty")
	}
	indexBytes, err := i.db.Get([]byte(id), nil)
	if err == leveldb.ErrNotFound {
		return nil, &utils.ErrIndexNotFound{}
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

func (i indexStore) Close() error {
	return i.db.Close()
}
