package fs

import (
	"os"
	"testing"
)

func TestIndexStore(t *testing.T) {
	indexStore := &indexStore{}
	storePath := "./indexstore"
	err := indexStore.Open(storePath)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(storePath)
	defer indexStore.Close()

	blockIndex := &BlockIndex{
		FSeq:    1,
		BlockId: "zzyucsgt",
		Offset:  78,
	}

	err = indexStore.SaveIndex(blockIndex)
	if err != nil {
		t.Fatal(err)
	}

	index, err := indexStore.FetchIndex(blockIndex.BlockId)
	if err != nil {
		t.Fatal(err)
	}

	if *index != *blockIndex {
		t.Fatal("index data has been changed")
	}

}
