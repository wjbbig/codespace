package fs

import (
	"os"
	"testing"
)

const (
	testIndexStore = "../test/fixture/indexstore"
	testFileStore  = "../test/fixture/filestore"
)

func TestFileManager_ScanForLastCompleteChunk(t *testing.T) {
	_, _, nums, err := scanForLastCompleteChunk(testFileStore, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if nums != 3 {
		t.Fatal("num should be 3")
	}
}

func TestFS_OpenFile(t *testing.T) {
	_, err := NewFileManager(testFileStore, testIndexStore)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testFileStore)
	defer os.RemoveAll(testIndexStore)
}

func TestFileManager_Write(t *testing.T) {
	if err := os.Mkdir(testIndexStore, 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(testFileStore, 0777); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFileManager(testFileStore, testIndexStore)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testFileStore)
	defer os.RemoveAll(testIndexStore)
	data := []byte("hello, world")
	id1, err := fs.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	readBytes, err := fs.Read(id1)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(readBytes))

	data = []byte("second data")
	id2, err := fs.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	readBytes, err = fs.Read(id2)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(readBytes))

	fs.truncate(fs.checkpoint.lastFileSize - len(data))

	readBytes, err = fs.Read(id1)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(readBytes))
}
