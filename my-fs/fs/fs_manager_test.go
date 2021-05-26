package fs

import (
	"os"
	"testing"
)

const (
	testIndexStore = "../testdata/indexstore"
	testFileStore  = "../testdata/filestore"
)

func TestFS_NewFileName(t *testing.T) {
	fs := &FileManager{}
	t.Log(fs.newFileName())
}

func TestFS_OpenFile(t *testing.T) {
	_, err := NewFileManager(testFileStore, testIndexStore)
	if err != nil {
		t.Fatal(err)
	}
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

	fs.truncate(int(fs.offset) - len(data))

	readBytes, err = fs.Read(id1)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(readBytes))
}
