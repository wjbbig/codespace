package fs

import (
	"os"
	"testing"
)

const (
	defaultIndexStore = "../test/indexstore"
	defaultFileStore  = "../test/filestore"
)

func TestFS_NewFileName(t *testing.T) {
	fs := &FileManager{}
	t.Log(fs.newFileName())
}

func TestFS_OpenFile(t *testing.T) {
	_, err := NewFileManager(defaultFileStore, defaultIndexStore)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFileManager_Write(t *testing.T) {
	if err := os.Mkdir(defaultIndexStore, 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(defaultFileStore, 0777); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFileManager(defaultFileStore, defaultIndexStore)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(defaultFileStore)
	defer os.RemoveAll(defaultIndexStore)
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

	fs.truncate(int64(fs.offset) - int64(len(data)))

	readBytes, err = fs.Read(id1)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(readBytes))
}
