package fs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateDirIfNotMissing(t *testing.T) {
	b, err := createDirIfMissing(testFileStore)
	if err != nil {
		t.Fatal(err)
	}
	if !b {
		t.Fatal("dir should be empty")
	}

	fileName := "aa"
	name := filepath.Join(testFileStore, fileName)
	f, err := os.OpenFile(name, os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("create file failed, err=%s", err)
	}
	defer f.Close()

	b, err = createDirIfMissing(testFileStore)
	if err != nil {
		t.Fatal(err)
	}
	if b {
		t.Fatal("dir should not be empty")
	}

	os.RemoveAll(testFileStore)
}
