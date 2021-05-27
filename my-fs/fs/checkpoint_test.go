package fs

import (
	"testing"
)

func TestCheckpoint_MarshalAndUnmarshal(t *testing.T) {
	cp := &checkpoint{
		lastFileSeq:  1,
		lastFileSize: 1000,
	}

	data, err := cp.marshal()
	if err != nil {
		t.Fatal(err)
	}

	cp1 := new(checkpoint)
	err = cp1.unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}

	if cp1.lastFileSeq != cp.lastFileSeq || cp1.lastFileSize != cp.lastFileSize {
		t.Fatalf("cp1 is not equal to cp")
	}
}

// 在测试这个方法之前，请先执行file_stream_test.go中的TestCreateTestFileData方法
func TestCheckpoint_ConstructCheckpointFromFiles(t *testing.T) {
	checkpoint, err := constructCheckpointFromFiles(testFileStore)
	if err != nil {
		t.Fatal(err)
	}

	if checkpoint.lastFileSeq != 1 {
		t.Fatal("checkpoint lastFileSeq should be 1")
	}

	if checkpoint.lastFileSize != 51 {
		t.Fatal("checkpoint lastFileSize should be 51")
	}
}

func TestCheckpoint_SyncCheckpointFromFS(t *testing.T) {
	cp := new(checkpoint)
	cp.lastFileSeq = 1
	err := syncCheckpointFromFS(testFileStore, cp)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(cp)
	if cp.lastFileSeq != 1 {
		t.Fatal("checkpoint lastFileSeq should be 1")
	}

	if cp.lastFileSize != 51 {
		t.Fatal("checkpoint lastFileSize should be 51")
	}

}