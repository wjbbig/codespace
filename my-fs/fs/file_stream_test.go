package fs

import (
	"bytes"
	pb "my-fs/proto"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

func TestFileStream_All(t *testing.T) {
	stream, err := newFileStream(testFileStore, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.close()

	for i := 0; i < 3; i++ {
		chunkBytes, err := stream.scanForNextChunk()
		if err != nil {
			t.Fatal(err)
		}
		chunk := &pb.Chunk{}
		_ = proto.Unmarshal(chunkBytes, chunk)
		if !bytes.Equal(chunk.Payload, []byte("helloworld")) {
			t.Fatal(err)
		}
	}

	_, err = stream.scanForNextChunk()
	if err == nil {
		t.Fatal("err should not be nil")
	}
}

func TestCreateTestFileData(t *testing.T) {
	file, err := os.OpenFile(filepath.Join(testFileStore, "file_000001"), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	payload := []byte("helloworld")
	chunk := &pb.Chunk{Payload: payload, Id: uuid.New().String()}
	data, _ := proto.Marshal(chunk)
	lenData := proto.EncodeVarint(uint64(len(data)))
	file.Write(lenData)
	file.Write(data)
	file.Write(lenData)
	file.Write(data)
	file.Write(lenData)
	file.Write(data)
	file.Write([]byte("uselessdata"))
	file.Sync()
}
