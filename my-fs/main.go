package main

import (
	"fmt"
	myfs "my-fs/fs"
	"net/http"

	"github.com/golang/protobuf/proto"
)

type Server struct {
	mux *http.ServeMux
	fs  myfs.FS
}

func main() {
	data := proto.EncodeVarint(uint64(10))
	fmt.Println(len(data))
	data = append(data, 1)
	data = append(data, 1)
	data = append(data, 2)
	data = append(data, 1)
	result, n := proto.DecodeVarint(data)
	fmt.Println(n)
	fmt.Println(result)
}