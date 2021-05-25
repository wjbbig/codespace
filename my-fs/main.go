package main

import (
	"my-fs/fs"
	"net/http"
)

type Server struct {
	mux *http.ServeMux
	fs  fs.FS
}
