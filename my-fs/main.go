package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"log"
	myfs "my-fs/fs"
	"my-fs/model"
	"net/http"
	"os"
)

const (
	defaultHttpPort = 8080
)

type server struct {
	engine *gin.Engine
	fs     myfs.FS
}

func newServer() (*server, error) {
	fileStorePath := os.Getenv("FILE_STORE_PATH")
	if fileStorePath == "" {
		return nil, errors.New("file store path can not be empty")
	}
	indexStorePath := os.Getenv("INDEX_STORE_PATH")
	if indexStorePath == "" {
		return nil, errors.New("index store path can not be empty")
	}

	fs, err := myfs.NewFileManager(fileStorePath, indexStorePath)
	if err != nil {
		return nil, err
	}
	engine := gin.Default()
	return &server{engine, fs}, nil
}

func (s *server) Start() error {
	s.engine.POST("/write", func(ctx *gin.Context) {
		upData := new(model.UploadData)
		if err := ctx.Bind(upData); err != nil {
			ctx.JSON(http.StatusOK, model.NewErrorResp(err.Error()))
			return
		}

		chunkId, err := s.fs.Write([]byte(upData.Data))
		if err != nil {
			ctx.JSON(http.StatusOK, model.NewErrorResp(err.Error()))
			return
		}
		ctx.JSON(http.StatusOK, model.NewSuccessResp(chunkId))
	})

	s.engine.GET("/read", func(ctx *gin.Context) {
		chunkId := ctx.Query("chunkid")
		if chunkId == "" {
			ctx.JSON(http.StatusOK, model.NewErrorResp("chunk id can not be empty"))
			return
		}

		data, err := s.fs.Read(chunkId)
		if err != nil {
			ctx.JSON(http.StatusOK, model.NewErrorResp(err.Error()))
			return
		}

		ctx.JSON(http.StatusOK, model.NewSuccessResp(string(data)))
	})

	return s.engine.Run(fmt.Sprintf(":%d", defaultHttpPort))
}

func main() {
	s, err := newServer()
	if err != nil {
		log.Panicln(err)
	}
	defer s.fs.Close()
	if err = s.Start(); err != nil {
		log.Panicln(err)
	}
}
