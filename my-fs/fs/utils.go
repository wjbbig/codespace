package fs

import (
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// checkValidFileName 检测文件名是否合法
func checkValidFileName(name string) bool {
	return strings.Contains(name, filePrefix)
}

// createDirIfMissing 创建文件夹，如果该文件夹不存在，并返回是否为空
func createDirIfMissing(dirPath string) (bool, error) {
	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return false, err
	}
	dir, err := os.Open(dirPath)
	if err != nil {
		return false, err
	}
	defer dir.Close()
	_, err = dir.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

// retrieveLastFileSuffix 获取最大的文件序号
func retrieveLastFileSuffix(rootDir string) (int, error) {
	biggestFileSeq := -1
	fileInfos, err := ioutil.ReadDir(rootDir)
	if err != nil {
		return -1, errors.Wrapf(err, "error reading dir %s", rootDir)
	}

	for _, info := range fileInfos {
		name := info.Name()
		if info.IsDir() || !checkValidFileName(name) {
			continue
		}

		fileSeqStr := strings.TrimPrefix(name, filePrefix)
		seq, err := strconv.Atoi(fileSeqStr)
		if err != nil {
			return -1, err
		}
		if seq > biggestFileSeq {
			biggestFileSeq = seq
		}
	}

	return biggestFileSeq, nil
}
