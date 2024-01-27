package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 清除临时文件
func destroyFile(fileName string) {
	if err := os.RemoveAll(fileName); err != nil {
		panic(err)
	}
}

// 测试
func TestNewFileIOManager(t *testing.T) {
	fileName := filepath.Join("/tmp", "test.data")
	fio, err := NewFileIOManager(fileName)
	defer destroyFile(fileName)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestRead(t *testing.T) {
	fileName := filepath.Join("/tmp", "test.data")
	fio, err := NewFileIOManager(fileName)
	defer destroyFile(fileName)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("0123456789"))
	assert.Nil(t, err)

	b := make([]byte, 4)
	l, err := fio.Read(b, 0)
	assert.Nil(t, err)
	assert.Equal(t, l, 4)
}

func TestWrite(t *testing.T) {
	fileName := filepath.Join("/tmp", "test.data")
	fio, err := NewFileIOManager(fileName)
	defer destroyFile(fileName)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("writesdfghjkl"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte(""))
	assert.Nil(t, err)
}

func TestSync(t *testing.T) {}

func TestClose(t *testing.T) {}
