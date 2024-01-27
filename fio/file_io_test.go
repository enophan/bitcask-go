package fio

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试
func TestNewFileIOManager(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp/bitcask", "go.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}
