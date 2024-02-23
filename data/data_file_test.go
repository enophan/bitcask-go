package data

import (
	"bitcask/fio"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 1, fio.StandardFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("file1"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 2, fio.StandardFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("feabhhabfhj"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 3, fio.StandardFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("feabhhabfhj"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 6, fio.StandardFile)
	assert.NotNil(t, dataFile)
	assert.Nil(t, err)

	// 只有一条LogRecord
	log1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("enophan"),
		Type:  LogRecordNormal,
	}
	encLog1, size1 := EncodeLogRecord(log1)
	err = dataFile.Write(encLog1)
	assert.Nil(t, err)

	readLog1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, readLog1, log1)
	assert.Equal(t, readSize1, size1)
	t.Log(len(encLog1))

	// 从多条数据的中间读取
	log2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	encLog2, size2 := EncodeLogRecord(log2)
	err = dataFile.Write(encLog2)
	assert.Nil(t, err)

	readLog2, readSize2, err := dataFile.ReadLogRecord(18)
	assert.Nil(t, err)
	assert.Equal(t, readLog2, log2)
	assert.Equal(t, readSize2, size2)
	t.Log(len(encLog2))

	// 被删除数据在文件末尾
	log3 := &LogRecord{
		Key:   []byte("n"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	encLog3, size3 := EncodeLogRecord(log3)
	err = dataFile.Write(encLog3)
	assert.Nil(t, err)

	readLog3, readSize3, err := dataFile.ReadLogRecord(readSize1 + readSize2)
	assert.Nil(t, err)
	assert.Equal(t, readLog3, log3)
	assert.Equal(t, readSize3, size3)
	t.Log(len(encLog3))
}
