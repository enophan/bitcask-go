package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	// 普通情况
	log1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("enophan"),
		Type:  LogRecordNormal,
	}
	enc1, n1 := EncodeLogRecord(log1)
	// enc1 = [161 162 44 96 0 8 14 110 97 109 101 101 110 111 112 104 97 110]
	// n1 = 18
	// header = [161 162 44 96 0 8 14]
	// crc = 1613537953
	// keySize = 4
	// valueSize = 7
	assert.NotNil(t, enc1)
	assert.Greater(t, n1, int64(5)) // 因为crc和recordType是一定存在的，所以正常情况一定是大于5的

	// value值为空
	log2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte(""),
		Type:  LogRecordNormal,
	}
	enc2, n2 := EncodeLogRecord(log2)
	// enc2 = [9 252 88 14 0 8 0 110 97 109 101]
	// n2 = 11
	// header = [9 252 88 14 0 8 0]
	// crc: 240712713
	// keySize: 4
	// valueSize: 0
	assert.NotNil(t, enc2)
	assert.Greater(t, n2, int64(5))
	// type：delete
}

func TestDecodeLogRecordHeader(t *testing.T) {
	headerBuf1 := []byte{161, 162, 44, 96, 0, 8, 14}
	header1, size1 := decodeLogRecordHeader(headerBuf1)

	assert.NotNil(t, header1)
	assert.Equal(t, size1, int64(7))
	assert.Equal(t, header1.crc, uint32(1613537953))
	assert.Equal(t, LogRecordNormal, header1.recordType)
	assert.Equal(t, uint32(4), header1.keySize)
	assert.Equal(t, uint32(7), header1.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	log1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("enophan"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{161, 162, 44, 96, 0, 8, 14}
	crc1 := getLogRecordCRC(log1, headerBuf1[crc32.Size:])
	assert.Equal(t, crc1, uint32(1613537953))

	log2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte(""),
		Type:  LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(log2, headerBuf2[crc32.Size:])
	assert.Equal(t, crc2, uint32(240712713))
}
