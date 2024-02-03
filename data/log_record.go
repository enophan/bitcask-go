package data

import (
	"encoding/binary"
	"hash/crc32"
)

// LogRecordPos 定义logRecord在哪个文件的哪个地方
type LogRecordPos struct {
	Fid    uint32 // 数据文件的文件id。文件名 int64 可能比较大，比较浪费 int32 比较合理
	Offset int64  // 存储值在这一条目中的偏移位置
}

type LogRecordType = byte

// header = crc + type + keySize + valueSize
// ?
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// LogRecord 写入到数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType // 这条数据是删除还是存储？
}

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// EncodeLogRecord 返回编码后的数组与其长度
//
// |   crc   |  recordType  |  keySize  |  valueSize  |  key  |  value  |
//
//	4            1          max: 5     max: 5        var      var
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 先对头部信息（keySize&valueSize）二进制编码（recordType,key和value已经是二进制了） -> binary.PutVarint
	// 作为校验选项，crc需要最后写入
	// 二进制编码先recordType开始，到valueSize
	// 之后，对二进制的LogRecord做crc编码 -> crc32.ChecksumIEEE &
	// 最后把crc加到header -> binary.LittleEndian.PutUint32
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = logRecord.Type

	// 5是keySize的位置
	var index = 5

	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)

	encodeBytes := make([]byte, size)

	copy(encodeBytes[:index], header[:index])
	copy(encodeBytes[index:], logRecord.Key)
	copy(encodeBytes[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encodeBytes[4:])
	binary.LittleEndian.PutUint32(encodeBytes[:4], crc)

	return encodeBytes, int64(size)
}

// decodeLogRecordHeader 解LogRecordHeader的码。
// 参数为LogRecord的header部分，返回解码后的LogRecordHeader地址及其长度
func decodeLogRecordHeader(headerbuf []byte) (*LogRecordHeader, int64) {
	// 先检验数据是否正常，不然直接解出的码肯定也是错的
	// 二进制解码keySize与valueSize

	if len(headerbuf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(headerbuf[:4]),
		recordType: headerbuf[4],
	}

	var index = 5

	keySize, n := binary.Varint(headerbuf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(headerbuf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// getLogRecordCRC 传入LogRecord和除crc之外的header部分，返回编码后的crc
func getLogRecordCRC(l *LogRecord, h []byte) uint32 {
	if l == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(h[:])
	crc = crc32.Update(crc, crc32.IEEETable, l.Key)
	crc = crc32.Update(crc, crc32.IEEETable, l.Value)

	return crc
}
