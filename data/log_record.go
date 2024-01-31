package data

import "encoding/binary"

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

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

func decodeLogRecordHeader(headerbuf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(l *LogRecord, h []byte) uint32 {
	return 0
}
