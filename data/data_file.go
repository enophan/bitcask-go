package data

import (
	"bitcask/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
)

type DataFile struct {
	FileId    uint32
	WOffset   int64 // 从文件哪个地方开始写的偏移量
	IOManager fio.IOManager
}

// OpenDataFile 根据目录，打开对应目的文件。返回目的数据文件地址、错误
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// 组合出完整的文件路径及名称
	// 然后打开文件 -> NewIOManager
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	return newDataFile(fileName, fileId)
}

func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WOffset:   0,
		IOManager: ioManager,
	}, nil
}

// <fileId>.data
func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

// ReadLogRecord 从数据文件中某位置（offset）读取logRecord日志数据。返回目的数据的地址、目的数据的长度、错误
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 获取header -> readNBytes
	// 解码header -> decodeLogRecordHeader
	// 解码后若发现此位置（offset）为数据末尾，则退出
	// 取出logRecord中的key与value -> readNBytes
	// 校验其有效性（crc） todo 怎么校验的？在校验什么？

	// todo ?
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)

	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	var logRecord = &LogRecord{Type: header.recordType}
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, errors.New("crc校验错误")
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) Write(b []byte) error {
	// 调用IOManager.Write
	// 写入后注意更新偏移量（WOffset）
	n, err := df.IOManager.Write(b)
	if err != nil {
		return err
	}
	df.WOffset += int64(n)
	return nil
}

func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	hintRecord := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(hintRecord)
	return df.Write(encRecord)
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

// readNBytes 从offset开始读n个数据
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}
