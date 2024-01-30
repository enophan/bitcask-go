package data

import "hutuodb/fio"

type DataFile struct {
	FileId    uint32
	WOffset   int64 // 从文件哪个地方开始写的偏移量
	IOManager fio.IOManager
}

var DataFileNameSuffix string = ".data"

func OpeanDataFile(dirPath string, id uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func (df *DataFile) Write(b []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}
