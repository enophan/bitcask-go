package data

import "hutuodb/fio"

type DataFile struct {
	FileId    uint32
	WOffset   int64
	IOManager fio.IOManager
}

func OpeanDataFile(dirPath string, id uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}

func (df *DataFile) Write(b []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}
