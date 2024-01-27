package fio

import "os"

// 封装标准文件接口
type FileIO struct {
	fd *os.File
}

// NewFileIOManager 创建一新 FileIO（实现了IOManager接口）
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}

	return &FileIO{fd: fd}, nil
}

// 实现 IOManager 接口

// Read 从哪个位置（offset）读多长（b）的数据，返回读出的数据长度和 err
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Write 写入多长数据（b），返回这次写入的数据长度和 err
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Sync
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
