package fio

const DataFilePerm = 0644

// IOManager 抽象 IO 管理接口，可接入不同类型的 IO
type IOManager interface {

	// Read 读给定文件的指定位置的对应数据
	Read([]byte, int64) (int, error)

	// Write 写入到指定文件，返回的写入的有效字节数
	Write([]byte) (int, error)

	// Sync 缓存持久化到磁盘
	Sync() error

	// Close 关闭文件
	Close() error
}
