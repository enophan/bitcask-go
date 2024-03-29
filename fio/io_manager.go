package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFile FileIOType = iota
	MemoryMap
)

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

	// Size 文件大小
	Size() (int64, error)
}

func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFile:
		// 新建FileIO类型IOManager
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("暂不支持此文件类型")
	}
}
