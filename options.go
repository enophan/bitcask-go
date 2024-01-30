package bitcask_go

type Options struct {
	DirPath      string // 数据目录
	DataFileSize int64
	SyncWrite    bool // 每次写入数据，持久化与否
	IndexType    IndexType
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
	BPlusTree
)
