package bitcask_go

import "os"

type Options struct {
	DirPath      string // 数据目录
	DataFileSize int64
	SyncWrite    bool      // 每次写入数据，持久化与否
	IndexType    IndexType // 所采用的索引类型
}

type IndexType = int8

type IteratorOptions struct {
	Prefix  []byte // 遍历前缀为指定的key（？）
	Reverse bool
}

// 目前所能支持的索引类型
const (
	Btree IndexType = iota + 1
	ART
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrite:    false,
	IndexType:    Btree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
