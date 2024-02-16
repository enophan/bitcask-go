package index

import (
	"bitcask/data"
	"bytes"

	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	Iterator(reverse bool) Iterator

	// Size 索引中的数据量
	Size() int
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
	BPTree // 暂时不打算开启此选项
)

func NewIndexer(t IndexType) Indexer {
	switch t {
	case Btree:
		return NewBtree()
	case ART:
		return NewART()
	case BPTree:
		return nil
	default:
		panic("暂不支持该类型")
	}
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器
type Iterator interface {
	// 回到迭代器起点，即第一个数据
	Rewind()

	// 查找首个大于（小于）等于key，并据此key开始遍历
	Seek(key []byte)

	// 跳转到下一个key
	Next()

	// 有效性检验，用于退出遍历
	Valid() bool

	// 当前遍历的key值
	Key() []byte

	// 当前遍历的value的位置
	Value() *data.LogRecordPos

	// 关闭迭代器
	Close()
}
