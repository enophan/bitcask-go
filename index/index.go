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
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
	BPlusTree
)

func NewIndexer(t IndexType) Indexer {
	switch t {
	case Btree:
		return NewBtree()

	case ART:
		return nil
	case BPlusTree:
		return nil
	default:
		panic("暂不支持该类型")
	}
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
