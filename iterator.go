package bitcask_go

import (
	"bitcask/index"
	"bytes"
)

type Iterator struct {
	indexIterator index.Iterator
	db            *DB
	options       IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIterator: indexIter,
		db:            db,
		options:       opts,
	}
}

// 回到迭代器起点，即第一个数据
func (i *Iterator) Rewind() {
	i.indexIterator.Rewind()
	i.skipToNext()
}

// 查找首个大于（小于）等于key，并据此key开始遍历
func (i *Iterator) Seek(key []byte) {
	i.indexIterator.Seek(key)
	i.skipToNext()
}

// 跳转到下一个key
func (i *Iterator) Next() {
	i.indexIterator.Next()
	i.skipToNext()
}

// 有效性检验，用于退出遍历
func (i *Iterator) Valid() bool {
	return i.indexIterator.Valid()
}

// 当前遍历的key值
func (i *Iterator) Key() []byte {
	return i.indexIterator.Key()
}

// 当前位置的value
func (i *Iterator) Value() ([]byte, error) {
	pos := i.indexIterator.Value()
	i.db.mu.RLock()
	defer i.db.mu.RUnlock()
	return i.db.getValueByPostion(pos)
}

// 关闭迭代器
func (i *Iterator) Close() {
	i.indexIterator.Close()
}

func (i *Iterator) skipToNext() {
	prefixlen := len(i.options.Prefix)
	if prefixlen == 0 {
		return
	}

	for ; i.indexIterator.Valid(); i.indexIterator.Next() {
		key := i.indexIterator.Key()
		if prefixlen <= len(key) && bytes.Equal(i.options.Prefix, key[:prefixlen]) {
			break
		}
	}
}
