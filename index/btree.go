package index

import (
	"bitcask/data"
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

// BTree 索引
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex // 并发要自己上锁，google/btree不提供
}

func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (b *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := &Item{
		key: key,
		pos: pos,
	}
	b.lock.Lock()
	b.tree.ReplaceOrInsert(item)
	b.lock.Unlock()
	return true
}
func (b *BTree) Get(key []byte) *data.LogRecordPos {
	item := &Item{
		key: key,
	}
	bitem := b.tree.Get(item)
	if bitem == nil {
		return nil
	}

	return bitem.(*Item).pos
}
func (b *BTree) Delete(key []byte) bool {
	item := &Item{
		key: key,
	}
	b.lock.Lock()
	bitem := b.tree.Delete(item)
	b.lock.Unlock()

	// if bitem == nil {
	// 	return false
	// }
	// return true

	return bitem != nil
}

func (b *BTree) Iterator(reverse bool) Iterator {
	if b.tree == nil {
		return nil
	}
	b.lock.RLock()
	defer b.lock.RUnlock()
	return newBtreeIterator(b.tree, reverse)
}

// Btree 索引迭代器
type btreeIterator struct {
	currIndex int     // 当前位置
	reverse   bool    // 反向遍历与否
	values    []*Item // b树元素集
}

// newBtreeIterator 构建Btree迭代器，以容纳索引迭代器Iterator接口操作
func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	// 主要任务是给btreeIterator.values添加正序或倒序的B树元素 -> Descend()&Ascend()

	var index int
	values := make([]*Item, tree.Len())

	saveValues := func(it btree.Item) bool {
		values[index] = it.(*Item)
		index++
		return true // 配合Descend()&Ascend()，必须返回true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bi *btreeIterator) Rewind() {
	bi.currIndex = 0
}

func (bi *btreeIterator) Seek(key []byte) {
	if bi.reverse {
		bi.currIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].key, key) <= 0
		})
	} else {
		bi.currIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].key, key) >= 0
		})
	}
}

func (bi *btreeIterator) Next() {
	bi.currIndex++
}

func (bi *btreeIterator) Valid() bool {
	return bi.currIndex < len(bi.values)
}

func (bi *btreeIterator) Key() []byte {
	return bi.values[bi.currIndex].key
}

func (bi *btreeIterator) Value() *data.LogRecordPos {
	return bi.values[bi.currIndex].pos
}

func (bi *btreeIterator) Close() {
	bi.values = nil
}
