package index

import (
	"hutuodb/data"
	"sync"

	"github.com/google/btree"
)

// Btree 索引
type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex // 并发要自己上锁
}

func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (b *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := Item{
		key: key,
		pos: pos,
	}
	b.lock.Lock()
	b.tree.ReplaceOrInsert(&item)
	b.lock.Unlock()
	return true
}
func (b *Btree) Get(key []byte) *data.LogRecordPos {
	item := &Item{
		key: key,
	}
	bitem := b.tree.Get(item)
	if bitem == nil {
		return nil
	}

	return bitem.(*Item).pos
}
func (b *Btree) Delete(key []byte) bool {
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
