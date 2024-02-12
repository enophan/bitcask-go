package bitcask_go

import (
	"bitcask/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_WriteBatch(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wBatch := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wBatch.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wBatch.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err) // 提交前是查不到的

	err = wBatch.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
}
func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultDBOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-wb")
	dir := "/tmp/batch-test-1"
	opts.DirPath = dir
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//// 数据不存在
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	//wb.Put(utils.GetTestKey(12), utils.RandomValue(10))
	//wb.Delete(utils.GetTestKey(12))
	//err = wb.Commit()
	//t.Log(err)
	//
	//	数据存在
	err = db.Put(utils.GetTestKey(12), utils.RandomValue(10))
	assert.Nil(t, err)
	wb.Delete(utils.GetTestKey(12))
	err = wb.Commit()
	t.Log(err)

	err = db.Put(utils.GetTestKey(12), utils.RandomValue(10))
	val, err := db.Get(utils.GetTestKey(12))
	t.Log(string(val))
	t.Log(err)

	t.Log(db.seqNo)
}

func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultDBOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-wb")
	dir := "/tmp/batch-test-1"
	opts.DirPath = dir
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	////	提交之后再提交
	//wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	//err = wb.Put(utils.GetTestKey(11), utils.RandomValue(12))
	//assert.Nil(t, err)
	//err = wb.Put(utils.GetTestKey(12), utils.RandomValue(12))
	//assert.Nil(t, err)
	//err = wb.Put(utils.GetTestKey(13), utils.RandomValue(12))
	//assert.Nil(t, err)
	//
	//err = wb.Commit()
	//t.Log(err)
	//
	//err = wb.Put(utils.GetTestKey(14), utils.RandomValue(12))
	//assert.Nil(t, err)
	//err = wb.Commit()
	//t.Log(err)

	keys := db.ListKeys()
	for _, k := range keys {
		t.Log(string(k))
	}
}

func TestDB_WriteBatch4(t *testing.T) {
	opts := DefaultDBOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-wb")
	dir := "/tmp/batch-test-3"
	opts.DirPath = dir
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	//for i := 0; i < 7000; i++ {
	//	wb.Put(utils.GetTestKey(i), utils.RandomValue(40960))
	//}
	//
	//err = wb.Commit()
	//t.Log(err)

	keys := db.ListKeys()
	t.Log(len(keys))
	t.Log(db.seqNo)
}
