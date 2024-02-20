package bitcask_go

import (
	"bitcask/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

var txnFin = []byte("txn-fin")

const nonTransactionSeqNo uint64 = 0

type WriteBatch struct {
	opts           WriteBatchOptions
	mu             *sync.Mutex
	db             *DB
	penddingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	// db.isInitial 因为首次启动肯定是没有序列号文件的
	if db.options.IndexType == BPlusTree && !db.seqFileExists && db.isInitial {
		panic("由于序列号文件不存在，已禁止使用WrtiteBatch功能")
	}
	return &WriteBatch{
		opts:           opts,
		mu:             new(sync.Mutex),
		db:             db,
		penddingWrites: make(map[string]*data.LogRecord),
	}
}

func (w *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}

	// 暂存
	w.penddingWrites[string(key)] = logRecord

	return nil
}

func (w *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// 如果不在索引内，则直接返回即可
	logRecordPos := w.db.index.Get(key)
	if logRecordPos == nil {
		if w.penddingWrites[string(key)] == nil {
			delete(w.penddingWrites, string(key))
		}
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	w.penddingWrites[string(key)] = logRecord

	return nil
}

func (w *WriteBatch) Commit() error {
	// 两阶段锁实现串行化
	// 为每条数据添加序列号
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.penddingWrites) == 0 {
		return nil
	}
	if uint(len(w.penddingWrites)) > w.opts.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 串行化
	w.db.mu.Lock()
	defer w.db.mu.Unlock()

	seqNo := atomic.AddUint64(&w.db.seqNo, 1)

	positions := make(map[string]*data.LogRecordPos)
	for _, record := range w.penddingWrites {
		logRecordPos, err := w.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// 标识事务结束的数据，用于
	finLogRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFin, seqNo),
		Type: data.LogRecordTxnFinished,
	}

	if _, err := w.db.appendLogRecord(finLogRecord); err != nil {
		return err
	}

	// 持久化
	if w.opts.SyncWrite && w.db.activeFile != nil {
		if err := w.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新索引
	for _, record := range w.penddingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordDeleted {
			w.db.index.Delete(record.Key)
		}
		if record.Type == data.LogRecordNormal {
			w.db.index.Put(record.Key, pos)
		}
	}

	// 清空
	w.penddingWrites = make(map[string]*data.LogRecord)
	return nil
}

func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))

	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析分离出key与seqNo
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)

	realKey := key[n:]
	return realKey, seqNo
}
