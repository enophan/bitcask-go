package bitcask_go

import (
	"hutuodb/data"
	"hutuodb/index"
	"sync"
)

// 给用户提供的接口

type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile
	olderFile  map[uint32]*data.DataFile
	options    Options
	index      index.Indexer
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// 校验Key合法与否，存在与否
	// 根据Fid从确定在活跃文件还是某个旧数据文件
	// 根据数据文件中的偏移值获得logRecord
	// 确定logRecord类型（normal/delete）
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFile[logRecordPos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	return logRecord.Value, nil

}

// appendLogRecord 数据写入活跃文件，返回地址信息
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 若未初始化活跃文件，则新建
	// 对数据二进制编码，调用DataFile的Write，将数据写入活跃文件
	// 写入此条数据之后，若是超出活跃文件阈值，则创建新活跃文件
	// 最后，根据编码后的偏移值计算出数据位置并返回

	// 除此之外还可以给用户提供其他选择，比如是否当即持久化数据

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	enLogRecord, size := data.EncodeLogRecord(logRecord)

	if db.activeFile.WOffset+size > db.options.DataFileSize {
		// 写入此条日志后，是否超出活跃文件阈值？
		// 若是，首先持久化当前活跃文件
		// 其次，将活跃文件加入旧数据文件
		// 最后，创建新活跃文件

		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.olderFile[db.activeFile.FileId] = db.activeFile

		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	wOffset := db.activeFile.WOffset

	if err := db.activeFile.Write(enLogRecord); err != nil {
		return nil, err
	}

	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: wOffset,
	}

	return pos, nil
}

func (db *DB) setActiveFile() error {
	var initFileId uint32 = 0
	if db.activeFile != nil {
		initFileId = db.activeFile.FileId + 1
	}

	// 打开数据文件
	dataFile, err := data.OpeanDataFile(db.options.DirPath, initFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile

	return nil
}
