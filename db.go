package bitcask_go

import (
	"bitcask/data"
	"bitcask/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 给用户提供的接口

type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
	options    Options
	index      index.Indexer
	fileIds    []int // 仅用于加载索引
}

func Open(options Options) (*DB, error) {
	// 检验配置项有无
	// 加载数据文件
	// 加载索引信息

	if err := checkOptions(options); err != nil {
		return nil, err
	}

	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		options:    options,
		index:      index.NewIndexer(options.IndexType),
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil

}

func (db *DB) Put(key []byte, value []byte) error {
	// 添加索引，更新索引
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

	return db.getValueByPostion(logRecordPos)

}

func (db *DB) Delete(key []byte) error {
	// 和put一样的逻辑，不过是没有value

	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDelete,
	}

	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return nil
	}

	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// getValueByPostion 如函数名所说
func (db *DB) getValueByPostion(pos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
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

		db.olderFiles[db.activeFile.FileId] = db.activeFile

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
	dataFile, err := data.OpenDataFile(db.options.DirPath, initFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile

	return nil
}

func (db *DB) loadDataFiles() error {
	// 读取目录中的所有合法文件，即 .data 文件
	// 得到活跃文件和旧数据文件
	// 把文件id存好，一会加载至内存索引中
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return nil
			}
			fileIds = append(fileIds, fileId)
		}
	}

	sort.Ints(fileIds)
	db.fileIds = fileIds

	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return nil
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

func (db *DB) loadIndexFromDataFiles() error {
	// 首先检验有没有datafile，空数据库当然不需要索引
	// 读每个文件里的每条数据，把数据与状态信息记录进索引中
	if len(db.fileIds) == 0 {
		return nil
	}

	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}

				return err
			}

			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}

			var ok bool
			if logRecord.Type == data.LogRecordNormal {
				ok = db.index.Put(logRecord.Key, logRecordPos)
			} else {
				ok = db.index.Delete(logRecord.Key)
			}
			if !ok {
				return ErrKeyIsEmpty
			}

			offset += size

		}

		if i == len(db.fileIds)-1 {
			db.activeFile.WOffset = offset
		}
	}
	return nil
}

func checkOptions(o Options) error {
	if o.DirPath == "" {
		return errors.New("DirPath 未配置")
	}

	if o.DataFileSize <= 0 {
		return errors.New("DataFileSize 配置错误")
	}

	return nil
}
