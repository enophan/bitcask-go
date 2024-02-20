package bitcask_go

import (
	"bitcask/data"
	"bitcask/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 给用户提供的接口

const SeqNoKey = "seq.no"

type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
	options    Options
	index      index.Indexer
	fileIds    []int  // 仅用于加载索引
	seqNo      uint64 // 事务序列号
	isMerging  bool

	// 因为B+树模式里，获取seqNo比较麻烦，
	// 所以选择了将seqNo保存在专门的文件seqNoFile中，
	// 如果序列号文件不存，那就直接禁止WriteBatch功能
	// 一个比较简单粗暴的方法
	//
	// 序列号文件为什么会不存在，因为close方法中执行着保存序列号文件的逻辑，
	// 而之前用户没有调用DB.Close()方法关闭数据库的话，就没办法读取序列号，
	// 此时使用WriteBatch功能就会出错，因此需要在DB.NewWriteBatch()时会判断这个属性，
	// 以此决定是否禁用WriteBatch功能，避免错误产生
	seqFileExists bool
	isInitial     bool
}

func Open(options Options) (*DB, error) {
	// 检验配置项有无
	// 加载数据文件
	// 加载索引信息

	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	db := &DB{
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		options:    options,
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrite),
		isInitial:  isInitial,
	}

	// 加载merge文件
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if options.IndexType != BPlusTree {
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

	}

	if db.options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WOffset = size
		}
	}

	return db, nil

}

func (db *DB) Put(key []byte, value []byte) error {
	// 添加索引，更新索引
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
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
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	i := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var index int
	for i.Rewind(); i.Valid(); i.Next() {
		keys[index] = i.Key()
		index++
	}
	return keys
}

// Fold 遍历所有数据，并执行用户指定的操作，用户操作返回 false 时退出
func (db *DB) Fold(f func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	i := db.index.Iterator(false)
	defer i.Close()
	for i.Rewind(); i.Valid(); i.Next() {
		value, err := db.getValueByPostion(i.Value())
		if err != nil {
			return err
		}
		if !f(i.Key(), value) {
			break
		}
	}

	return nil
}

// Sync TODO
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

// Close 关闭数据库
func (db *DB) Close() error {
	// 关闭或与文件和旧数据文件

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.index.Close(); err != nil {
		return err
	}

	file, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}

	logRecord := &data.LogRecord{
		Key:   []byte(SeqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}

	encLogRecord, _ := data.EncodeLogRecord(logRecord)
	if err := file.Write(encLogRecord); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
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

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 数据写入活跃文件，返回地址信息
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 若未初始化活跃文件，则新建
	// 对数据二进制编码，调用DataFile的Write，将数据写入活跃文件
	// 写入此条数据之后，若是超出活跃文件阈值，则创建新活跃文件
	// 最后，根据编码后的偏移值计算出数据位置并返回

	// 除此之外还可以给用户提供其他选择，比如是否当即持久化数据
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

	update := func(k []byte, t data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if t == data.LogRecordNormal {
			ok = db.index.Put(k, pos)
		} else {
			ok = db.index.Delete(k)
		}
		if !ok {
			panic("启动更新索引时失败")
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)

	var currentSeqNo = nonTransactionSeqNo

	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		// 一条一条地取文件里的数据
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

			realKey, seqNo := parseLogRecordKey(logRecord.Key)

			// 更新索引
			if seqNo == nonTransactionSeqNo {
				update(realKey, logRecord.Type, logRecordPos)
			} else {
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, tRecord := range transactionRecords[seqNo] {
						update(tRecord.Record.Key, tRecord.Record.Type, tRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 事务序列号更新
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			offset += size

		}

		if i == len(db.fileIds)-1 {
			db.activeFile.WOffset = offset
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

func (db *DB) loadSeqNo() error {
	filename := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(filename); err != nil {
		return err
	}

	seqNoFile, err := data.OpenSeqNoFile(filename)
	if err != nil {
		return err
	}

	encLogRecord, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	seqNo, err := strconv.ParseUint(string(encLogRecord.Key), 10, 64)
	if err != nil {
		return err
	}

	db.seqNo = seqNo
	db.seqFileExists = true
	return os.Remove(filename)
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
