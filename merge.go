package bitcask_go

import (
	"bitcask/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

func (db *DB) Merge() error {
	// 检查activeFile是否存在
	// 检查此刻是否有进程正在merge
	// 持久化当前activeFile，并加入olderFiles的行列等待merge
	// 记录未持久化的FileId，此ID值为activeFile.FileId+1，以后要用做区分
	// 打开一个新activeFile，避免影响其他进程操作
	//
	// 进入正题
	// 将所有olderFiles移交至mergeFiles暂存，等待下一步操作
	//
	// 创建一个mergeDB实例，在此实例上操作
	// 通过索引，把每一个文件中的数据重新写入新的mergeFile中，然后增加hint索引文件
	// 最后尾部添加“完成文件”来标识这一系列merge已完成

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeInProgress
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	nonMergeFileId := db.activeFile.FileId + 1
	db.olderFiles[db.activeFile.FileId] = db.activeFile

	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}

	db.mu.Unlock()

	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergeDirPath()

	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	mergeOpts := db.options
	mergeOpts.SyncWrite = false
	mergeOpts.DirPath = mergePath
	mergeDB, err := Open(mergeOpts)
	if err != nil {
		return err
	}

	hintFile, err := data.OpenHintFile(mergeDB.options.DirPath)
	if err != nil {
		return err
	}

	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)

			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset {
				// 写入数据
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 录入索引
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}

	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 标识完成
	mergeFinFile, err := data.OpenMergeFinishedFile(mergeDB.options.DirPath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) getMergeDirPath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// 在启动数据库时调用，此函数负责将临时merge目录内的所有文件移动到原始目录下
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergeDirPath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 遍历检查merge是否完成
	var mergeFinished bool
	var fileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		// 旧序列号文件丢掉就行
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		fileNames = append(fileNames, entry.Name())
	}
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// 移除原始目录下的旧数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	for _, fileName := range fileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		logRecordPos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, logRecordPos)

		offset += size
	}
	return nil
}

// dirPath：merge文件夹路径
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileId), nil
}
