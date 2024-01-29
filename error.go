package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("传入了空key")
	ErrIndexUpdateFailed = errors.New("内存索引更新失败")
	ErrKeyNotFound       = errors.New("找不着key")
	ErrDataFileNotFound  = errors.New("找不着数据文件")
)
