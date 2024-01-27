package data

type LogRecordPos struct {
	// 文件名 int64 可能比较大，比较浪费 int32 比较合理
	Fid uint32
	// 存储值在这一条目中的偏移位置
	Offset int64
}
