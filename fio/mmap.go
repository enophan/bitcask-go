package fio

import "golang.org/x/exp/mmap"

type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{
		readerAt: readerAt,
	}, nil
}

func (mm *MMap) Read(b []byte, offset int64) (int, error) {
	return mm.readerAt.ReadAt(b, offset)
}

func (mm *MMap) Write([]byte) (int, error) {
	panic("暂无此功能")
}

func (mm *MMap) Sync() error {
	panic("暂无此功能")
}

func (mm *MMap) Close() error {
	return mm.readerAt.Close()
}

func (mm *MMap) Size() (int64, error) {
	return int64(mm.readerAt.Len()), nil
}
