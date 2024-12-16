package utils

// Iterator 迭代器
type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
	Seek(key []byte)
}

// Item _
type Item interface {
	Entry() *Entry
}

// Options _
// TODO 可能被重构
type Options struct {
	Prefix []byte
	IsAsc  bool
}