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
// TODO maybe add more options in future
type Options struct {
	Prefix []byte
	IsAsc  bool
}