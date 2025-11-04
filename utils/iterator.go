package utils

import "github.com/feichai0017/NoKV/kv"

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
	Entry() *kv.Entry
}

// Options _
type Options struct {
	Prefix []byte
	IsAsc  bool
	// OnlyUseKey instructs iterators to avoid materialising value log entries
	// eagerly. Callers should rely on Item.ValueCopy when value access is
	// required. This keeps the default behaviour (false) for existing users.
	OnlyUseKey bool
}
