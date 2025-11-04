package file

import "io"

// Options
type Options struct {
	FID      uint64
	FileName string
	Dir      string
	Path     string
	Flag     int
	MaxSz    int
}

type CoreFile interface {
	Close() error
	Truncature(n int64) error
	ReName(name string) error
	NewReader(offset int) io.Reader
	Bytes(off, sz int) ([]byte, error)
	AllocateSlice(sz, offset int) ([]byte, int, error)
	Sync() error
	Delete() error
	Slice(offset int) []byte
}
