// Package file provides low-level file and mmap primitives shared by WAL, vlog, and SST layers.
package file

import "io"

import "github.com/feichai0017/NoKV/vfs"

// Options
type Options struct {
	FID      uint64
	FileName string
	Dir      string
	Path     string
	Flag     int
	MaxSz    int
	FS       vfs.FS
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
