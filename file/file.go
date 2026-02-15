// Package file provides low-level file and mmap primitives shared by WAL, vlog, and SST layers.
package file

import "github.com/feichai0017/NoKV/vfs"

// Options controls file opening parameters used by storage primitives.
type Options struct {
	FID      uint64
	FileName string
	Dir      string
	Flag     int
	MaxSz    int
	FS       vfs.FS
}
