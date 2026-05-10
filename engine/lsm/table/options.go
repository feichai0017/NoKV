// Package table owns the on-disk SSTable format: open, read, build, iterate.
package table

import (
	"github.com/feichai0017/NoKV/engine/lsm/cache"
	"github.com/feichai0017/NoKV/engine/vfs"
)

// Compression selects the SST data-block compression codec.
type Compression uint32

const (
	CompressionNone Compression = iota
	CompressionSnappy
)

// Options configures Table reads and Builder writes.
type Options struct {
	WorkDir            string
	FS                 vfs.FS
	SSTableMaxSize     int64
	BlockSize          int64
	BloomFalsePositive float64
	BlockCompression   Compression
	PrefixExtractor    func([]byte) []byte
	ManifestSync       bool
}

// Runtime exposes the cache and options a Table needs at runtime.
type Runtime interface {
	Cache() *cache.Cache
	Options() Options
}
