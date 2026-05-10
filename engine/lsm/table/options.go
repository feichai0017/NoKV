// Package table owns the on-disk SSTable format: open, read, build,
// iterate. It is policy-free and engine-free above the bytes — the lsm
// package supplies a Runtime that exposes the block/index cache and the
// per-engine option subset Table needs.
//
// Key boundaries:
//
//   - Table holds no back-pointer to lsm; it talks to the surrounding
//     engine through a small Runtime interface.
//   - Compression is an SSTable codec concept owned by this package.
//     The lsm Options struct re-exports the same constants for caller
//     convenience.
//   - Block / blockIterator are package-private; only Table and Builder
//     observe them.
package table

import (
	"github.com/feichai0017/NoKV/engine/lsm/cache"
	"github.com/feichai0017/NoKV/engine/vfs"
)

// Compression selects the SST data-block compression codec.
type Compression uint32

const (
	// CompressionNone stores blocks uncompressed.
	CompressionNone Compression = iota
	// CompressionSnappy stores blocks compressed with Snappy.
	CompressionSnappy
)

// Options is the subset of engine-level Options that Table reads at open
// time and Builder reads at write time. The lsm package mirrors a
// matching subset; see Runtime.Options.
type Options struct {
	// WorkDir is the directory containing SST files.
	WorkDir string
	// FS provides filesystem access (defaults to OS FS when nil).
	FS vfs.FS
	// SSTableMaxSize bounds builder output and bounds the mmap window.
	SSTableMaxSize int64
	// BlockSize controls how often the builder finishes a data block.
	BlockSize int64
	// BloomFalsePositive controls bloom-filter capacity at build time.
	BloomFalsePositive float64
	// BlockCompression selects the codec used for new SST data blocks.
	BlockCompression Compression
	// PrefixExtractor optionally derives a key prefix used by the prefix
	// bloom filter. nil disables prefix bloom.
	PrefixExtractor func([]byte) []byte
	// ManifestSync requests strict durability: builder writes go through a
	// temp file + rename instead of direct write to the final path.
	ManifestSync bool
}

// Runtime is the back-channel a Table needs from the surrounding lsm
// system. The lsm package implements it via *levelManager.
type Runtime interface {
	// Cache returns the shared block + index cache. Returning nil disables
	// caching for this Table; Get/Add become no-ops.
	Cache() *cache.Cache
	// Options returns the active engine Options snapshot. Table reads only
	// the documented fields; mutating the returned struct is undefined.
	Options() Options
}
