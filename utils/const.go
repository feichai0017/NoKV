package utils

import (
	"math"
	"os"

	"github.com/feichai0017/NoKV/kv"
)

const (
	// MaxLevelNum _
	MaxLevelNum = 7
	// DefaultValueThreshold _
	DefaultValueThreshold = 1024
)

// file
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666
	MaxValueLogSize                   = 10 << 20
	// This is O_DSYNC (datasync) on platforms that support it -- see file_unix.go
	datasyncFileFlag = 0x0
	// 基于可变长编码,其最可能的编码
	MaxHeaderSize            = 21
	VlogHeaderSize           = 0
	MaxVlogFileSize   uint32 = math.MaxUint32
	Mi                int64  = 1 << 20
	KVWriteChCapacity        = 1000
)

// meta
const (
	BitDelete       = kv.BitDelete
	BitValuePointer = kv.BitValuePointer
)

// codec
var (
	MagicText    = [4]byte{'H', 'A', 'R', 'D'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = kv.CastagnoliCrcTable
)
