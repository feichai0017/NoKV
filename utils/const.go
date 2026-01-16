package utils

import (
	"math"
	"os"
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
	// MaxHeaderSize is the worst-case size for uvarint encoding.
	MaxHeaderSize            = 21
	VlogHeaderSize           = 0
	MaxVlogFileSize   uint32 = math.MaxUint32
	Mi                int64  = 1 << 20
	KVWriteChCapacity        = 1000
)

// codec
var (
	MagicText    = [4]byte{'N', 'O', 'K', 'V'}
	MagicVersion = uint32(1)
)
