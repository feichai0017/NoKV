package manifest

import "errors"

// ErrUnsupportedValueLogManifest is returned when a manifest contains
// historical value-log edits. The value-log subsystem has been removed, so such
// workdirs must be rebuilt or migrated before opening with this engine.
var ErrUnsupportedValueLogManifest = errors.New("manifest contains removed value-log edits")

// FileMeta describes an SST file.
type FileMeta struct {
	Level     int
	FileID    uint64
	Size      uint64
	Smallest  []byte
	Largest   []byte
	CreatedAt uint64
	ValueSize uint64
	Landing   bool
}

// Version represents current storage manifest state. Recovery drives
// WAL replay per-shard via wal.Manager.Replay using each shard's own
// segment inventory; the per-table EditAddFile.LogSeg carries the
// "this WAL segment is now in an SST" signal. There is no manifest-
// level WAL anchor.
type Version struct {
	Levels map[int][]FileMeta
}

// Edit operation types.
type EditType uint8

const (
	EditAddFile    EditType = 0
	EditDeleteFile EditType = 1

	editValueLogHead   EditType = 2
	editDeleteValueLog EditType = 3
	editUpdateValueLog EditType = 4
)

// Edit describes a single metadata operation. LogSeg is the WAL
// segment identifier carried with EditAddFile so recovery can skip
// segments that have already been flushed into SSTs.
type Edit struct {
	Type   EditType
	File   *FileMeta
	LogSeg uint32
}
