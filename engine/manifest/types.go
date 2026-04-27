package manifest

// FileMeta describes an SST file.
type FileMeta struct {
	Level     int
	FileID    uint64
	Size      uint64
	Smallest  []byte
	Largest   []byte
	CreatedAt uint64
	ValueSize uint64
	Ingest    bool
}

// ValueLogMeta describes a value log segment.
type ValueLogMeta struct {
	Bucket uint32
	FileID uint32
	Offset uint64
	Valid  bool
}

// ValueLogID identifies a value log segment within a bucket.
type ValueLogID struct {
	Bucket uint32
	FileID uint32
}

// Version represents current storage manifest state. Recovery drives
// WAL replay per-shard via wal.Manager.Replay using each shard's own
// segment inventory; the per-table EditAddFile.LogSeg carries the
// "this WAL segment is now in an SST" signal. There is no manifest-
// level WAL anchor.
type Version struct {
	Levels       map[int][]FileMeta
	ValueLogs    map[ValueLogID]ValueLogMeta
	ValueLogHead map[uint32]ValueLogMeta
}

// Edit operation types.
type EditType uint8

const (
	EditAddFile EditType = iota
	EditDeleteFile
	EditValueLogHead
	EditDeleteValueLog
	EditUpdateValueLog
)

// Edit describes a single metadata operation. LogSeg is the WAL
// segment identifier carried with EditAddFile so recovery can skip
// segments that have already been flushed into SSTs.
type Edit struct {
	Type     EditType
	File     *FileMeta
	LogSeg   uint32
	ValueLog *ValueLogMeta
}
