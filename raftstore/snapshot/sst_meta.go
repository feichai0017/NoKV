package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
)

const (
	sstVersion       = 1
	sstSnapshotName  = "sst-snapshot.json"
	sstTablesDirName = "tables"
)

// Source exports detached internal entries over a bounded key range.
type Source interface {
	NewInternalIterator(opt *utils.Options) utils.Iterator
	MaterializeInternalEntry(src *kv.Entry) (*kv.Entry, error)
}

// SSTTableMeta describes one SST file inside a region snapshot.
type SSTTableMeta struct {
	RelativePath string `json:"relative_path"`
	SmallestKey  []byte `json:"smallest_key"`
	LargestKey   []byte `json:"largest_key"`
	EntryCount   uint64 `json:"entry_count"`
	SizeBytes    uint64 `json:"size_bytes"`
	ValueBytes   uint64 `json:"value_bytes"`
}

// SSTMeta describes one region-scoped SST snapshot.
type SSTMeta struct {
	Version      uint32              `json:"version"`
	Region       raftmeta.RegionMeta `json:"region"`
	EntryCount   uint64              `json:"entry_count"`
	TableCount   uint64              `json:"table_count"`
	InlineValues bool                `json:"inline_values"`
	Tables       []SSTTableMeta      `json:"tables"`
	CreatedAt    time.Time           `json:"created_at"`
}

// SSTExportResult reports the persisted snapshot metadata after a successful export.
type SSTExportResult struct {
	Meta SSTMeta
}

// SSTSink installs external SST files into the target engine and can roll back
// a completed ingest before higher-level metadata is published.
type SSTSink interface {
	ImportExternalSST(paths []string) (*lsm.ExternalSSTImportResult, error)
	RollbackExternalSST(fileIDs []uint64) error
}

// SnapshotIO exposes region snapshot export/install helpers.
type SnapshotIO interface {
	ExportSnapshot(region raftmeta.RegionMeta) ([]byte, error)
	InstallSnapshot(payload []byte) (raftmeta.RegionMeta, error)
}

// Engine is the full snapshot bridge exposed by the storage engine to
// raftstore wiring.
type Engine interface {
	Source
	SSTSink
	SnapshotIO
	SSTOptions() *lsm.Options
}

// SSTImportResult reports one successful SST snapshot install.
type SSTImportResult struct {
	Meta            SSTMeta
	ImportedTables  uint64
	ImportedBytes   uint64
	ImportedFileIDs []uint64
}

// ReadSSTMeta loads one SST snapshot metadata file from dir.
func ReadSSTMeta(dir string, fs vfs.FS) (SSTMeta, error) {
	fs = vfs.Ensure(fs)
	data, err := fs.ReadFile(filepath.Join(dir, sstSnapshotName))
	if err != nil {
		return SSTMeta{}, fmt.Errorf("snapshot: read sst meta %s: %w", filepath.Join(dir, sstSnapshotName), err)
	}
	var meta SSTMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return SSTMeta{}, fmt.Errorf("snapshot: decode sst meta %s: %w", filepath.Join(dir, sstSnapshotName), err)
	}
	if meta.Version != sstVersion {
		return SSTMeta{}, fmt.Errorf("snapshot: unsupported sst version %d", meta.Version)
	}
	return meta, nil
}

func writeSSTMeta(path string, meta *SSTMeta, fs vfs.FS) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot: encode sst meta %s: %w", path, err)
	}
	f, err := fs.OpenFileHandle(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("snapshot: create sst meta %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: write sst meta %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: sync sst meta %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("snapshot: close sst meta %s: %w", path, err)
	}
	return nil
}
