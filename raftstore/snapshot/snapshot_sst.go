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
	sstFormatVersion = 1
	sstArtifactKind  = "sst-inline-v1"
	sstSnapshotName  = "sst-snapshot.json"
	sstTablesDirName = "tables"
)

// SSTTableManifest describes one SST file inside a region snapshot artifact.
type SSTTableManifest struct {
	RelativePath string `json:"relative_path"`
	SmallestKey  []byte `json:"smallest_key"`
	LargestKey   []byte `json:"largest_key"`
	EntryCount   uint64 `json:"entry_count"`
	SizeBytes    uint64 `json:"size_bytes"`
	ValueBytes   uint64 `json:"value_bytes"`
}

// SSTManifest describes one region-scoped SST snapshot artifact.
type SSTManifest struct {
	FormatVersion uint32              `json:"format_version"`
	ArtifactKind  string              `json:"artifact_kind"`
	Region        raftmeta.RegionMeta `json:"region"`
	EntryCount    uint64              `json:"entry_count"`
	TableCount    uint64              `json:"table_count"`
	InlineValues  bool                `json:"inline_values"`
	Tables        []SSTTableManifest  `json:"tables"`
	CreatedAt     time.Time           `json:"created_at"`
}

// SSTExportResult reports the persisted manifest after a successful export.
type SSTExportResult struct {
	Manifest SSTManifest
}

// SSTSink installs external SST files into the target engine.
type SSTSink interface {
	ImportExternalSST(paths []string) error
}

// SSTImportResult reports one successful SST artifact install.
type SSTImportResult struct {
	Manifest       SSTManifest
	ImportedTables uint64
	ImportedBytes  uint64
}

// ExportSST persists one region snapshot artifact as one or more self-contained
// SST files. Phase one emits a single table with inline values only.
func ExportSST(src Source, dir string, region raftmeta.RegionMeta, opt *lsm.Options, fs vfs.FS) (*SSTExportResult, error) {
	if src == nil {
		return nil, fmt.Errorf("snapshot: export sst requires source")
	}
	if dir == "" {
		return nil, fmt.Errorf("snapshot: export sst requires dir")
	}
	if opt == nil {
		return nil, fmt.Errorf("snapshot: export sst requires lsm options")
	}
	fs = vfs.Ensure(fs)
	if _, err := fs.Stat(dir); err == nil {
		return nil, fmt.Errorf("snapshot: export sst target %s already exists", dir)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("snapshot: stat export sst target %s: %w", dir, err)
	}

	parent := filepath.Dir(dir)
	if err := fs.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("snapshot: create export sst parent %s: %w", parent, err)
	}
	tmpDir := fmt.Sprintf("%s.tmp.%d.%d", dir, os.Getpid(), time.Now().UnixNano())
	if err := fs.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, fmt.Errorf("snapshot: create export sst temp dir %s: %w", tmpDir, err)
	}
	success := false
	defer func() {
		if !success {
			_ = fs.RemoveAll(tmpDir)
		}
	}()

	entries, err := collectMaterializedEntries(src, region)
	if err != nil {
		return nil, err
	}
	manifest := SSTManifest{
		FormatVersion: sstFormatVersion,
		ArtifactKind:  sstArtifactKind,
		Region:        raftmeta.CloneRegionMeta(region),
		InlineValues:  true,
		CreatedAt:     time.Now().UTC(),
	}

	if len(entries) > 0 {
		tablesDir := filepath.Join(tmpDir, sstTablesDirName)
		if err := fs.MkdirAll(tablesDir, 0o755); err != nil {
			return nil, fmt.Errorf("snapshot: create sst tables dir %s: %w", tablesDir, err)
		}
		tablePath := filepath.Join(tablesDir, "000001.sst")
		tableMeta, err := lsm.BuildExternalSST(tablePath, entries, opt)
		if err != nil {
			return nil, fmt.Errorf("snapshot: build export sst table: %w", err)
		}
		manifest.EntryCount = tableMeta.EntryCount
		manifest.TableCount = 1
		manifest.Tables = []SSTTableManifest{{
			RelativePath: filepath.Join(sstTablesDirName, filepath.Base(tableMeta.Path)),
			SmallestKey:  kv.SafeCopy(nil, tableMeta.SmallestKey),
			LargestKey:   kv.SafeCopy(nil, tableMeta.LargestKey),
			EntryCount:   tableMeta.EntryCount,
			SizeBytes:    tableMeta.SizeBytes,
			ValueBytes:   tableMeta.ValueBytes,
		}}
	}

	if err := writeSSTManifest(filepath.Join(tmpDir, sstSnapshotName), &manifest, fs); err != nil {
		return nil, err
	}
	if err := vfs.SyncDir(fs, tmpDir); err != nil {
		return nil, fmt.Errorf("snapshot: sync export sst temp dir %s: %w", tmpDir, err)
	}
	if err := fs.Rename(tmpDir, dir); err != nil {
		return nil, fmt.Errorf("snapshot: publish export sst %s: %w", dir, err)
	}
	if err := vfs.SyncDir(fs, parent); err != nil {
		return nil, fmt.Errorf("snapshot: sync export sst parent %s: %w", parent, err)
	}
	success = true
	return &SSTExportResult{Manifest: manifest}, nil
}

// ReadSSTManifest loads one SST snapshot manifest from dir.
func ReadSSTManifest(dir string, fs vfs.FS) (SSTManifest, error) {
	fs = vfs.Ensure(fs)
	data, err := fs.ReadFile(filepath.Join(dir, sstSnapshotName))
	if err != nil {
		return SSTManifest{}, fmt.Errorf("snapshot: read sst manifest %s: %w", filepath.Join(dir, sstSnapshotName), err)
	}
	var manifest SSTManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return SSTManifest{}, fmt.Errorf("snapshot: decode sst manifest %s: %w", filepath.Join(dir, sstSnapshotName), err)
	}
	if manifest.FormatVersion != sstFormatVersion {
		return SSTManifest{}, fmt.Errorf("snapshot: unsupported sst format version %d", manifest.FormatVersion)
	}
	return manifest, nil
}

// ImportSST installs one SST snapshot artifact through the engine's external
// table ingest path. Phase one consumes the table files listed by the
// artifact manifest, matching ImportExternalSST rename-based semantics.
func ImportSST(dst SSTSink, dir string, fs vfs.FS) (*SSTImportResult, error) {
	if dst == nil {
		return nil, fmt.Errorf("snapshot: import sst requires sink")
	}
	fs = vfs.Ensure(fs)
	manifest, err := ReadSSTManifest(dir, fs)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(manifest.Tables))
	var importedBytes uint64
	for _, table := range manifest.Tables {
		if table.RelativePath == "" {
			return nil, fmt.Errorf("snapshot: sst manifest contains empty table path")
		}
		path := filepath.Join(dir, table.RelativePath)
		stat, err := fs.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("snapshot: stat sst table %s: %w", path, err)
		}
		paths = append(paths, path)
		importedBytes += uint64(stat.Size())
	}
	if len(paths) == 0 {
		return &SSTImportResult{Manifest: manifest}, nil
	}
	if err := dst.ImportExternalSST(paths); err != nil {
		return nil, fmt.Errorf("snapshot: import sst artifact from %s: %w", dir, err)
	}
	return &SSTImportResult{
		Manifest:       manifest,
		ImportedTables: uint64(len(paths)),
		ImportedBytes:  importedBytes,
	}, nil
}

func collectMaterializedEntries(src Source, region raftmeta.RegionMeta) ([]*kv.Entry, error) {
	bounds := raftmeta.CloneRegionMeta(region)
	iter := src.NewInternalIterator(&utils.Options{
		IsAsc:      true,
		LowerBound: bounds.StartKey,
		UpperBound: bounds.EndKey,
	})
	if iter == nil {
		return nil, fmt.Errorf("snapshot: nil internal iterator")
	}
	defer func() { _ = iter.Close() }()

	var entries []*kv.Entry
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			continue
		}
		_, userKey, _, ok := kv.SplitInternalKey(item.Entry().Key)
		if !ok {
			return nil, fmt.Errorf("snapshot: invalid internal key")
		}
		if !keyInRegion(bounds, userKey) {
			continue
		}
		materialized, err := src.MaterializeInternalEntry(item.Entry())
		if err != nil {
			return nil, fmt.Errorf("snapshot: materialize entry: %w", err)
		}
		entries = append(entries, materialized)
	}
	return entries, nil
}

func writeSSTManifest(path string, manifest *SSTManifest, fs vfs.FS) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot: encode sst manifest %s: %w", path, err)
	}
	f, err := fs.OpenFileHandle(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("snapshot: create sst manifest %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: write sst manifest %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: sync sst manifest %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("snapshot: close sst manifest %s: %w", path, err)
	}
	return nil
}
