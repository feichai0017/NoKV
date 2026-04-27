package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/feichai0017/NoKV/engine/file"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
)

// ExternalSSTMeta describes one snapshot/export SST produced outside the live
// level manager. These files are intended to be consumed later via
// ImportExternalSST.
type ExternalSSTMeta struct {
	Path        string `json:"path"`
	SmallestKey []byte `json:"smallest_key"`
	LargestKey  []byte `json:"largest_key"`
	EntryCount  uint64 `json:"entry_count"`
	SizeBytes   uint64 `json:"size_bytes"`
	ValueBytes  uint64 `json:"value_bytes"`
}

// ExternalSSTImportResult reports the live LSM state created by one successful
// external SST ingest operation.
type ExternalSSTImportResult struct {
	FileIDs       []uint64 `json:"file_ids"`
	ImportedBytes uint64   `json:"imported_bytes"`
}

// ExternalSSTOptions returns a normalized clone of the live LSM configuration
// for building external SST files compatible with this instance.
func (lsm *LSM) ExternalSSTOptions() *Options {
	if lsm == nil || lsm.option == nil || lsm.closed.Load() {
		return nil
	}
	opt := lsm.option.Clone()
	opt.NormalizeInPlace()
	return opt
}

// ExportExternalSST materializes one standalone SST file that can later be
// imported through ImportExternalSST. Entries must already be detached and
// sorted by internal key order.
func ExportExternalSST(path string, entries []*kv.Entry, opt *Options) (_ *ExternalSSTMeta, err error) {
	if path == "" {
		return nil, fmt.Errorf("lsm: external sst path required")
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("lsm: external sst requires at least one entry")
	}
	if opt == nil {
		return nil, fmt.Errorf("lsm: external sst requires options")
	}

	cfg := opt.Clone()
	cfg.NormalizeInPlace()
	fs := vfs.Ensure(cfg.FS)

	parent := filepath.Dir(path)
	if err := fs.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("lsm: create external sst parent %s: %w", parent, err)
	}
	if _, err := fs.Stat(path); err == nil {
		return nil, fmt.Errorf("lsm: external sst target already exists: %s", path)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("lsm: stat external sst target %s: %w", path, err)
	}

	builder := newTableBuiler(cfg)
	var valueBytes uint64
	for i, entry := range entries {
		if entry == nil {
			return nil, fmt.Errorf("lsm: nil external sst entry at index %d", i)
		}
		if len(entry.Key) == 0 {
			return nil, fmt.Errorf("lsm: empty external sst entry key at index %d", i)
		}
		if i > 0 && kv.CompareInternalKeys(entries[i-1].Key, entry.Key) >= 0 {
			return nil, fmt.Errorf("lsm: external sst entries not strictly increasing at index %d", i)
		}
		vlen := entryValueLen(entry)
		builder.AddKeyWithLen(entry, vlen)
		valueBytes += uint64(vlen)
	}

	build, err := builder.done()
	if err != nil {
		return nil, fmt.Errorf("lsm: build external sst %s: %w", path, err)
	}
	if build.size == 0 {
		return nil, fmt.Errorf("lsm: external sst build for %s is empty", path)
	}

	tmpPath := fmt.Sprintf("%s.tmp.%d.%d", path, os.Getpid(), time.Now().UnixNano())
	tmp := file.OpenSStable(&file.Options{
		FileName: tmpPath,
		Flag:     os.O_CREATE | os.O_EXCL | os.O_RDWR,
		MaxSz:    build.size,
		FS:       cfg.FS,
	})
	if tmp == nil {
		return nil, fmt.Errorf("lsm: open external sst %s", tmpPath)
	}

	renamed := false
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		if err != nil && !renamed {
			_ = fs.Remove(tmpPath)
		}
	}()

	if err := writeBuildDataToSST(tmp, build); err != nil {
		return nil, fmt.Errorf("lsm: write external sst %s: %w", tmpPath, err)
	}
	if err := tmp.Sync(); err != nil {
		return nil, fmt.Errorf("lsm: sync external sst %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("lsm: close external sst %s: %w", tmpPath, err)
	}
	closed = true
	if err := fs.RenameNoReplace(tmpPath, path); err != nil {
		return nil, fmt.Errorf("lsm: publish external sst %s: %w", path, err)
	}
	renamed = true
	if err := vfs.SyncDir(fs, parent); err != nil {
		return nil, fmt.Errorf("lsm: sync external sst parent %s: %w", parent, err)
	}

	stat, err := fs.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("lsm: stat external sst %s: %w", path, err)
	}
	return &ExternalSSTMeta{
		Path:        path,
		SmallestKey: kv.SafeCopy(nil, entries[0].Key),
		LargestKey:  kv.SafeCopy(nil, entries[len(entries)-1].Key),
		EntryCount:  uint64(len(entries)),
		SizeBytes:   uint64(stat.Size()),
		ValueBytes:  valueBytes,
	}, nil
}

// ImportExternalSST ingests externally built SST files into the live LSM and
// reports the file IDs created by the ingest. Callers can later use
// RollbackExternalSST to roll back a completed ingest before the new state is
// published.
func (lsm *LSM) ImportExternalSST(paths []string) (*ExternalSSTImportResult, error) {
	if lsm == nil {
		return nil, ErrLSMNil
	}
	if lsm.closed.Load() {
		return nil, ErrLSMClosed
	}
	if len(paths) == 0 {
		return &ExternalSSTImportResult{}, nil
	}

	lsm.closer.Add(1)
	defer lsm.closer.Done()

	return lsm.levels.importExternalSST(paths)
}

// RollbackExternalSST removes previously imported SST files from the live LSM.
// This is intended for snapshot-install rollback before the imported tables are
// published to higher-level runtime metadata.
func (lsm *LSM) RollbackExternalSST(fileIDs []uint64) error {
	if lsm == nil {
		return ErrLSMNil
	}
	if lsm.closed.Load() {
		return ErrLSMClosed
	}
	if len(fileIDs) == 0 {
		return nil
	}

	lsm.closer.Add(1)
	defer lsm.closer.Done()

	return lsm.levels.removeExternalSST(fileIDs)
}

func checkTablesOverlap(tables []*table) error {
	sorted := make([]*table, len(tables))
	copy(sorted, tables)

	sort.Slice(sorted, func(i, j int) bool {
		return kv.CompareBaseKeys(sorted[i].MinKey(), sorted[j].MinKey()) < 0
	})

	for i := 1; i < len(sorted); i++ {
		prev := sorted[i-1]
		curr := sorted[i]
		if kv.CompareBaseKeys(prev.MaxKey(), curr.MinKey()) >= 0 {
			return fmt.Errorf("imported SSTs have key range overlap: fid=%d <-> fid=%d",
				prev.fid, curr.fid)
		}
	}
	return nil
}

// checkTablesOverlapWithL0Locked checks imported tables against existing L0
// tables. Caller must hold l0.Lock().
func (lm *levelManager) checkTablesOverlapWithL0Locked(tables []*table) error {
	l0 := lm.levels[0]

	for _, tbl := range tables {
		for _, existing := range l0.tables {
			if existing == nil {
				continue
			}
			if kv.CompareBaseKeys(tbl.MinKey(), existing.MaxKey()) <= 0 &&
				kv.CompareBaseKeys(tbl.MaxKey(), existing.MinKey()) >= 0 {
				return fmt.Errorf("SST(fid=%d) overlaps with L0 existing table(fid=%d)",
					tbl.fid, existing.fid)
			}
		}
	}
	return nil
}

func (lm *levelManager) importExternalSST(paths []string) (*ExternalSSTImportResult, error) {
	fs := vfs.Ensure(lm.opt.FS)
	workDir := lm.opt.WorkDir
	var (
		importedTables []*table
		importedMetas  []*manifest.FileMeta
		tempFIDs       []uint64
		pathMappings   = make(map[string]string)
		manifestLogged bool
	)

	rollback := func() {
		for _, tbl := range importedTables {
			if tbl != nil {
				lm.cache.delIndex(tbl.fid)
				_ = tbl.closeHandle()
			}
		}
		for sourcePath, targetPath := range pathMappings {
			if _, err := fs.Stat(targetPath); err == nil {
				_ = fs.Rename(targetPath, sourcePath)
			}
		}
		for _, fid := range tempFIDs {
			sstPath := vfs.FileNameSSTable(workDir, fid)
			if _, err := fs.Stat(sstPath); err == nil {
				_ = fs.Remove(sstPath)
			}
		}

		if !manifestLogged {
			return
		}
		rollbackEdits := make([]manifest.Edit, len(importedMetas))
		for i, meta := range importedMetas {
			rollbackEdits[i] = manifest.Edit{
				Type: manifest.EditDeleteFile,
				File: meta,
			}
		}
		if len(rollbackEdits) == 0 {
			return
		}
		if err := lm.manifestMgr.LogEdits(rollbackEdits...); err != nil {
			lm.getLogger().Error("failed to log import rollback edits", "error", err, "files", len(rollbackEdits))
		}
	}

	for _, path := range paths {
		stat, err := fs.Stat(path)
		if err != nil {
			rollback()
			return nil, fmt.Errorf("invalid external SST: %s, err: %w", path, err)
		}
		if stat.IsDir() {
			rollback()
			return nil, fmt.Errorf("external SST is a directory: %s", path)
		}
		if !strings.HasSuffix(path, ".sst") {
			rollback()
			return nil, fmt.Errorf("external file is not an SST (missing .sst suffix): %s", path)
		}

		tempFID := lm.maxFID.Add(1)
		tempFIDs = append(tempFIDs, tempFID)
		targetPath := vfs.FileNameSSTable(workDir, tempFID)

		if _, err := fs.Stat(targetPath); err == nil {
			rollback()
			return nil, fmt.Errorf("target SST path already exists: %s", targetPath)
		}
		if err := fs.Rename(path, targetPath); err != nil {
			rollback()
			return nil, fmt.Errorf("failed to move external SST: %s -> %s, err: %w", path, targetPath, err)
		}
		pathMappings[path] = targetPath

		tbl, err := openTable(lm, targetPath, nil)
		if err != nil {
			rollback()
			return nil, fmt.Errorf("open imported sst failed: %s, err: %w", targetPath, err)
		}
		importedTables = append(importedTables, tbl)
	}

	if err := checkTablesOverlap(importedTables); err != nil {
		rollback()
		return nil, fmt.Errorf("imported ssts overlap: %w", err)
	}

	l0 := lm.levels[0]
	l0.Lock()
	defer l0.Unlock()

	if err := lm.checkTablesOverlapWithL0Locked(importedTables); err != nil {
		rollback()
		return nil, fmt.Errorf("overlap with L0: %w", err)
	}

	for _, tbl := range importedTables {
		meta := &manifest.FileMeta{
			Level:     0,
			FileID:    tbl.fid,
			Size:      uint64(tbl.Size()),
			Smallest:  kv.SafeCopy(nil, tbl.MinKey()),
			Largest:   kv.SafeCopy(nil, tbl.MaxKey()),
			CreatedAt: uint64(time.Now().Unix()),
			ValueSize: tbl.ValueSize(),
			Staging:   false,
		}
		importedMetas = append(importedMetas, meta)
	}

	edits := make([]manifest.Edit, len(importedMetas))
	for i, meta := range importedMetas {
		edits[i] = manifest.Edit{
			Type: manifest.EditAddFile,
			File: meta,
		}
	}

	if lm.opt.ManifestSync {
		if err := vfs.SyncDir(lm.opt.FS, workDir); err != nil {
			rollback()
			return nil, fmt.Errorf("sync work dir failed: %w", err)
		}
	}

	manifestLogged = true
	if err := lm.manifestMgr.LogEdits(edits...); err != nil {
		rollback()
		return nil, fmt.Errorf("log manifest edits failed: %w", err)
	}

	result := &ExternalSSTImportResult{FileIDs: make([]uint64, 0, len(importedTables))}
	for _, tbl := range importedTables {
		if tbl == nil {
			continue
		}
		tbl.setLevel(l0.levelNum)
		l0.tables = append(l0.tables, tbl)
		l0.totalSize += tbl.Size()
		l0.totalStaleSize += int64(tbl.StaleDataSize())
		l0.totalValueSize += int64(tbl.ValueSize())
		result.FileIDs = append(result.FileIDs, tbl.fid)
		result.ImportedBytes += uint64(tbl.Size())
	}
	l0.sortTablesLocked()
	l0.rebuildRangeFilterLocked()
	if lm.lsm != nil {
		lm.lsm.clearNegativeCache()
	}

	return result, nil
}

func (lm *levelManager) removeExternalSST(fileIDs []uint64) error {
	if len(fileIDs) == 0 {
		return nil
	}

	l0 := lm.levels[0]
	l0.RLock()
	tablesByID := make(map[uint64]*table, len(fileIDs))
	for _, tbl := range l0.tables {
		if tbl != nil {
			tablesByID[tbl.fid] = tbl
		}
	}
	toDelete := make([]*table, 0, len(fileIDs))
	edits := make([]manifest.Edit, 0, len(fileIDs))
	for _, fid := range fileIDs {
		tbl, ok := tablesByID[fid]
		if !ok {
			continue
		}
		toDelete = append(toDelete, tbl)
		edits = append(edits, manifest.Edit{
			Type: manifest.EditDeleteFile,
			File: &manifest.FileMeta{Level: 0, FileID: fid},
		})
	}
	l0.RUnlock()

	if len(toDelete) == 0 {
		return nil
	}
	if err := lm.manifestMgr.LogEdits(edits...); err != nil {
		return fmt.Errorf("log external sst delete edits failed: %w", err)
	}
	if err := l0.deleteTables(toDelete); err != nil {
		return fmt.Errorf("delete external sst tables: %w", err)
	}
	return nil
}
