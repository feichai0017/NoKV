package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/feichai0017/NoKV/kv"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
)

const (
	// FormatVersion identifies the logical region snapshot artifact layout.
	FormatVersion = 1

	logicalSnapshotName = "logical-snapshot.json"
	entriesName         = "entries.bin"
	payloadMagic        = "NKVSNP1\x00"
)

// Source exports detached internal entries over a bounded key range.
type Source interface {
	NewInternalIterator(opt *utils.Options) utils.Iterator
	MaterializeInternalEntry(src *kv.Entry) (*kv.Entry, error)
}

// Sink imports detached internal entries through the regular write path.
type Sink interface {
	ApplyInternalEntries(entries []*kv.Entry) error
}

// LogicalSnapshotManifest describes one logical region snapshot artifact.
type LogicalSnapshotManifest struct {
	FormatVersion uint32              `json:"format_version"`
	Region        raftmeta.RegionMeta `json:"region"`
	EntryCount    uint64              `json:"entry_count"`
	PayloadBytes  uint64              `json:"payload_bytes"`
	PayloadCRC32  uint32              `json:"payload_crc32"`
	CreatedAt     time.Time           `json:"created_at"`
}

// LogicalSnapshotExportResult reports the persisted manifest after a successful export.
type LogicalSnapshotExportResult struct {
	Manifest LogicalSnapshotManifest
}

// LogicalSnapshotImportResult reports the imported manifest and observed payload counters.
type LogicalSnapshotImportResult struct {
	Manifest     LogicalSnapshotManifest
	Imported     uint64
	PayloadBytes uint64
}

// ExportLogicalSnapshotPayload materializes one logical region snapshot into a transport-safe
// in-memory payload suitable for raft snapshot Data.
func ExportLogicalSnapshotPayload(src Source, region raftmeta.RegionMeta) ([]byte, LogicalSnapshotManifest, error) {
	if src == nil {
		return nil, LogicalSnapshotManifest{}, fmt.Errorf("snapshot: export requires source")
	}
	var entries bytes.Buffer
	manifest, err := exportEntriesToWriter(src, &entries, region)
	if err != nil {
		return nil, LogicalSnapshotManifest{}, err
	}
	payload, err := encodePayload(*manifest, entries.Bytes())
	if err != nil {
		return nil, LogicalSnapshotManifest{}, err
	}
	return payload, *manifest, nil
}

// ImportLogicalSnapshotPayload replays one in-memory logical region snapshot payload.
func ImportLogicalSnapshotPayload(dst Sink, payload []byte) (*LogicalSnapshotImportResult, error) {
	if dst == nil {
		return nil, fmt.Errorf("snapshot: import requires sink")
	}
	manifest, payloadEntries, err := decodePayload(payload)
	if err != nil {
		return nil, err
	}
	return importEntries(dst, manifest, bytes.NewReader(payloadEntries))
}

// ReadLogicalSnapshotPayloadManifest decodes only the manifest carried by one in-memory
// logical region snapshot payload.
func ReadLogicalSnapshotPayloadManifest(payload []byte) (LogicalSnapshotManifest, error) {
	manifest, _, err := decodePayload(payload)
	return manifest, err
}

// ExportLogicalSnapshot persists one logical region snapshot artifact into dir.
// The target is written into a sibling temporary directory and atomically
// renamed into place on success.
func ExportLogicalSnapshot(src Source, dir string, region raftmeta.RegionMeta, fs vfs.FS) (*LogicalSnapshotExportResult, error) {
	if src == nil {
		return nil, fmt.Errorf("snapshot: export requires source")
	}
	if dir == "" {
		return nil, fmt.Errorf("snapshot: export requires dir")
	}
	fs = vfs.Ensure(fs)
	if _, err := fs.Stat(dir); err == nil {
		return nil, fmt.Errorf("snapshot: export target %s already exists", dir)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("snapshot: stat export target %s: %w", dir, err)
	}

	parent := filepath.Dir(dir)
	if err := fs.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("snapshot: create parent dir %s: %w", parent, err)
	}

	tmpDir := fmt.Sprintf("%s.tmp.%d.%d", dir, os.Getpid(), time.Now().UnixNano())
	if err := fs.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, fmt.Errorf("snapshot: create temp dir %s: %w", tmpDir, err)
	}
	success := false
	defer func() {
		if !success {
			_ = fs.RemoveAll(tmpDir)
		}
	}()

	manifest, err := exportEntries(src, filepath.Join(tmpDir, entriesName), region, fs)
	if err != nil {
		return nil, err
	}
	if err := writeManifest(filepath.Join(tmpDir, logicalSnapshotName), manifest, fs); err != nil {
		return nil, err
	}
	if err := vfs.SyncDir(fs, tmpDir); err != nil {
		return nil, fmt.Errorf("snapshot: sync temp dir %s: %w", tmpDir, err)
	}
	if err := fs.Rename(tmpDir, dir); err != nil {
		return nil, fmt.Errorf("snapshot: publish %s: %w", dir, err)
	}
	if err := vfs.SyncDir(fs, parent); err != nil {
		return nil, fmt.Errorf("snapshot: sync parent dir %s: %w", parent, err)
	}
	success = true
	return &LogicalSnapshotExportResult{Manifest: *manifest}, nil
}

// ImportLogicalSnapshot replays one logical region snapshot artifact into dst using the
// engine's regular internal-entry apply path.
func ImportLogicalSnapshot(dst Sink, dir string, fs vfs.FS) (*LogicalSnapshotImportResult, error) {
	if dst == nil {
		return nil, fmt.Errorf("snapshot: import requires sink")
	}
	fs = vfs.Ensure(fs)
	manifest, err := ReadLogicalSnapshotManifest(dir, fs)
	if err != nil {
		return nil, err
	}
	entriesPath := filepath.Join(dir, entriesName)
	f, err := fs.OpenHandle(entriesPath)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open entries %s: %w", entriesPath, err)
	}
	defer func() { _ = f.Close() }()
	return importEntries(dst, manifest, f)
}

// ReadLogicalSnapshotManifest loads one logical snapshot manifest from dir.
func ReadLogicalSnapshotManifest(dir string, fs vfs.FS) (LogicalSnapshotManifest, error) {
	fs = vfs.Ensure(fs)
	data, err := fs.ReadFile(filepath.Join(dir, logicalSnapshotName))
	if err != nil {
		return LogicalSnapshotManifest{}, fmt.Errorf("snapshot: read manifest %s: %w", filepath.Join(dir, logicalSnapshotName), err)
	}
	var manifest LogicalSnapshotManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return LogicalSnapshotManifest{}, fmt.Errorf("snapshot: decode manifest %s: %w", filepath.Join(dir, logicalSnapshotName), err)
	}
	if manifest.FormatVersion != FormatVersion {
		return LogicalSnapshotManifest{}, fmt.Errorf("snapshot: unsupported format version %d", manifest.FormatVersion)
	}
	return manifest, nil
}

func exportEntries(src Source, path string, region raftmeta.RegionMeta, fs vfs.FS) (*LogicalSnapshotManifest, error) {
	f, err := fs.OpenFileHandle(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("snapshot: create entries %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	manifest, err := exportEntriesToWriter(src, f, region)
	if err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("snapshot: sync entries %s: %w", path, err)
	}
	return manifest, nil
}

func exportEntriesToWriter(src Source, writer io.Writer, region raftmeta.RegionMeta) (*LogicalSnapshotManifest, error) {
	hasher := crc32.New(kv.CastagnoliCrcTable)
	multiWriter := io.MultiWriter(writer, hasher)

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

	var entryCount uint64
	var payloadBytes uint64
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
		recordLen, err := kv.EncodeEntryTo(multiWriter, materialized)
		if err != nil {
			return nil, fmt.Errorf("snapshot: encode entry: %w", err)
		}
		entryCount++
		payloadBytes += uint64(recordLen)
	}
	manifest := &LogicalSnapshotManifest{
		FormatVersion: FormatVersion,
		Region:        raftmeta.CloneRegionMeta(region),
		EntryCount:    entryCount,
		PayloadBytes:  payloadBytes,
		PayloadCRC32:  hasher.Sum32(),
		CreatedAt:     time.Now().UTC(),
	}
	return manifest, nil
}

func importEntries(dst Sink, manifest LogicalSnapshotManifest, r io.Reader) (*LogicalSnapshotImportResult, error) {
	iter := kv.NewEntryIterator(r)
	defer func() { _ = iter.Close() }()

	hasher := crc32.New(kv.CastagnoliCrcTable)
	var batch []*kv.Entry
	var imported uint64
	var payloadBytes uint64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := dst.ApplyInternalEntries(batch); err != nil {
			for _, entry := range batch {
				if entry != nil {
					entry.DecrRef()
				}
			}
			return err
		}
		for _, entry := range batch {
			if entry != nil {
				entry.DecrRef()
			}
		}
		batch = batch[:0]
		return nil
	}

	for iter.Next() {
		entry := iter.Entry()
		if entry == nil {
			continue
		}
		entry.IncrRef()
		batch = append(batch, entry)
		imported++
		payloadBytes += uint64(iter.RecordLen())
		record, err := kv.EncodeEntry(nil, entry)
		if err != nil {
			for _, held := range batch {
				if held != nil {
					held.DecrRef()
				}
			}
			return nil, fmt.Errorf("snapshot: re-encode entry: %w", err)
		}
		if _, err := hasher.Write(record); err != nil {
			for _, held := range batch {
				if held != nil {
					held.DecrRef()
				}
			}
			return nil, fmt.Errorf("snapshot: hash payload: %w", err)
		}
		if len(batch) >= 256 {
			if err := flush(); err != nil {
				return nil, fmt.Errorf("snapshot: import apply batch: %w", err)
			}
		}
	}
	if err := iter.Err(); err != nil && err != io.EOF {
		for _, held := range batch {
			if held != nil {
				held.DecrRef()
			}
		}
		return nil, fmt.Errorf("snapshot: iterate entries: %w", err)
	}
	if err := flush(); err != nil {
		return nil, fmt.Errorf("snapshot: import apply batch: %w", err)
	}
	if imported != manifest.EntryCount {
		return nil, fmt.Errorf("snapshot: entry count mismatch manifest=%d imported=%d", manifest.EntryCount, imported)
	}
	if payloadBytes != manifest.PayloadBytes {
		return nil, fmt.Errorf("snapshot: payload size mismatch manifest=%d imported=%d", manifest.PayloadBytes, payloadBytes)
	}
	if sum := hasher.Sum32(); sum != manifest.PayloadCRC32 {
		return nil, fmt.Errorf("snapshot: payload checksum mismatch manifest=%08x imported=%08x", manifest.PayloadCRC32, sum)
	}
	return &LogicalSnapshotImportResult{
		Manifest:     manifest,
		Imported:     imported,
		PayloadBytes: payloadBytes,
	}, nil
}

func encodePayload(manifest LogicalSnapshotManifest, entries []byte) ([]byte, error) {
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("snapshot: encode payload manifest: %w", err)
	}
	if len(manifestData) > int(^uint32(0)) {
		return nil, fmt.Errorf("snapshot: payload manifest too large")
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(payloadMagic)+4+len(manifestData)+len(entries)))
	buf.WriteString(payloadMagic)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(manifestData)))
	buf.Write(lenBuf)
	buf.Write(manifestData)
	buf.Write(entries)
	return buf.Bytes(), nil
}

func decodePayload(payload []byte) (LogicalSnapshotManifest, []byte, error) {
	if len(payload) < len(payloadMagic)+4 {
		return LogicalSnapshotManifest{}, nil, fmt.Errorf("snapshot: payload too short")
	}
	if string(payload[:len(payloadMagic)]) != payloadMagic {
		return LogicalSnapshotManifest{}, nil, fmt.Errorf("snapshot: invalid payload magic")
	}
	payload = payload[len(payloadMagic):]
	manifestLen := binary.BigEndian.Uint32(payload[:4])
	payload = payload[4:]
	if uint32(len(payload)) < manifestLen {
		return LogicalSnapshotManifest{}, nil, fmt.Errorf("snapshot: truncated payload manifest")
	}
	var manifest LogicalSnapshotManifest
	if err := json.Unmarshal(payload[:manifestLen], &manifest); err != nil {
		return LogicalSnapshotManifest{}, nil, fmt.Errorf("snapshot: decode payload manifest: %w", err)
	}
	if manifest.FormatVersion != FormatVersion {
		return LogicalSnapshotManifest{}, nil, fmt.Errorf("snapshot: unsupported payload format version %d", manifest.FormatVersion)
	}
	return manifest, payload[manifestLen:], nil
}

func keyInRegion(region raftmeta.RegionMeta, key []byte) bool {
	if len(region.StartKey) > 0 && bytes.Compare(key, region.StartKey) < 0 {
		return false
	}
	if len(region.EndKey) > 0 && bytes.Compare(key, region.EndKey) >= 0 {
		return false
	}
	return true
}

func writeManifest(path string, manifest *LogicalSnapshotManifest, fs vfs.FS) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot: encode manifest %s: %w", path, err)
	}
	f, err := fs.OpenFileHandle(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("snapshot: create manifest %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: write manifest %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: sync manifest %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("snapshot: close manifest %s: %w", path, err)
	}
	return nil
}
