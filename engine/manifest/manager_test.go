package manifest_test

import (
	"encoding/binary"
	"errors"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/stretchr/testify/require"
)

func TestManagerCreateAndRecover(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	edit := manifest.Edit{
		Type: manifest.EditAddFile,
		File: &manifest.FileMeta{
			Level:     0,
			FileID:    1,
			Size:      123,
			Smallest:  []byte("a"),
			Largest:   []byte("z"),
			CreatedAt: 1,
		},
	}
	if err := mgr.LogEdits(edit); err != nil {
		t.Fatalf("log edit: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	version := mgr.Current()
	files := version.Levels[0]
	if len(files) != 1 || files[0].FileID != 1 {
		t.Fatalf("unexpected version: %+v", version)
	}
}

func TestManagerValueLog(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	if err := mgr.LogValueLogHead(0, 2, 4096); err != nil {
		t.Fatalf("log value log head: %v", err)
	}
	version := mgr.Current()
	meta := mgr.ValueLogHead()[0]
	if !meta.Valid || meta.FileID != 2 || meta.Offset != 4096 {
		t.Fatalf("value log head mismatch: %+v", meta)
	}
	if err := mgr.LogValueLogDelete(0, 2); err != nil {
		t.Fatalf("log value log delete: %v", err)
	}
	version = mgr.Current()
	if meta, ok := version.ValueLogs[manifest.ValueLogID{Bucket: 0, FileID: 2}]; !ok {
		t.Fatalf("expected value log entry tracked after deletion")
	} else if meta.Valid {
		t.Fatalf("expected value log entry marked invalid")
	}
	meta = mgr.ValueLogHead()[0]
	if meta.Valid {
		t.Fatalf("expected head cleared after deletion: %+v", meta)
	}
}

func TestManagerValueLogUpdate(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := mgr.LogValueLogHead(0, 3, 2048); err != nil {
		t.Fatalf("log head: %v", err)
	}
	if err := mgr.LogValueLogDelete(0, 3); err != nil {
		t.Fatalf("delete: %v", err)
	}

	meta := manifest.ValueLogMeta{Bucket: 0, FileID: 3, Offset: 2048, Valid: true}
	if err := mgr.LogValueLogUpdate(meta); err != nil {
		t.Fatalf("update: %v", err)
	}

	current := mgr.Current()
	restored, ok := current.ValueLogs[manifest.ValueLogID{Bucket: 0, FileID: 3}]
	if !ok {
		t.Fatalf("expected fid 3 metadata after update")
	}
	if !restored.Valid || restored.Offset != 2048 {
		t.Fatalf("unexpected restored meta: %+v", restored)
	}
	head := mgr.ValueLogHead()[0]
	if head.Valid {
		t.Fatalf("expected head to remain cleared after update: %+v", head)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	current = mgr.Current()
	restored, ok = current.ValueLogs[manifest.ValueLogID{Bucket: 0, FileID: 3}]
	if !ok {
		t.Fatalf("expected fid 3 metadata after reopen")
	}
	if !restored.Valid || restored.Offset != 2048 {
		t.Fatalf("unexpected metadata after reopen: %+v", restored)
	}
}

func TestManagerCorruptManifest(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	name, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	path := filepath.Join(dir, string(name))
	require.NoError(t, mgr.Close())

	if err := os.WriteFile(path, []byte("corrupt"), 0o666); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}
	if _, err := manifest.Open(dir, nil); err == nil {
		t.Fatalf("expected error for corrupt manifest")
	}
}

func TestManagerValueLogReplaySequence(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := mgr.LogValueLogHead(0, 1, 128); err != nil {
		t.Fatalf("log head 1: %v", err)
	}
	if err := mgr.LogValueLogDelete(0, 1); err != nil {
		t.Fatalf("delete head 1: %v", err)
	}
	if err := mgr.LogValueLogHead(0, 2, 4096); err != nil {
		t.Fatalf("log head 2: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	version := mgr.Current()
	if meta1, ok := version.ValueLogs[manifest.ValueLogID{Bucket: 0, FileID: 1}]; !ok {
		t.Fatalf("expected fid 1 metadata after replay")
	} else if meta1.Valid {
		t.Fatalf("expected fid 1 to remain invalid after deletion: %+v", meta1)
	}
	if meta2, ok := version.ValueLogs[manifest.ValueLogID{Bucket: 0, FileID: 2}]; !ok {
		t.Fatalf("expected fid 2 metadata after replay")
	} else {
		if !meta2.Valid {
			t.Fatalf("expected fid 2 to be valid: %+v", meta2)
		}
		if meta2.Offset != 4096 {
			t.Fatalf("unexpected fid 2 offset: %d", meta2.Offset)
		}
	}
	head := mgr.ValueLogHead()[0]
	if !head.Valid || head.FileID != 2 || head.Offset != 4096 {
		t.Fatalf("unexpected replay head: %+v", head)
	}
}

func TestManifestVerifyTruncatesPartialEdit(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := mgr.LogEdits(manifest.Edit{Type: manifest.EditAddFile, File: &manifest.FileMeta{FileID: 11}}); err != nil {
		t.Fatalf("log edit: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	current, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	path := filepath.Join(dir, string(current))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	before := info.Size()

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(24)); err != nil {
		t.Fatalf("write length: %v", err)
	}
	if _, err := f.Write([]byte("NoK")); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	require.NoError(t, f.Close())

	if err := manifest.Verify(dir, nil); err != nil {
		t.Fatalf("verify: %v", err)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat after verify: %v", err)
	}
	if info.Size() != before {
		t.Fatalf("expected manifest truncated to %d, got %d", before, info.Size())
	}

	mgr, err = manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen after verify: %v", err)
	}
	defer func() { _ = mgr.Close() }()
}

func TestManagerRewrite(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	mgr.SetRewriteThreshold(1)

	if err := mgr.LogEdits(manifest.Edit{
		Type: manifest.EditAddFile,
		File: &manifest.FileMeta{Level: 0, FileID: 10, Size: 1},
	}); err != nil {
		t.Fatalf("log edit: %v", err)
	}
	// Add a second EditAddFile so the rewrite threshold is breached
	// without requiring the now-removed EditLogPointer edit type.
	if err := mgr.LogEdits(manifest.Edit{
		Type: manifest.EditAddFile,
		File: &manifest.FileMeta{Level: 0, FileID: 11, Size: 1},
	}); err != nil {
		t.Fatalf("log edit 2: %v", err)
	}

	current, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	currentName := strings.TrimSpace(string(current))
	if currentName == "MANIFEST-000001" || currentName == "" {
		t.Fatalf("expected rewritten manifest, got %q", currentName)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	version := mgr.Current()
	if len(version.Levels[0]) != 2 {
		t.Fatalf("expected 2 files after rewrite, got: %+v", version.Levels[0])
	}
}

func TestManagerSnapshotsAndCloneHelpers(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	mgr.SetSync(true)
	if got := mgr.Dir(); got != dir {
		t.Fatalf("expected dir %s, got %s", dir, got)
	}

	if err := mgr.LogValueLogUpdate(manifest.ValueLogMeta{Bucket: 0, FileID: 7, Offset: 9, Valid: true}); err != nil {
		t.Fatalf("log value log update: %v", err)
	}
	status := mgr.ValueLogStatus()
	if len(status) != 1 || status[manifest.ValueLogID{Bucket: 0, FileID: 7}].Offset != 9 {
		t.Fatalf("unexpected value log status: %+v", status)
	}

	if err := mgr.Rewrite(); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	meta := localmeta.RegionMeta{
		ID:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 10},
		},
	}
	cp := localmeta.CloneRegionMeta(meta)
	meta.StartKey[0] = 'z'
	if string(cp.StartKey) != "a" {
		t.Fatalf("expected clone to preserve start key")
	}

	ptrMeta := localmeta.CloneRegionMetaPtr(&meta)
	meta.EndKey[0] = 'y'
	if string(ptrMeta.EndKey) != "b" {
		t.Fatalf("expected cloned ptr meta to preserve end key")
	}

	metaMap := map[uint64]localmeta.RegionMeta{1: meta}
	cloned := localmeta.CloneRegionMetas(metaMap)
	meta.Peers[0].PeerID = 99
	if cloned[1].Peers[0].PeerID != 10 {
		t.Fatalf("expected cloned map to preserve peers")
	}
}

func TestManagerLogEditSyncFailureDoesNotApplyVersion(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST-000001")
	injected := errors.New("sync fail")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpFileSync, manifestPath, injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	mgr, err := manifest.Open(dir, fs)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	edit := manifest.Edit{
		Type: manifest.EditAddFile,
		File: &manifest.FileMeta{Level: 0, FileID: 99, Size: 1},
	}
	err = mgr.LogEdits(edit)
	if !errors.Is(err, injected) {
		t.Fatalf("expected sync failure, got %v", err)
	}

	files := mgr.Current().Levels[0]
	if len(files) != 0 {
		t.Fatalf("expected in-memory version rollback on sync failure, got %+v", files)
	}
}

func TestManagerCloseRetriesAfterInjectedFailure(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST-000001")
	injected := errors.New("close fail")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpFileClose, manifestPath, injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	mgr, err := manifest.Open(dir, fs)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	err = mgr.Close()
	if !errors.Is(err, injected) {
		t.Fatalf("expected close failure, got %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("retry close: %v", err)
	}
}

func TestOpenInjectedFailure(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("manifest mkdir injected")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpMkdirAll, "", injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	_, err := manifest.Open(dir, fs)
	require.ErrorIs(t, err, injected)
}

func TestVerifyInjectedFailure(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("manifest read current injected")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpReadFile, "", injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	err := manifest.Verify(dir, fs)
	require.ErrorIs(t, err, injected)
}

func TestVerifyPropagatesTruncateFailure(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := mgr.LogEdits(manifest.Edit{Type: manifest.EditAddFile, File: &manifest.FileMeta{FileID: 7}}); err != nil {
		t.Fatalf("log edit: %v", err)
	}
	require.NoError(t, mgr.Close())

	current, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	path := filepath.Join(dir, strings.TrimSpace(string(current)))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(24)); err != nil {
		t.Fatalf("write length: %v", err)
	}
	if _, err := f.Write([]byte("NoK")); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	require.NoError(t, f.Close())

	injected := errors.New("truncate fail")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpFileTrunc, path, injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)
	err = manifest.Verify(dir, fs)
	require.ErrorIs(t, err, injected)
}

func TestManagerOpenFailsWhenCurrentTempSyncFails(t *testing.T) {
	dir := t.TempDir()
	tmpPath := filepath.Join(dir, "CURRENT.tmp")
	injected := errors.New("current temp sync failed")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpFileSync, tmpPath, injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	_, err := manifest.Open(dir, fs)
	require.ErrorIs(t, err, injected)
	_, statErr := os.Stat(filepath.Join(dir, "CURRENT"))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestManagerOpenFailsWhenCurrentDirSyncFails(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("current dir sync failed")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpFileSync, dir, injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	_, err := manifest.Open(dir, fs)
	require.ErrorIs(t, err, injected)
	_, statErr := os.Stat(filepath.Join(dir, "CURRENT"))
	require.NoError(t, statErr)
}
