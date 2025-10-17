package manifest_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/manifest"
)

func TestManagerCreateAndRecover(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mgr.Close()

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
	if err := mgr.LogEdit(edit); err != nil {
		t.Fatalf("log edit: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	version := mgr.Current()
	files := version.Levels[0]
	if len(files) != 1 || files[0].FileID != 1 {
		t.Fatalf("unexpected version: %+v", version)
	}
}

func TestManagerLogPointer(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mgr.Close()

	edit := manifest.Edit{
		Type:      manifest.EditLogPointer,
		LogSeg:    5,
		LogOffset: 1024,
	}
	if err := mgr.LogEdit(edit); err != nil {
		t.Fatalf("log pointer: %v", err)
	}
	version := mgr.Current()
	if version.LogSegment != 5 || version.LogOffset != 1024 {
		t.Fatalf("log pointer mismatch: %+v", version)
	}
}

func TestManagerRaftPointer(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ptr := manifest.RaftLogPointer{
		GroupID:      7,
		Segment:      3,
		Offset:       2048,
		AppliedIndex: 42,
		AppliedTerm:  5,
		Committed:    41,
	}
	if err := mgr.LogRaftPointer(ptr); err != nil {
		t.Fatalf("log raft pointer: %v", err)
	}
	version := mgr.Current()
	stored, ok := version.RaftPointers[ptr.GroupID]
	if !ok {
		t.Fatalf("expected raft pointer stored in current version")
	}
	if stored.Segment != ptr.Segment || stored.Offset != ptr.Offset {
		t.Fatalf("unexpected raft pointer %+v", stored)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	recovered, ok := mgr.RaftPointer(ptr.GroupID)
	if !ok {
		t.Fatalf("expected raft pointer after reopen")
	}
	if recovered.Segment != ptr.Segment || recovered.Offset != ptr.Offset || recovered.AppliedIndex != ptr.AppliedIndex {
		t.Fatalf("unexpected pointer after reopen: %+v", recovered)
	}
}

func TestManagerValueLog(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mgr.Close()

	if err := mgr.LogValueLogHead(2, 4096); err != nil {
		t.Fatalf("log value log head: %v", err)
	}
	version := mgr.Current()
	meta := mgr.ValueLogHead()
	if !meta.Valid || meta.FileID != 2 || meta.Offset != 4096 {
		t.Fatalf("value log head mismatch: %+v", meta)
	}
	if err := mgr.LogValueLogDelete(2); err != nil {
		t.Fatalf("log value log delete: %v", err)
	}
	version = mgr.Current()
	if meta, ok := version.ValueLogs[2]; !ok {
		t.Fatalf("expected value log entry tracked after deletion")
	} else if meta.Valid {
		t.Fatalf("expected value log entry marked invalid")
	}
	meta = mgr.ValueLogHead()
	if meta.Valid {
		t.Fatalf("expected head cleared after deletion: %+v", meta)
	}
}

func TestManagerValueLogUpdate(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := mgr.LogValueLogHead(3, 2048); err != nil {
		t.Fatalf("log head: %v", err)
	}
	if err := mgr.LogValueLogDelete(3); err != nil {
		t.Fatalf("delete: %v", err)
	}

	meta := manifest.ValueLogMeta{FileID: 3, Offset: 2048, Valid: true}
	if err := mgr.LogValueLogUpdate(meta); err != nil {
		t.Fatalf("update: %v", err)
	}

	current := mgr.Current()
	restored, ok := current.ValueLogs[3]
	if !ok {
		t.Fatalf("expected fid 3 metadata after update")
	}
	if !restored.Valid || restored.Offset != 2048 {
		t.Fatalf("unexpected restored meta: %+v", restored)
	}
	head := mgr.ValueLogHead()
	if head.Valid {
		t.Fatalf("expected head to remain cleared after update: %+v", head)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	current = mgr.Current()
	restored, ok = current.ValueLogs[3]
	if !ok {
		t.Fatalf("expected fid 3 metadata after reopen")
	}
	if !restored.Valid || restored.Offset != 2048 {
		t.Fatalf("unexpected metadata after reopen: %+v", restored)
	}
}

func TestManagerCorruptManifest(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	name, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	path := filepath.Join(dir, string(name))
	mgr.Close()

	if err := os.WriteFile(path, []byte("corrupt"), 0o666); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}
	if _, err := manifest.Open(dir); err == nil {
		t.Fatalf("expected error for corrupt manifest")
	}
}

func TestManagerValueLogReplaySequence(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := mgr.LogValueLogHead(1, 128); err != nil {
		t.Fatalf("log head 1: %v", err)
	}
	if err := mgr.LogValueLogDelete(1); err != nil {
		t.Fatalf("delete head 1: %v", err)
	}
	if err := mgr.LogValueLogHead(2, 4096); err != nil {
		t.Fatalf("log head 2: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	version := mgr.Current()
	if meta1, ok := version.ValueLogs[1]; !ok {
		t.Fatalf("expected fid 1 metadata after replay")
	} else if meta1.Valid {
		t.Fatalf("expected fid 1 to remain invalid after deletion: %+v", meta1)
	}
	if meta2, ok := version.ValueLogs[2]; !ok {
		t.Fatalf("expected fid 2 metadata after replay")
	} else {
		if !meta2.Valid {
			t.Fatalf("expected fid 2 to be valid: %+v", meta2)
		}
		if meta2.Offset != 4096 {
			t.Fatalf("unexpected fid 2 offset: %d", meta2.Offset)
		}
	}
	head := mgr.ValueLogHead()
	if !head.Valid || head.FileID != 2 || head.Offset != 4096 {
		t.Fatalf("unexpected replay head: %+v", head)
	}
}

func TestManifestVerifyTruncatesPartialEdit(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := mgr.LogEdit(manifest.Edit{Type: manifest.EditAddFile, File: &manifest.FileMeta{FileID: 11}}); err != nil {
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
	f.Close()

	if err := manifest.Verify(dir); err != nil {
		t.Fatalf("verify: %v", err)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat after verify: %v", err)
	}
	if info.Size() != before {
		t.Fatalf("expected manifest truncated to %d, got %d", before, info.Size())
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen after verify: %v", err)
	}
	defer mgr.Close()
}
