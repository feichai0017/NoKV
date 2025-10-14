package manifest_test

import (
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
