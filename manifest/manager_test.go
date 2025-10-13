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

	edit := manifest.Edit{
		Type: manifest.EditValueLog,
		ValueLog: &manifest.ValueLogMeta{
			FileID: 2,
			Offset: 4096,
		},
	}
	if err := mgr.LogEdit(edit); err != nil {
		t.Fatalf("log value log: %v", err)
	}
	version := mgr.Current()
	meta, ok := version.ValueLogs[2]
	if !ok || meta.Offset != 4096 {
		t.Fatalf("value log mismatch: %+v", version.ValueLogs)
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
