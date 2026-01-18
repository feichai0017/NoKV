package vlog

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/feichai0017/NoKV/kv"
)

func TestManagerAppendRead(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer mgr.Close()

	payload := []byte("record-data")
	entry := kv.NewEntry([]byte("key"), payload)
	vp, err := mgr.AppendEntry(entry)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	raw, release, err := mgr.Read(vp)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	defer release()

	got, _, err := kv.DecodeValueSlice(raw)
	if err != nil {
		t.Fatalf("decode value slice: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %q want %q", got, payload)
	}
}

func TestManagerReadValueAutoCopiesSmall(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer mgr.Close()

	entry := kv.NewEntry([]byte("key"), []byte("v"))
	vp, err := mgr.AppendEntry(entry)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	val, release, err := mgr.ReadValue(vp, ReadOptions{
		Mode:                ReadModeAuto,
		SmallValueThreshold: 16,
	})
	if err != nil {
		t.Fatalf("read value: %v", err)
	}
	if release != nil {
		release()
		t.Fatalf("expected copied value to return nil release")
	}
	if string(val) != "v" {
		t.Fatalf("value mismatch: got %q", val)
	}
}

func TestManagerRotate(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer mgr.Close()

	first := mgr.ActiveFID()
	if err := mgr.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	second := mgr.ActiveFID()
	if second == first {
		t.Fatalf("expected new active fid, got %d", second)
	}
}

func TestManagerRemove(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer mgr.Close()

	entry := kv.NewEntry([]byte("key"), []byte("abc"))
	vp, err := mgr.AppendEntry(entry)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := mgr.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if err := mgr.Remove(vp.Fid); err != nil {
		t.Fatalf("remove: %v", err)
	}
	_, _, err = mgr.Read(&kv.ValuePtr{Fid: vp.Fid, Offset: vp.Offset, Len: vp.Len})
	if err == nil {
		t.Fatalf("expected error reading removed fid")
	}
}

func TestManagerPopulateExisting(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	payload := []byte("hello")
	entry := kv.NewEntry([]byte("key"), payload)
	vp, err := mgr.AppendEntry(entry)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	mgr.Close()

	mgr, err = Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()
	if mgr.ActiveFID() != mgr.MaxFID() {
		t.Fatalf("active fid not equal max fid")
	}
	raw, release, err := mgr.Read(vp)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	defer release()

	data, _, err := kv.DecodeValueSlice(raw)
	if err != nil {
		t.Fatalf("decode value slice: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("data mismatch after reopen")
	}
}

func TestManagerRewind(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer mgr.Close()

	if _, err := mgr.AppendEntry(kv.NewEntry([]byte("key"), []byte("alpha"))); err != nil {
		t.Fatalf("append: %v", err)
	}
	head := mgr.Head()

	if err := mgr.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if mgr.ActiveFID() == head.Fid {
		t.Fatalf("expected active fid to change after rotate")
	}

	if err := mgr.Rewind(head); err != nil {
		t.Fatalf("rewind: %v", err)
	}

	if got := mgr.Head(); got != head {
		t.Fatalf("head mismatch after rewind: got %+v want %+v", got, head)
	}
	if mgr.ActiveFID() != head.Fid {
		t.Fatalf("active fid mismatch after rewind: got %d want %d", mgr.ActiveFID(), head.Fid)
	}
	if mgr.MaxFID() != head.Fid {
		t.Fatalf("max fid mismatch after rewind: got %d want %d", mgr.MaxFID(), head.Fid)
	}
	if fids := mgr.ListFIDs(); len(fids) != 1 || fids[0] != head.Fid {
		t.Fatalf("unexpected fid list after rewind: %v", fids)
	}

	vp, err := mgr.AppendEntry(kv.NewEntry([]byte("key"), []byte("beta")))
	if err != nil {
		t.Fatalf("append after rewind: %v", err)
	}
	if vp.Fid != head.Fid || vp.Offset < head.Offset {
		t.Fatalf("append after rewind produced unexpected pointer: %+v head=%+v", vp, head)
	}
}

func TestVerifyDirTruncatesPartialRecord(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}

	entry1 := kv.NewEntry([]byte("k1"), []byte("value-data"))
	ptr1, err := mgr.AppendEntry(entry1)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entry2 := kv.NewEntry([]byte("k2"), []byte("partial"))
	ptr2, err := mgr.AppendEntry(entry2)
	if err != nil {
		t.Fatalf("append second: %v", err)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.vlog"))
	if err != nil || len(files) == 0 {
		t.Fatalf("list files err=%v files=%v", err, files)
	}
	partialSize := int64(ptr1.Offset) + int64(ptr1.Len) + int64(ptr2.Len) - 5
	if err := os.Truncate(files[0], partialSize); err != nil {
		t.Fatalf("truncate vlog: %v", err)
	}

	if err := VerifyDir(Config{Dir: dir}); err != nil {
		t.Fatalf("verify dir: %v", err)
	}

	infoAfter, err := os.Stat(files[0])
	if err != nil {
		t.Fatalf("stat after verify: %v", err)
	}
	wantSize := int64(ptr1.Offset + ptr1.Len)
	if infoAfter.Size() != wantSize {
		t.Fatalf("expected truncated size %d, got %d", wantSize, infoAfter.Size())
	}
}

func TestManagerHooksAndSync(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer mgr.Close()

	mgr.SetTestingHooks(ManagerTestingHooks{
		BeforeAppend: func(_ *Manager, payload []byte) error {
			if len(payload) == 0 {
				return errors.New("empty payload")
			}
			return errors.New("append hook")
		},
	})
	if _, err := mgr.AppendEntry(kv.NewEntry([]byte("k"), []byte("v"))); err == nil {
		t.Fatalf("expected append hook error")
	}

	mgr.SetTestingHooks(ManagerTestingHooks{
		BeforeRotate: func(*Manager) error {
			return errors.New("rotate hook")
		},
	})
	if err := mgr.Rotate(); err == nil {
		t.Fatalf("expected rotate hook error")
	}

	mgr.SetTestingHooks(ManagerTestingHooks{
		BeforeSync: func(*Manager, uint32) error {
			return errors.New("sync hook")
		},
	})
	if err := mgr.SyncActive(); err == nil {
		t.Fatalf("expected sync hook error")
	}
}

func TestManagerSyncFIDsDedup(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer mgr.Close()

	vp, err := mgr.AppendEntry(kv.NewEntry([]byte("k"), []byte("v")))
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	var calls int32
	mgr.SetTestingHooks(ManagerTestingHooks{
		BeforeSync: func(_ *Manager, fid uint32) error {
			if fid != vp.Fid {
				return errors.New("unexpected fid")
			}
			atomic.AddInt32(&calls, 1)
			return nil
		},
	})

	if err := mgr.SyncFIDs([]uint32{vp.Fid, vp.Fid, vp.Fid + 10}); err != nil {
		t.Fatalf("sync fids: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected single sync call, got %d", got)
	}
}

func TestManagerSegmentOps(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer mgr.Close()

	vp, err := mgr.AppendEntry(kv.NewEntry([]byte("k"), []byte("v")))
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	size, err := mgr.SegmentSize(vp.Fid)
	if err != nil {
		t.Fatalf("segment size: %v", err)
	}
	if size <= 0 {
		t.Fatalf("expected size > 0, got %d", size)
	}
	if err := mgr.SegmentInit(vp.Fid); err != nil {
		t.Fatalf("segment init: %v", err)
	}
	if err := mgr.SegmentTruncate(vp.Fid, uint32(kv.ValueLogHeaderSize)); err != nil {
		t.Fatalf("segment truncate: %v", err)
	}
	size, err = mgr.SegmentSize(vp.Fid)
	if err != nil {
		t.Fatalf("segment size after truncate: %v", err)
	}
	if size != int64(kv.ValueLogHeaderSize) {
		t.Fatalf("expected size %d after truncate, got %d", kv.ValueLogHeaderSize, size)
	}
	if err := mgr.SegmentBootstrap(vp.Fid); err != nil {
		t.Fatalf("segment bootstrap: %v", err)
	}
	size, err = mgr.SegmentSize(vp.Fid)
	if err != nil {
		t.Fatalf("segment size after bootstrap: %v", err)
	}
	if size < int64(kv.ValueLogHeaderSize) {
		t.Fatalf("expected size >= %d after bootstrap, got %d", kv.ValueLogHeaderSize, size)
	}
	if _, err := mgr.SegmentSize(vp.Fid + 10); err == nil {
		t.Fatalf("expected error for missing fid")
	}
}
