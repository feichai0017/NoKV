package vlog

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/wal"
)

func TestManagerAppendRead(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer mgr.Close()

	payload := []byte("record-data")
	vp, err := mgr.Append(payload)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	got, release, err := mgr.Read(vp)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	defer release()
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %q want %q", got, payload)
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

	vp, err := mgr.Append([]byte("abc"))
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

func TestEncodeDecodeHead(t *testing.T) {
	fid := uint32(10)
	off := uint32(1234)
	buf := EncodeHead(fid, off)
	rf, ro := DecodeHead(buf)
	if rf != fid || ro != off {
		t.Fatalf("decode mismatch fid=%d off=%d", rf, ro)
	}
	rf, ro = DecodeHead(nil)
	if rf != 0 || ro != 0 {
		t.Fatalf("decode nil mismatch")
	}
}

func TestManagerPopulateExisting(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	vp, err := mgr.Append([]byte("hello"))
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
	data, release, err := mgr.Read(vp)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	defer release()
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

	if _, err := mgr.Append([]byte("alpha")); err != nil {
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

	vp, err := mgr.Append([]byte("beta"))
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
	var buf bytes.Buffer
	entry1 := kv.NewEntry([]byte("k1"), []byte("value-data"))
	firstEncoded := wal.EncodeEntry(&buf, entry1)
	ptr1, err := mgr.Append(firstEncoded)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	buf.Reset()
	entry2 := kv.NewEntry([]byte("k2"), []byte("partial"))
	secondEncoded := wal.EncodeEntry(&buf, entry2)
	secondLen := len(secondEncoded)
	if _, err := mgr.Append(secondEncoded); err != nil {
		t.Fatalf("append second: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.vlog"))
	if err != nil || len(files) == 0 {
		t.Fatalf("list files err=%v files=%v", err, files)
	}
	partialSize := int64(ptr1.Offset) + int64(ptr1.Len) + int64(secondLen) - 5
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
