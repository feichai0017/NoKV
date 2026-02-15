package vlog

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

func TestAppendEntriesMaskAndErrors(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	entries := []*kv.Entry{
		kv.NewEntry([]byte("k1"), []byte("v1")),
	}
	if _, err := mgr.AppendEntries(entries, []bool{true, false}); err == nil {
		t.Fatalf("expected write mask length error")
	}
	if _, err := mgr.AppendEntries([]*kv.Entry{nil}, nil); err == nil {
		t.Fatalf("expected nil entry error")
	}

	ptrs, err := mgr.AppendEntries([]*kv.Entry{
		kv.NewEntry([]byte("k2"), []byte("v2")),
		kv.NewEntry([]byte("k3"), []byte("v3")),
	}, []bool{false, false})
	if err != nil {
		t.Fatalf("append entries with mask: %v", err)
	}
	if ptrs[0].Len != 0 || ptrs[1].Len != 0 {
		t.Fatalf("expected masked entries to have zero pointers")
	}
}

func TestAppendEntriesBatchAndIterate(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	entries := []*kv.Entry{
		kv.NewEntry([]byte("k1"), []byte("v1")),
		kv.NewEntry([]byte("k2"), []byte("v2")),
		kv.NewEntry([]byte("k3"), []byte("v3")),
	}
	ptrs, err := mgr.AppendEntries(entries, []bool{true, false, true})
	if err != nil {
		t.Fatalf("append entries: %v", err)
	}
	if ptrs[1].Len != 0 {
		t.Fatalf("expected masked pointer to be empty")
	}

	if _, err := mgr.Iterate(mgr.ActiveFID(), 0, nil); err == nil {
		t.Fatalf("expected nil iterate callback error")
	}

	var keys []string
	end, err := mgr.Iterate(mgr.ActiveFID(), 0, func(e *kv.Entry, _ *kv.ValuePtr) error {
		keys = append(keys, string(e.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if end < uint32(kv.ValueLogHeaderSize) {
		t.Fatalf("unexpected end offset: %d", end)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(keys))
	}
}

func TestReadAfterRotateSealsSegment(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir, FileMode: utils.DefaultFileMode, MaxSize: 1 << 20})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	entry := kv.NewEntry([]byte("sealed-key"), []byte("sealed-val"))
	ptrs, err := mgr.AppendEntries([]*kv.Entry{entry}, nil)
	entry.DecrRef()
	if err != nil {
		t.Fatalf("append entries: %v", err)
	}
	if len(ptrs) != 1 {
		t.Fatalf("expected 1 pointer, got %d", len(ptrs))
	}
	oldFID := ptrs[0].Fid
	if err := mgr.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	seg := mgr.files[oldFID]
	if seg == nil || !seg.isSealed() {
		t.Fatalf("expected sealed segment for fid %d", oldFID)
	}

	data, unlock, err := mgr.Read(&ptrs[0])
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if unlock != nil {
		unlock()
	}
	decoded, err := kv.DecodeEntry(data)
	if err != nil {
		t.Fatalf("decode entry: %v", err)
	}
	if string(decoded.Key) != "sealed-key" {
		t.Fatalf("expected key sealed-key, got %q", string(decoded.Key))
	}
	if string(decoded.Value) != "sealed-val" {
		t.Fatalf("expected value sealed-val, got %q", string(decoded.Value))
	}
	decoded.DecrRef()
}

func TestIterateStopsEarly(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	if _, err := mgr.AppendEntries([]*kv.Entry{
		kv.NewEntry([]byte("k1"), []byte("v1")),
		kv.NewEntry([]byte("k2"), []byte("v2")),
	}, nil); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	var stop uint32
	end, err := mgr.Iterate(mgr.ActiveFID(), 0, func(_ *kv.Entry, vp *kv.ValuePtr) error {
		stop = vp.Offset + vp.Len
		return utils.ErrStop
	})
	if err != nil {
		t.Fatalf("iterate stop: %v", err)
	}
	if end != stop {
		t.Fatalf("expected end offset %d, got %d", stop, end)
	}
}

func TestAppendEntriesLargeBatchFallback(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{
		Dir:     dir,
		MaxSize: int64(kv.ValueLogHeaderSize) + 16,
	})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	payload := make([]byte, 64)
	ptrs, err := mgr.AppendEntries([]*kv.Entry{
		kv.NewEntry([]byte("k1"), payload),
		kv.NewEntry([]byte("k2"), payload),
	}, nil)
	if err != nil {
		t.Fatalf("append entries fallback: %v", err)
	}
	for i, ptr := range ptrs {
		if ptr.Len == 0 {
			t.Fatalf("expected pointer %d to be populated", i)
		}
	}
}

func TestSampleStats(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	entries := []*kv.Entry{
		kv.NewEntry([]byte("keep"), []byte("v1")),
		kv.NewEntry([]byte("drop"), []byte("v2")),
		kv.NewEntry([]byte("keep2"), []byte("v3")),
	}
	if _, err := mgr.AppendEntries(entries, nil); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	stats, err := mgr.Sample(mgr.ActiveFID(), SampleOptions{
		SizeRatio:     1,
		CountRatio:    1,
		FromBeginning: true,
		MaxEntries:    uint32(len(entries)),
	}, func(e *kv.Entry, _ *kv.ValuePtr) (bool, error) {
		return string(e.Key) == "drop", nil
	})
	if err != nil {
		t.Fatalf("sample: %v", err)
	}
	if stats.Count != len(entries) {
		t.Fatalf("expected %d samples, got %d", len(entries), stats.Count)
	}
	if stats.DiscardMiB > stats.TotalMiB {
		t.Fatalf("discarded size exceeds total")
	}
}

func TestSampleNilCallback(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	if _, err := mgr.Sample(mgr.ActiveFID(), SampleOptions{}, nil); err == nil {
		t.Fatalf("expected nil callback error")
	}
}
