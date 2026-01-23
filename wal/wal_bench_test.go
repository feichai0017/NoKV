package wal

import (
	"testing"
)

func newBenchManager(b *testing.B) *Manager {
	b.Helper()
	mgr, err := Open(Config{Dir: b.TempDir(), SyncOnWrite: false, SegmentSize: 256 << 20})
	if err != nil {
		b.Fatalf("open wal: %v", err)
	}
	b.Cleanup(func() {
		_ = mgr.Close()
	})
	return mgr
}

func BenchmarkWALAppend(b *testing.B) {
	mgr := newBenchManager(b)
	payload := make([]byte, 256)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for b.Loop() {
		if _, err := mgr.Append(payload); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

func BenchmarkWALReplay(b *testing.B) {
	mgr := newBenchManager(b)
	payload := make([]byte, 128)
	for range 10_000 {
		if _, err := mgr.Append(payload); err != nil {
			b.Fatalf("append preload: %v", err)
		}
	}
	if err := mgr.Sync(); err != nil {
		b.Fatalf("sync: %v", err)
	}
	seg := mgr.ActiveSegment()
	b.ReportAllocs()

	for b.Loop() {
		if err := mgr.ReplaySegment(seg, func(EntryInfo, []byte) error { return nil }); err != nil {
			b.Fatalf("replay: %v", err)
		}
	}
}
