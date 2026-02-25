package lsm

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
)

func newBenchLSM(b *testing.B, memTableSize int64) *LSM {
	b.Helper()
	dir := b.TempDir()
	wlog, err := wal.Open(wal.Config{Dir: dir, SyncOnWrite: false})
	if err != nil {
		b.Fatalf("open wal: %v", err)
	}
	opt := &Options{
		WorkDir:                       dir,
		MemTableSize:                  memTableSize,
		MemTableEngine:                "skiplist",
		SSTableMaxSz:                  256 << 20,
		BlockSize:                     8 * 1024,
		BloomFalsePositive:            0.01,
		BaseLevelSize:                 32 << 20,
		LevelSizeMultiplier:           8,
		BaseTableSize:                 8 << 20,
		TableSizeMultiplier:           2,
		NumLevelZeroTables:            8,
		MaxLevelNum:                   utils.MaxLevelNum,
		NumCompactors:                 1,
		IngestCompactBatchSize:        4,
		IngestBacklogMergeScore:       2.0,
		IngestShardParallelism:        1,
		CompactionValueWeight:         0.35,
		CompactionValueAlertThreshold: 0.6,
	}
	lsm := NewLSM(opt, wlog)
	b.Cleanup(func() {
		_ = lsm.Close()
		_ = wlog.Close()
	})
	return lsm
}

func makeLSMBatch(batchSize int, valueSize int) []*kv.Entry {
	entries := make([]*kv.Entry, batchSize)
	value := make([]byte, valueSize)
	for i := range batchSize {
		key := make([]byte, 16)
		copy(key, "benchkey")
		binary.LittleEndian.PutUint64(key[8:], uint64(i))
		internal := kv.InternalKey(kv.CFDefault, key, uint64(i+1))
		entries[i] = &kv.Entry{
			Key:     internal,
			Value:   value,
			CF:      kv.CFDefault,
			Version: uint64(i + 1),
		}
	}
	return entries
}

func waitForFlush(b *testing.B, lsm *LSM) {
	b.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if lsm.FlushPending() == 0 {
			lsm.lock.RLock()
			pending := len(lsm.immutables)
			lsm.lock.RUnlock()
			if pending == 0 {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.Fatalf("timeout waiting for flush (pending=%d)", lsm.FlushPending())
}

func BenchmarkLSMSetBatch(b *testing.B) {
	lsm := newBenchLSM(b, 64<<20)
	batchSize := 64
	valueSize := 128
	b.ReportAllocs()
	b.SetBytes(int64(batchSize * valueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries := makeLSMBatch(batchSize, valueSize)
		if err := lsm.SetBatch(entries); err != nil {
			b.Fatalf("set batch: %v", err)
		}
	}
}

func BenchmarkLSMRotateFlush(b *testing.B) {
	lsm := newBenchLSM(b, 1<<20)
	entries := makeLSMBatch(256, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := lsm.SetBatch(entries); err != nil {
			b.Fatalf("set batch: %v", err)
		}
		lsm.Rotate()
		waitForFlush(b, lsm)
	}
}
