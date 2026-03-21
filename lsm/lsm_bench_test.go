package lsm

import (
	"encoding/binary"
	"fmt"
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
	lsm, err := NewLSM(opt, wlog)
	if err != nil {
		b.Fatalf("new lsm: %v", err)
	}
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
		if err := lsm.Rotate(); err != nil {
			b.Fatalf("rotate: %v", err)
		}
		waitForFlush(b, lsm)
	}
}

func benchUserKey(i int) []byte {
	return []byte(fmt.Sprintf("k%08d", i))
}

func buildBenchLevelTables(b *testing.B, lsm *LSM, levelNum int, tableCount int) *levelHandler {
	b.Helper()
	lh := lsm.levels.levels[levelNum]
	for i := 0; i < tableCount; i++ {
		builderOpt := *lsm.option
		builderOpt.BlockSize = 4 << 10
		builderOpt.BloomFalsePositive = 0.0
		builder := newTableBuiler(&builderOpt)
		userKey := benchUserKey(i)
		builder.AddKey(kv.NewEntry(
			kv.InternalKey(kv.CFDefault, userKey, 1),
			[]byte("value"),
		))
		tableName := utils.FileNameSSTable(lsm.option.WorkDir, uint64(10000+i))
		tbl, err := openTable(lsm.levels, tableName, builder)
		if err != nil {
			b.Fatalf("open bench table: %v", err)
		}
		if tbl == nil {
			b.Fatalf("expected bench table")
		}
		lh.add(tbl)
	}
	lh.Sort()
	return lh
}

func BenchmarkLevelPointMissPruning(b *testing.B) {
	const tableCount = 2048
	for _, useGuide := range []bool{false, true} {
		name := "linear"
		if useGuide {
			name = "range_filter"
		}
		b.Run(name, func(b *testing.B) {
			lsm := newBenchLSM(b, 64<<20)
			lh := buildBenchLevelTables(b, lsm, 1, tableCount)
			if !useGuide {
				lh.Lock()
				lh.filter = rangeFilter{}
				lh.Unlock()
			}
			missKey := kv.InternalKey(kv.CFDefault, benchUserKey(tableCount+1024), kv.MaxVersion)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry, err := lh.Get(missKey)
				if err != utils.ErrKeyNotFound {
					b.Fatalf("expected miss, got entry=%v err=%v", entry, err)
				}
				if entry != nil {
					b.Fatalf("expected nil entry on miss")
				}
			}
		})
	}
}

func BenchmarkLevelIteratorBoundsPruning(b *testing.B) {
	const tableCount = 2048
	lower := benchUserKey(tableCount / 2)
	upper := benchUserKey(tableCount/2 + 1)
	for _, useGuide := range []bool{false, true} {
		name := "linear"
		if useGuide {
			name = "range_filter"
		}
		b.Run(name, func(b *testing.B) {
			lsm := newBenchLSM(b, 64<<20)
			lh := buildBenchLevelTables(b, lsm, 1, tableCount)
			if !useGuide {
				lh.Lock()
				lh.filter = rangeFilter{}
				lh.Unlock()
			}
			opt := &utils.Options{
				IsAsc:      true,
				LowerBound: lower,
				UpperBound: upper,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				iters := lh.iterators(opt)
				merge := NewMergeIterator(iters, false)
				merge.Rewind()
				count := 0
				for ; merge.Valid(); merge.Next() {
					count++
				}
				if count != 1 {
					b.Fatalf("expected exactly one item in bounded scan, got %d", count)
				}
				if err := merge.Close(); err != nil {
					b.Fatalf("close merge iterator: %v", err)
				}
			}
		})
	}
}
