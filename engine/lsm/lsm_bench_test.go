package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/utils"
)

const (
	benchMemTableEngineART      = "art"
	benchMemTableEngineSkiplist = "skiplist"
)

var benchMemTableEngines = []string{
	benchMemTableEngineART,
	benchMemTableEngineSkiplist,
}

func newBenchLSM(b *testing.B, memTableSize int64) *LSM {
	return newBenchLSMWithEngine(b, memTableSize, benchMemTableEngineART)
}

func newBenchLSMWithEngine(b *testing.B, memTableSize int64, memTableEngine string) *LSM {
	b.Helper()
	dir := b.TempDir()
	wlog, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		b.Fatalf("open wal: %v", err)
	}
	opt := &Options{
		WorkDir:                       dir,
		MemTableSize:                  memTableSize,
		MemTableEngine:                memTableEngine,
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
	lsm, err := NewLSM(opt, []*wal.Manager{wlog})
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
			pending := 0
			for _, s := range lsm.shards {
				s.lock.RLock()
				pending += len(s.immutables)
				s.lock.RUnlock()
			}
			if pending == 0 {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.Fatalf("timeout waiting for flush (pending=%d)", lsm.FlushPending())
}

func BenchmarkLSMSetBatch(b *testing.B) {
	batchSize := 64
	valueSize := 128
	for _, memTableEngine := range benchMemTableEngines {
		b.Run(memTableEngine, func(b *testing.B) {
			lsm := newBenchLSMWithEngine(b, 64<<20, memTableEngine)
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * valueSize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entries := makeLSMBatch(batchSize, valueSize)
				if err := lsm.SetBatch(entries); err != nil {
					b.Fatalf("set batch: %v", err)
				}
			}
		})
	}
}

func BenchmarkLSMRotateFlush(b *testing.B) {
	entries := makeLSMBatch(256, 256)
	for _, memTableEngine := range benchMemTableEngines {
		b.Run(memTableEngine, func(b *testing.B) {
			lsm := newBenchLSMWithEngine(b, 1<<20, memTableEngine)
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
		})
	}
}

func BenchmarkLSMGetMemtableHit(b *testing.B) {
	const (
		keySpace  = 4096
		valueSize = 128
	)
	for _, memTableEngine := range benchMemTableEngines {
		b.Run(memTableEngine, func(b *testing.B) {
			lsm := newBenchLSMWithEngine(b, 64<<20, memTableEngine)
			lookups := make([][]byte, keySpace)
			for i := range keySpace {
				userKey := make([]byte, 8)
				binary.LittleEndian.PutUint64(userKey, uint64(i))
				internal := kv.InternalKey(kv.CFDefault, userKey, uint64(i+1))
				lookups[i] = internal
				entry := &kv.Entry{
					Key:     internal,
					Value:   bytes.Repeat([]byte("v"), valueSize),
					CF:      kv.CFDefault,
					Version: uint64(i + 1),
				}
				if err := lsm.Set(entry); err != nil {
					b.Fatalf("seed memtable: %v", err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry, err := lsm.Get(lookups[i%len(lookups)])
				if err != nil {
					b.Fatalf("get: %v", err)
				}
				if entry == nil {
					b.Fatal("expected entry")
				}
				entry.DecrRef()
			}
		})
	}
}

func BenchmarkLSMMemtableIterSeek(b *testing.B) {
	const (
		keySpace  = 4096
		valueSize = 64
	)
	for _, memTableEngine := range benchMemTableEngines {
		b.Run(memTableEngine, func(b *testing.B) {
			lsm := newBenchLSMWithEngine(b, 64<<20, memTableEngine)
			seekKeys := make([][]byte, keySpace)
			for i := range keySpace {
				userKey := make([]byte, 8)
				binary.LittleEndian.PutUint64(userKey, uint64(i))
				internal := kv.InternalKey(kv.CFDefault, userKey, uint64(i+1))
				seekKeys[i] = internal
				entry := &kv.Entry{
					Key:     internal,
					Value:   bytes.Repeat([]byte("v"), valueSize),
					CF:      kv.CFDefault,
					Version: uint64(i + 1),
				}
				if err := lsm.Set(entry); err != nil {
					b.Fatalf("seed memtable: %v", err)
				}
			}
			it := lsm.shards[0].memTable.NewIterator(&index.Options{IsAsc: true})
			defer func() { _ = it.Close() }()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it.Seek(seekKeys[i%len(seekKeys)])
				if !it.Valid() {
					b.Fatal("seek missed key")
				}
			}
		})
	}
}

func benchUserKey(i int) []byte {
	return fmt.Appendf(nil, "k%08d", i)
}

func buildBenchLevelTables(b *testing.B, lsm *LSM, levelNum int, tableCount int) *levelHandler {
	return buildBenchLevelTablesAtOffset(b, lsm, levelNum, 0, tableCount)
}

func buildBenchLevelTablesAtOffset(b *testing.B, lsm *LSM, levelNum int, start int, tableCount int) *levelHandler {
	b.Helper()
	lh := lsm.levels.levels[levelNum]
	fidBase := uint64(levelNum*1_000_000 + start + 10_000)
	for i := range tableCount {
		builderOpt := *lsm.option
		builderOpt.BlockSize = 4 << 10
		builderOpt.BloomFalsePositive = 0.0
		builder := newTableBuiler(&builderOpt)
		userKey := benchUserKey(start + i)
		builder.AddKey(kv.NewEntry(
			kv.InternalKey(kv.CFDefault, userKey, 1),
			[]byte("value"),
		))
		tableName := vfs.FileNameSSTable(lsm.option.WorkDir, fidBase+uint64(i))
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

func buildBenchLevelTablesWithInRangeGap(b *testing.B, lsm *LSM, levelNum int, tableCount int) *levelHandler {
	return buildBenchLevelTablesWithInRangeGapAtOffset(b, lsm, levelNum, 0, tableCount)
}

func buildBenchLevelTablesWithInRangeGapAtOffset(b *testing.B, lsm *LSM, levelNum int, start int, tableCount int) *levelHandler {
	b.Helper()
	lh := lsm.levels.levels[levelNum]
	fidBase := uint64(levelNum*1_000_000 + start + 20_000)
	for i := range tableCount {
		builderOpt := *lsm.option
		builderOpt.BlockSize = 4 << 10
		builderOpt.BloomFalsePositive = 0.01
		builder := newTableBuiler(&builderOpt)
		left := benchUserKey(start + i*4)
		right := benchUserKey(start + i*4 + 2)
		builder.AddKey(kv.NewEntry(
			kv.InternalKey(kv.CFDefault, left, 1),
			[]byte("value-left"),
		))
		builder.AddKey(kv.NewEntry(
			kv.InternalKey(kv.CFDefault, right, 1),
			[]byte("value-right"),
		))
		tableName := vfs.FileNameSSTable(lsm.option.WorkDir, fidBase+uint64(i))
		tbl, err := openTable(lsm.levels, tableName, builder)
		if err != nil {
			b.Fatalf("open bench table with gap: %v", err)
		}
		if tbl == nil {
			b.Fatalf("expected bench table with gap")
		}
		lh.add(tbl)
	}
	lh.Sort()
	return lh
}

func buildBenchL0OverlapTables(b *testing.B, lsm *LSM, tableCount int) *levelHandler {
	b.Helper()
	lh := lsm.levels.levels[0]
	for i := range tableCount {
		builderOpt := *lsm.option
		builderOpt.BlockSize = 4 << 10
		builderOpt.BloomFalsePositive = 0.01
		builder := newTableBuiler(&builderOpt)
		left := benchUserKey(i * 4)
		right := benchUserKey(i*4 + 2048)
		builder.AddKey(kv.NewEntry(
			kv.InternalKey(kv.CFDefault, left, 1),
			[]byte("value-left"),
		))
		builder.AddKey(kv.NewEntry(
			kv.InternalKey(kv.CFDefault, right, 1),
			[]byte("value-right"),
		))
		tableName := vfs.FileNameSSTable(lsm.option.WorkDir, uint64(30000+i))
		tbl, err := openTable(lsm.levels, tableName, builder)
		if err != nil {
			b.Fatalf("open overlapping L0 table: %v", err)
		}
		if tbl == nil {
			b.Fatalf("expected overlapping L0 table")
		}
		lh.add(tbl)
	}
	lh.Sort()
	return lh
}

func disableBenchRangeFilter(levels ...*levelHandler) {
	for _, lh := range levels {
		if lh == nil {
			continue
		}
		lh.Lock()
		lh.filter = rangeFilter{}
		lh.Unlock()
	}
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

func BenchmarkLevelPointHitPruning(b *testing.B) {
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
			hitKey := kv.InternalKey(kv.CFDefault, benchUserKey(tableCount/2), kv.MaxVersion)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry, err := lh.Get(hitKey)
				if err != nil {
					b.Fatalf("expected hit, got err=%v", err)
				}
				if entry == nil {
					b.Fatalf("expected hit entry")
				}
				entry.DecrRef()
			}
		})
	}
}

func BenchmarkLevelPointInRangeMissPruning(b *testing.B) {
	const tableCount = 2048
	for _, useGuide := range []bool{false, true} {
		name := "linear"
		if useGuide {
			name = "range_filter"
		}
		b.Run(name, func(b *testing.B) {
			lsm := newBenchLSM(b, 64<<20)
			lh := buildBenchLevelTablesWithInRangeGap(b, lsm, 1, tableCount)
			if !useGuide {
				lh.Lock()
				lh.filter = rangeFilter{}
				lh.Unlock()
			}
			missKey := kv.InternalKey(kv.CFDefault, benchUserKey(tableCount*2+1), kv.MaxVersion)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry, err := lh.Get(missKey)
				if err != utils.ErrKeyNotFound {
					b.Fatalf("expected in-range miss, got entry=%v err=%v", entry, err)
				}
				if entry != nil {
					b.Fatalf("expected nil entry on in-range miss")
				}
			}
		})
	}
}

func BenchmarkLevelIteratorBoundsPruning(b *testing.B) {
	const tableCount = 2048
	start := tableCount / 2
	for _, width := range []int{1, 8, 64} {
		lower := benchUserKey(start)
		upper := benchUserKey(start + width)
		b.Run(fmt.Sprintf("width_%d", width), func(b *testing.B) {
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
					opt := &index.Options{
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
						if count != width {
							b.Fatalf("expected %d items in bounded scan, got %d", width, count)
						}
						if err := merge.Close(); err != nil {
							b.Fatalf("close merge iterator: %v", err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkTableIteratorBlockBounds(b *testing.B) {
	const totalKeys = 4096
	for _, width := range []int{8, 64, 256} {
		lower := fmt.Appendf(nil, "k%06d", totalKeys/2)
		upper := fmt.Appendf(nil, "k%06d", totalKeys/2+width)
		b.Run(fmt.Sprintf("width_%d", width), func(b *testing.B) {
			for _, bounded := range []bool{false, true} {
				name := "manual_seek_break"
				if bounded {
					name = "block_range"
				}
				b.Run(name, func(b *testing.B) {
					lsm := newBenchLSM(b, 64<<20)
					builderOpt := *lsm.option
					builderOpt.BlockSize = 128
					builderOpt.BloomFalsePositive = 0.0
					builder := newTableBuiler(&builderOpt)
					for i := range totalKeys {
						key := fmt.Appendf(nil, "k%06d", i)
						builder.AddKey(kv.NewEntry(
							kv.InternalKey(kv.CFDefault, key, 1),
							[]byte("value-with-more-data"),
						))
					}
					tableName := vfs.FileNameSSTable(lsm.option.WorkDir, uint64(90000+width))
					tbl, err := openTable(lsm.levels, tableName, builder)
					if err != nil {
						b.Fatalf("open bench multi-block table: %v", err)
					}
					if tbl == nil {
						b.Fatalf("expected bench multi-block table")
					}
					defer func() { _ = tbl.DecrRef() }()

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var it index.Iterator
						if bounded {
							it = tbl.NewIterator(&index.Options{
								IsAsc:      true,
								LowerBound: lower,
								UpperBound: upper,
							})
							it.Rewind()
						} else {
							it = tbl.NewIterator(&index.Options{IsAsc: true})
							it.Seek(kv.InternalKey(kv.CFDefault, lower, kv.MaxVersion))
						}

						count := 0
						for ; it.Valid(); it.Next() {
							entry := it.Item().Entry()
							_, userKey, _, ok := kv.SplitInternalKey(entry.Key)
							if !ok {
								b.Fatalf("expected internal key")
							}
							if bytes.Compare(userKey, upper) >= 0 {
								break
							}
							count++
						}
						if count != width {
							b.Fatalf("expected %d items in bounded table scan, got %d", width, count)
						}
						if err := it.Close(); err != nil {
							b.Fatalf("close table iterator: %v", err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkLSMMultiLevelPointPruning(b *testing.B) {
	const tableCount = 1024
	for _, query := range []struct {
		name string
		key  []byte
		hit  bool
	}{
		{
			name: "deep_hit",
			key:  kv.InternalKey(kv.CFDefault, benchUserKey(20000+tableCount/2), kv.MaxVersion),
			hit:  true,
		},
		{
			name: "miss",
			key:  kv.InternalKey(kv.CFDefault, benchUserKey(50000), kv.MaxVersion),
			hit:  false,
		},
	} {
		b.Run(query.name, func(b *testing.B) {
			for _, useFilter := range []bool{false, true} {
				name := "linear"
				if useFilter {
					name = "range_filter"
				}
				b.Run(name, func(b *testing.B) {
					lsm := newBenchLSM(b, 64<<20)
					l1 := buildBenchLevelTablesAtOffset(b, lsm, 1, 0, tableCount)
					l2 := buildBenchLevelTablesAtOffset(b, lsm, 2, 10000, tableCount)
					l3 := buildBenchLevelTablesAtOffset(b, lsm, 3, 20000, tableCount)
					if !useFilter {
						disableBenchRangeFilter(l1, l2, l3)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						entry, err := lsm.Get(query.key)
						if query.hit {
							if err != nil {
								b.Fatalf("expected hit, got err=%v", err)
							}
							if entry == nil {
								b.Fatalf("expected hit entry")
							}
							entry.DecrRef()
							continue
						}
						if err != utils.ErrKeyNotFound {
							b.Fatalf("expected miss, got entry=%v err=%v", entry, err)
						}
						if entry != nil {
							b.Fatalf("expected nil entry on miss")
						}
					}
				})
			}
		})
	}
}

func BenchmarkLevelL0OverlapFallback(b *testing.B) {
	const tableCount = 512
	for _, query := range []struct {
		name string
		key  []byte
		hit  bool
	}{
		{
			name: "hit",
			key:  kv.InternalKey(kv.CFDefault, benchUserKey((tableCount/2)*4), kv.MaxVersion),
			hit:  true,
		},
		{
			name: "in_range_miss",
			key:  kv.InternalKey(kv.CFDefault, benchUserKey(tableCount*2+1), kv.MaxVersion),
			hit:  false,
		},
	} {
		b.Run(query.name, func(b *testing.B) {
			for _, useFilter := range []bool{false, true} {
				name := "linear"
				if useFilter {
					name = "range_filter"
				}
				b.Run(name, func(b *testing.B) {
					lsm := newBenchLSM(b, 64<<20)
					l0 := buildBenchL0OverlapTables(b, lsm, tableCount)
					if !useFilter {
						disableBenchRangeFilter(l0)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						entry, err := l0.Get(query.key)
						if query.hit {
							if err != nil {
								b.Fatalf("expected hit, got err=%v", err)
							}
							if entry == nil {
								b.Fatalf("expected hit entry")
							}
							entry.DecrRef()
							continue
						}
						if err != utils.ErrKeyNotFound {
							b.Fatalf("expected miss, got entry=%v err=%v", entry, err)
						}
						if entry != nil {
							b.Fatalf("expected nil entry on miss")
						}
					}
				})
			}
		})
	}
}

func BenchmarkLSMMixedPointPruning(b *testing.B) {
	const tableCount = 1024
	for _, mix := range []struct {
		name string
		keys [][]byte
	}{
		{
			name: "hit_50_miss_50",
			keys: [][]byte{
				kv.InternalKey(kv.CFDefault, benchUserKey(tableCount/8), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(10000+tableCount/4), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(20000+tableCount/2), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(40000), kv.MaxVersion),
			},
		},
		{
			name: "hit_90_miss_10",
			keys: [][]byte{
				kv.InternalKey(kv.CFDefault, benchUserKey(tableCount/8), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(tableCount/4), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(10000+tableCount/3), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(10000+tableCount/2), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(20000+tableCount/4), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(20000+tableCount/2), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(20000+tableCount*3/4), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(tableCount/2), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(10000+tableCount*3/4), kv.MaxVersion),
				kv.InternalKey(kv.CFDefault, benchUserKey(50000), kv.MaxVersion),
			},
		},
	} {
		b.Run(mix.name, func(b *testing.B) {
			for _, useFilter := range []bool{false, true} {
				name := "linear"
				if useFilter {
					name = "range_filter"
				}
				b.Run(name, func(b *testing.B) {
					lsm := newBenchLSM(b, 64<<20)
					l1 := buildBenchLevelTablesAtOffset(b, lsm, 1, 0, tableCount)
					l2 := buildBenchLevelTablesAtOffset(b, lsm, 2, 10000, tableCount)
					l3 := buildBenchLevelTablesAtOffset(b, lsm, 3, 20000, tableCount)
					if !useFilter {
						disableBenchRangeFilter(l1, l2, l3)
					}
					keys := mix.keys
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						key := keys[i%len(keys)]
						entry, err := lsm.Get(key)
						if err == nil {
							if entry == nil {
								b.Fatalf("expected hit entry")
							}
							entry.DecrRef()
							continue
						}
						if err != utils.ErrKeyNotFound {
							b.Fatalf("unexpected mixed point err=%v", err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkLSMMultiLevelIteratorBoundsPruning(b *testing.B) {
	const tableCount = 1024
	for _, tc := range []struct {
		name  string
		lower []byte
		upper []byte
		want  int
	}{
		{
			name:  "narrow",
			lower: benchUserKey(10000 + tableCount/2),
			upper: benchUserKey(10000 + tableCount/2 + 1),
			want:  1,
		},
		{
			name:  "medium",
			lower: benchUserKey(10000 + tableCount/2),
			upper: benchUserKey(10000 + tableCount/2 + 8),
			want:  8,
		},
		{
			name:  "wide",
			lower: benchUserKey(10000 + tableCount/2),
			upper: benchUserKey(10000 + tableCount/2 + 64),
			want:  64,
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for _, useFilter := range []bool{false, true} {
				name := "linear"
				if useFilter {
					name = "range_filter"
				}
				b.Run(name, func(b *testing.B) {
					lsm := newBenchLSM(b, 64<<20)
					l1 := buildBenchLevelTablesAtOffset(b, lsm, 1, 0, tableCount)
					l2 := buildBenchLevelTablesAtOffset(b, lsm, 2, 10000, tableCount)
					l3 := buildBenchLevelTablesAtOffset(b, lsm, 3, 20000, tableCount)
					if !useFilter {
						disableBenchRangeFilter(l1, l2, l3)
					}
					opt := &index.Options{
						IsAsc:      true,
						LowerBound: tc.lower,
						UpperBound: tc.upper,
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						iters := lsm.NewIterators(opt)
						merge := NewMergeIterator(iters, false)
						merge.Rewind()
						count := 0
						for ; merge.Valid(); merge.Next() {
							count++
						}
						if count != tc.want {
							b.Fatalf("expected %d items in multilevel bounded scan, got %d", tc.want, count)
						}
						if err := merge.Close(); err != nil {
							b.Fatalf("close merge iterator: %v", err)
						}
					}
				})
			}
		})
	}
}

// openShardedBenchLSM mirrors openShardHintTestLSM but takes *testing.B and
// uses a larger memtable so the bench loops do not trigger flush.
func openShardedBenchLSM(b *testing.B, shardCount int) (*LSM, []*wal.Manager) {
	b.Helper()
	dir := b.TempDir()
	opts := newTestLSMOptions(dir, nil)
	opts.MemTableSize = 64 << 20
	wals := make([]*wal.Manager, shardCount)
	for i := range wals {
		mgr, err := wal.Open(wal.Config{Dir: dir + fmt.Sprintf("/wal-%02d", i)})
		if err != nil {
			b.Fatalf("open wal %d: %v", i, err)
		}
		wals[i] = mgr
	}
	lsm, err := NewLSM(opts, wals)
	if err != nil {
		b.Fatalf("new lsm: %v", err)
	}
	b.Cleanup(func() {
		_ = lsm.Close()
		for _, mgr := range wals {
			_ = mgr.Close()
		}
	})
	return lsm, wals
}

// makeShardedBenchEntries fabricates internal-key entries with monotonically
// increasing versions so MVCC tiebreak is well-defined.
func makeShardedBenchEntries(count, valueSize int) []*kv.Entry {
	entries := make([]*kv.Entry, count)
	value := make([]byte, valueSize)
	for i := range entries {
		userKey := make([]byte, 16)
		copy(userKey, "shardbench")
		binary.LittleEndian.PutUint64(userKey[8:], uint64(i))
		entries[i] = &kv.Entry{
			Key:     kv.InternalKey(kv.CFDefault, userKey, uint64(i+1)),
			Value:   value,
			CF:      kv.CFDefault,
			Version: uint64(i + 1),
		}
	}
	return entries
}

// BenchmarkShardedSetBatchByShardCount measures writeSome / SetBatchGroup
// throughput as the data plane shard count varies. Pins to shardID=0 so
// per-shard hot path cost is observed without dispatcher overhead.
func BenchmarkShardedSetBatchByShardCount(b *testing.B) {
	const batchSize = 64
	const valueSize = 128
	for _, shardCount := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("shards_%d", shardCount), func(b *testing.B) {
			lsm, _ := openShardedBenchLSM(b, shardCount)
			entries := makeShardedBenchEntries(batchSize, valueSize)
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * valueSize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := lsm.SetBatchGroup(0, [][]*kv.Entry{entries}); err != nil {
					b.Fatalf("set batch group: %v", err)
				}
			}
		})
	}
}

// BenchmarkShardedCrossShardMVCCMerge measures the cost of the cross-shard
// memtable walk + max-version selection without any hint. Seeds the same
// userKey on every shard with strictly increasing versions, then reads
// with kv.MaxVersion and verifies Get picks the latest.
func BenchmarkShardedCrossShardMVCCMerge(b *testing.B) {
	for _, shardCount := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("shards_%d", shardCount), func(b *testing.B) {
			lsm, _ := openShardedBenchLSM(b, shardCount)
			userKey := []byte("mvcc-merge-bench")
			value := []byte("v")

			for shardID := range shardCount {
				entry := &kv.Entry{
					Key:     kv.InternalKey(kv.CFDefault, userKey, uint64(shardID+1)),
					Value:   value,
					CF:      kv.CFDefault,
					Version: uint64(shardID + 1),
				}
				if _, err := lsm.SetBatchGroup(shardID, [][]*kv.Entry{{entry}}); err != nil {
					b.Fatalf("seed shard %d: %v", shardID, err)
				}
			}
			if lsm.shardHints != nil {
				lsm.shardHints = newShardHintTable()
			}

			query := kv.InternalKey(kv.CFDefault, userKey, kv.MaxVersion)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry, err := lsm.Get(query)
				if err != nil || entry == nil {
					b.Fatalf("get: err=%v entry=%v", err, entry)
				}
				if entry.Version != uint64(shardCount) {
					b.Fatalf("expected version=%d got %d", shardCount, entry.Version)
				}
				entry.DecrRef()
			}
		})
	}
}

// BenchmarkShardedNegativeCache measures the effect of the negative cache
// on a high-miss read workload. "warm_neg" lets the cache populate before
// timing; "cold_neg" wipes the cache every iteration so the hot path
// always pays the full miss probe through every shard's memtable + the
// L0..LN levels. Folded in from the now-deleted
// engine/lsm/negative_cache_bench_test.go after the cache itself moved
// to engine/slab/negativecache/.
func BenchmarkShardedNegativeCache(b *testing.B) {
	const seedHits = 256
	for _, mode := range []string{"warm_neg", "cold_neg"} {
		b.Run(mode, func(b *testing.B) {
			lsm, _ := openShardedBenchLSM(b, 4)
			value := make([]byte, 64)

			// Seed a small set of present keys so reads aren't all-miss
			// (otherwise levels.Get fast path may dominate).
			for i := range seedHits {
				userKey := make([]byte, 16)
				copy(userKey, "negbench-hit")
				binary.LittleEndian.PutUint64(userKey[8:], uint64(i))
				entry := &kv.Entry{
					Key:     kv.InternalKey(kv.CFDefault, userKey, uint64(i+1)),
					Value:   value,
					CF:      kv.CFDefault,
					Version: uint64(i + 1),
				}
				if _, err := lsm.SetBatchGroup(int(rand.Uint32())&3, [][]*kv.Entry{{entry}}); err != nil {
					b.Fatalf("seed: %v", err)
				}
			}

			missQuery := func(i int) []byte {
				userKey := make([]byte, 16)
				copy(userKey, "negbench-miss")
				binary.LittleEndian.PutUint64(userKey[8:], uint64(i))
				return kv.InternalKey(kv.CFDefault, userKey, kv.MaxVersion)
			}

			if mode == "warm_neg" {
				for i := range 1024 {
					_, _ = lsm.Get(missQuery(i % 1024))
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode == "cold_neg" && lsm.negatives != nil {
					lsm.negatives.Clear()
				}
				_, _ = lsm.Get(missQuery(i & 1023))
			}
		})
	}
}
