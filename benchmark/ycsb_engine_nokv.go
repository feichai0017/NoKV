package benchmark

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/utils"
)

func newNoKVEngine(opts ycsbEngineOptions) ycsbEngine {
	return newNoKVEngineWithMemtable(opts, "nokv", "NoKV", NoKV.MemTableEngineART)
}

type nokvEngine struct {
	opts           ycsbEngineOptions
	db             *NoKV.DB
	valuePool      sync.Pool
	valueSize      int
	valueCap       int
	statsStop      chan struct{}
	statsWG        sync.WaitGroup
	engineID       string
	name           string
	memtableEngine NoKV.MemTableEngine
}

func newNoKVEngineWithMemtable(opts ycsbEngineOptions, engineID, name string, memtable NoKV.MemTableEngine) ycsbEngine {
	return &nokvEngine{
		opts:           opts,
		engineID:       engineID,
		name:           name,
		memtableEngine: memtable,
	}
}

func (e *nokvEngine) Name() string {
	if e.name != "" {
		return e.name
	}
	if e.engineID != "" {
		return e.engineID
	}
	return "NoKV"
}

func (e *nokvEngine) Open(clean bool) error {
	engineID := e.engineID
	if engineID == "" {
		engineID = "nokv"
	}
	dir := e.opts.engineDir(engineID)
	if clean {
		if err := ensureCleanDir(dir); err != nil {
			return fmt.Errorf("nokv: ensure dir: %w", err)
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("nokv: mkdir: %w", err)
	}
	opt := &NoKV.Options{
		WorkDir:            dir,
		MemTableSize:       int64(e.opts.MemtableMB) << 20,
		SSTableMaxSz:       int64(e.opts.SSTableMB) << 20,
		ValueLogFileSize:   e.opts.VlogFileMB << 20,
		ValueLogMaxEntries: 1 << 20,
		ValueThreshold:     int64(e.opts.ValueThreshold),
		MaxBatchCount:      10000,
		MaxBatchSize:       128 << 20,
		DetectConflicts:    false,
		SyncWrites:         e.opts.SyncWrites,
	}
	if e.memtableEngine != "" {
		opt.MemTableEngine = e.memtableEngine
	}
	if e.opts.BlockCacheMB >= 0 {
		// BlockCacheSize counts blocks; translate MB to ~4KB blocks for parity with other engines.
		opt.BlockCacheSize = e.opts.BlockCacheMB * (1 << 20) / (4 << 10)
	}
	e.db = NoKV.Open(opt)
	e.valueSize = e.opts.ValueSize
	if e.valueSize <= 0 {
		e.valueSize = 1
	}
	e.valueCap = e.valueSize
	e.valuePool.New = func() any { return make([]byte, e.valueCap) }
	e.startStatsTicker()
	return nil
}

func (e *nokvEngine) Close() error {
	if e.statsStop != nil {
		close(e.statsStop)
		e.statsWG.Wait()
		e.statsStop = nil
	}
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

func (e *nokvEngine) Read(key []byte, dst []byte) ([]byte, error) {
	var out []byte
	err := e.db.View(func(txn *NoKV.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, utils.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		out, err = item.ValueCopy(dst)
		return err
	})
	return out, err
}

func (e *nokvEngine) Insert(key, value []byte) error {
	return e.db.Update(func(txn *NoKV.Txn) error {
		return txn.Set(key, value)
	})
}

func (e *nokvEngine) Update(key, value []byte) error {
	return e.db.Update(func(txn *NoKV.Txn) error {
		return txn.Set(key, value)
	})
}

func (e *nokvEngine) Scan(startKey []byte, count int) (int, error) {
	var read int
	err := e.db.View(func(txn *NoKV.Txn) error {
		it := txn.NewIterator(NoKV.IteratorOptions{})
		defer it.Close()
		it.Seek(startKey)
		for ; it.Valid() && read < count; it.Next() {
			if _, err := it.Item().ValueCopy(nil); err != nil {
				return err
			}
			read++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return read, nil
}

func (e *nokvEngine) startStatsTicker() {
	interval := os.Getenv("NOKV_BENCH_STATS_INTERVAL")
	if interval == "" || e.db == nil {
		return
	}
	d, err := time.ParseDuration(interval)
	if err != nil || d <= 0 {
		return
	}
	e.statsStop = make(chan struct{})
	e.statsWG.Add(1)
	go func() {
		defer e.statsWG.Done()
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				e.printStats()
			case <-e.statsStop:
				return
			}
		}
	}()
}

func (e *nokvEngine) printStats() {
	if e.db == nil {
		return
	}
	snap := e.db.Info().Snapshot()
	var (
		l0Tables int
		l0Bytes  int64
		l0Ingest int
	)
	for _, lvl := range snap.LSMLevels {
		if lvl.Level == 0 {
			l0Tables = lvl.TableCount
			l0Bytes = lvl.SizeBytes
			l0Ingest = lvl.IngestTables
			break
		}
	}
	fmt.Printf("[NoKV Stats] entries=%d l0_tables=%d l0_bytes=%d l0_ingest=%d flush_pending=%d compaction_backlog=%d compaction_max=%.2f write_q=%d write_entries=%d write_bytes=%d throttle=%v vlog_segments=%d vlog_pending=%d vlog_discard=%d\n",
		snap.Entries,
		l0Tables,
		l0Bytes,
		l0Ingest,
		snap.FlushPending,
		snap.CompactionBacklog,
		snap.CompactionMaxScore,
		snap.WriteQueueDepth,
		snap.WriteQueueEntries,
		snap.WriteQueueBytes,
		snap.WriteThrottleActive,
		snap.ValueLogSegments,
		snap.ValueLogPendingDel,
		snap.ValueLogDiscardQueue,
	)
}
