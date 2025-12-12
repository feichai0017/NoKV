package benchmark

import (
	"errors"
	"fmt"
	"os"
	"sync"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/utils"
)

func newNoKVEngine(opts ycsbEngineOptions) ycsbEngine {
	return &nokvEngine{opts: opts}
}

type nokvEngine struct {
	opts      ycsbEngineOptions
	db        *NoKV.DB
	valuePool sync.Pool
	valueSize int
	valueCap  int
}

func (e *nokvEngine) Name() string { return "NoKV" }

func (e *nokvEngine) Open(clean bool) error {
	dir := e.opts.engineDir("nokv")
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
	return nil
}

func (e *nokvEngine) Close() error {
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
