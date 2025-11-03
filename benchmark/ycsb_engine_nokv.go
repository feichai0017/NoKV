package benchmark

import (
	"errors"
	"fmt"
	"os"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/utils"
)

func newNoKVEngine(opts ycsbEngineOptions) ycsbEngine {
	return &nokvEngine{opts: opts}
}

type nokvEngine struct {
	opts ycsbEngineOptions
	db   *NoKV.DB
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
		WorkDir:             dir,
		MemTableSize:        64 << 20,
		SSTableMaxSz:        512 << 20,
		ValueLogFileSize:    512 << 20,
		ValueLogMaxEntries:  1 << 20,
		ValueThreshold:      int64(e.opts.ValueThreshold),
		MaxBatchCount:       10000,
		MaxBatchSize:        128 << 20,
		VerifyValueChecksum: true,
		DetectConflicts:     false,
		SyncWrites:          e.opts.SyncWrites,
	}
	if e.opts.BlockCacheMB >= 0 {
		// BlockCacheSize counts blocks; approximate 4KB blocks per MB.
		opt.BlockCacheSize = e.opts.BlockCacheMB * 256
	}
	e.db = NoKV.Open(opt)
	return nil
}

func (e *nokvEngine) Close() error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

func (e *nokvEngine) Read(key []byte) error {
	return e.db.View(func(txn *NoKV.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, utils.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		_, err = item.ValueCopy(nil)
		return err
	})
}

func (e *nokvEngine) Insert(key, value []byte) error {
	val := append([]byte(nil), value...)
	return e.db.Update(func(txn *NoKV.Txn) error {
		return txn.Set(key, val)
	})
}

func (e *nokvEngine) Update(key, value []byte) error {
	val := append([]byte(nil), value...)
	return e.db.Update(func(txn *NoKV.Txn) error {
		return txn.Set(key, val)
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
