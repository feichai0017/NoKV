package benchmark

import (
	"errors"
	"fmt"
	"os"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

func newBadgerEngine(opts ycsbEngineOptions) ycsbEngine {
	return &badgerEngine{opts: opts}
}

type badgerEngine struct {
	opts ycsbEngineOptions
	db   *badger.DB
}

func (e *badgerEngine) Name() string { return "Badger" }

func (e *badgerEngine) Open(clean bool) error {
	dir := e.opts.engineDir("badger")
	if clean {
		if err := ensureCleanDir(dir); err != nil {
			return fmt.Errorf("badger: ensure dir: %w", err)
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("badger: mkdir: %w", err)
	}
	comp := options.None
	switch e.opts.BadgerCompression {
	case "snappy":
		comp = options.Snappy
	case "zstd":
		comp = options.ZSTD
	default:
		comp = options.None
	}
	opts := badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(e.opts.SyncWrites).
		WithCompression(comp).
		WithValueThreshold(int64(e.opts.ValueThreshold)).
		WithBlockCacheSize(int64(e.opts.BadgerBlockCacheMB) << 20).
		WithIndexCacheSize(int64(e.opts.BadgerIndexCacheMB) << 20)
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	e.db = db
	return nil
}

func (e *badgerEngine) Close() error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

func (e *badgerEngine) Read(key []byte, dst []byte) ([]byte, error) {
	var out []byte
	err := e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		out, err = item.ValueCopy(dst)
		return err
	})
	return out, err
}

func (e *badgerEngine) Insert(key, value []byte) error {
	val := append([]byte(nil), value...)
	return e.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func (e *badgerEngine) Update(key, value []byte) error {
	val := append([]byte(nil), value...)
	return e.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func (e *badgerEngine) Scan(startKey []byte, count int) (int, error) {
	var read int
	err := e.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchSize:   count,
			PrefetchValues: true,
		})
		defer it.Close()
		for it.Seek(startKey); it.Valid(); it.Next() {
			if read >= count {
				break
			}
			item := it.Item()
			if item == nil {
				break
			}
			if err := item.Value(func(_ []byte) error { return nil }); err != nil {
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
