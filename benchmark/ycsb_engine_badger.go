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
	}
	blockCacheMB := e.opts.BadgerBlockCacheMB
	if blockCacheMB <= 0 {
		blockCacheMB = e.opts.BlockCacheMB
	}
	indexCacheMB := e.opts.BadgerIndexCacheMB
	if indexCacheMB <= 0 {
		indexCacheMB = blockCacheMB
	}
	opts := badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(e.opts.SyncWrites).
		WithCompression(comp).
		WithValueThreshold(int64(e.opts.ValueThreshold)).
		WithBlockCacheSize(int64(blockCacheMB) << 20).
		WithIndexCacheSize(int64(indexCacheMB) << 20).
		WithDetectConflicts(false)
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
	txn := e.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(dst)
}

func (e *badgerEngine) Insert(key, value []byte) error {
	txn := e.db.NewTransaction(true)
	defer txn.Discard()
	if err := txn.Set(key, value); err != nil {
		return err
	}
	return txn.Commit()
}

func (e *badgerEngine) Update(key, value []byte) error {
	txn := e.db.NewTransaction(true)
	defer txn.Discard()
	if err := txn.Set(key, value); err != nil {
		return err
	}
	return txn.Commit()
}

func (e *badgerEngine) Scan(startKey []byte, count int) (int, error) {
	var read int
	txn := e.db.NewTransaction(false)
	defer txn.Discard()

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
			return 0, err
		}
		read++
	}
	return read, nil
}
