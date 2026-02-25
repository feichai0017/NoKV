package benchmark

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble"
)

// newPebbleEngine creates a YCSB engine backed by Pebble.
func newPebbleEngine(opts ycsbEngineOptions) ycsbEngine {
	return &pebbleEngine{opts: opts}
}

// pebbleEngine adapts Pebble's API to the benchmark engine interface.
type pebbleEngine struct {
	opts      ycsbEngineOptions
	db        *pebble.DB
	cache     *pebble.Cache
	writeOpts *pebble.WriteOptions
}

// Name returns the benchmark display name for this engine.
func (e *pebbleEngine) Name() string { return "Pebble" }

// Open initializes a Pebble instance in an engine-scoped benchmark directory.
func (e *pebbleEngine) Open(clean bool) error {
	dir := e.opts.engineDir("pebble")
	if clean {
		if err := ensureCleanDir(dir); err != nil {
			return fmt.Errorf("pebble: ensure dir: %w", err)
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("pebble: mkdir: %w", err)
	}

	opts := &pebble.Options{}
	if mb := e.opts.BlockCacheMB; mb > 0 {
		e.cache = pebble.NewCache(int64(mb) << 20)
		opts.Cache = e.cache
	}
	if mb := e.opts.MemtableMB; mb > 0 {
		opts.MemTableSize = uint64(mb) << 20
	}
	level0 := pebble.LevelOptions{
		Compression: parsePebbleCompression(e.opts.PebbleCompression),
	}
	if mb := e.opts.SSTableMB; mb > 0 {
		level0.TargetFileSize = int64(mb) << 20
	}
	opts.Levels = []pebble.LevelOptions{level0}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		if e.cache != nil {
			e.cache.Unref()
			e.cache = nil
		}
		return err
	}
	e.db = db
	if e.opts.SyncWrites {
		e.writeOpts = pebble.Sync
	} else {
		e.writeOpts = pebble.NoSync
	}
	return nil
}

// Close closes the DB and releases cache resources owned by the engine.
func (e *pebbleEngine) Close() error {
	var closeErr error
	if e.db != nil {
		closeErr = e.db.Close()
		e.db = nil
	}
	if e.cache != nil {
		e.cache.Unref()
		e.cache = nil
	}
	e.writeOpts = nil
	return closeErr
}

// Read fetches the value for key and copies it into dst.
func (e *pebbleEngine) Read(key []byte, dst []byte) ([]byte, error) {
	value, closer, err := e.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return dst[:0], nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	if cap(dst) < len(value) {
		dst = make([]byte, len(value))
	}
	dst = dst[:len(value)]
	copy(dst, value)
	return dst, nil
}

// Insert writes a key/value pair.
func (e *pebbleEngine) Insert(key, value []byte) error {
	return e.db.Set(key, value, e.writeOpts)
}

// Update overwrites a key/value pair.
func (e *pebbleEngine) Update(key, value []byte) error {
	return e.db.Set(key, value, e.writeOpts)
}

// Scan performs a forward iterator scan and touches each value.
func (e *pebbleEngine) Scan(startKey []byte, count int) (int, error) {
	it, err := e.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	read := 0
	valid := false
	if len(startKey) == 0 {
		valid = it.First()
	} else {
		valid = it.SeekGE(startKey)
	}
	for ; valid && read < count; valid = it.Next() {
		value, err := it.ValueAndErr()
		if err != nil {
			_ = it.Close()
			return 0, err
		}
		_ = len(value)
		read++
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		return 0, err
	}
	if err := it.Close(); err != nil {
		return 0, err
	}
	return read, nil
}

// parsePebbleCompression maps benchmark codec names to Pebble compression constants.
func parsePebbleCompression(codec string) pebble.Compression {
	switch strings.ToLower(strings.TrimSpace(codec)) {
	case "snappy":
		return pebble.SnappyCompression
	case "zstd":
		return pebble.ZstdCompression
	case "none":
		return pebble.NoCompression
	default:
		return pebble.NoCompression
	}
}
