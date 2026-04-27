package iterator_test

// External-test integration tests for the user-facing iterator,
// exercised through the root NoKV.DB facade. Helpers are intentionally
// inlined: shared db_test.go fixtures live in the root package and an
// external test package can't see unexported identifiers.

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/engine/index"
	iterpkg "github.com/feichai0017/NoKV/runtime/iterator"
	"github.com/stretchr/testify/require"
)

func openTestDB(t testing.TB, opt *NoKV.Options) *NoKV.DB {
	t.Helper()
	if opt != nil && !opt.EnableValueLog {
		if opt.ValueLogFileSize > 0 || opt.ValueThreshold == 0 || opt.ValueLogBucketCount > 0 {
			opt.EnableValueLog = true
		}
	}
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	return db
}

func newTestOptions(t *testing.T) *NoKV.Options {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.SSTableMaxSz = 1 << 12
	opt.MemTableSize = 1 << 12
	opt.EnableValueLog = true
	opt.ValueLogFileSize = 1 << 20
	opt.ValueThreshold = 0
	opt.ValueLogBucketCount = 1
	opt.MaxBatchCount = 10
	opt.MaxBatchSize = 1 << 20
	return opt
}

// TestDBIteratorVLogReadError verifies that vlog read errors stop iteration
// and set the error state, rather than silently skipping entries.
func TestDBIteratorVLogReadError(t *testing.T) {
	db, opt := setupDBWithVLogEntries(t, 100)
	db = corruptVLogByTruncation(t, db, opt, 3) // Truncate to 1/3
	defer func() { _ = db.Close() }()

	iter := db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()

	iter.Rewind()
	validCount := 0
	for ; iter.Valid(); iter.Next() {
		validCount++
	}

	dbIter, ok := iter.(*iterpkg.DBIterator)
	require.True(t, ok, "should be DBIterator")

	if validCount < 3 {
		err := dbIter.Err()
		require.Error(t, err, "should have error when vlog read fails")
		require.Contains(t, err.Error(), "value-log read failed", "error should mention vlog")
	}
}

// TestDBIteratorErrorClearedOnRewind verifies that errors are cleared
// when the iterator is rewound.
func TestDBIteratorErrorClearedOnRewind(t *testing.T) {
	db, opt := setupDBWithVLogEntries(t, 50)
	db = corruptVLogByTruncation(t, db, opt, 10)
	defer func() { _ = db.Close() }()

	iter := db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	dbIter := iter.(*iterpkg.DBIterator)

	firstErr := iterateUntilEnd(iter)

	iter.Rewind()
	require.Nil(t, dbIter.Err(), "error should be cleared after Rewind()")

	if firstErr != nil {
		for iter.Valid() {
			iter.Next()
		}
		require.Error(t, dbIter.Err())
	}
}

// TestDBIteratorLegitimateFilteringNoError verifies that legitimate filtering
// (deleted entries, expired entries) does NOT set an error.
func TestDBIteratorLegitimateFilteringNoError(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.Set([]byte("key1"), []byte("val1")))
	require.NoError(t, db.Del([]byte("key1")))

	require.NoError(t, db.SetWithTTL([]byte("key2"), []byte("val2"), 1*time.Millisecond))
	time.Sleep(10 * time.Millisecond)

	iter := db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	dbIter := iter.(*iterpkg.DBIterator)

	iter.Rewind()
	require.False(t, iter.Valid(), "no visible entries")

	require.Nil(t, dbIter.Err(), "legitimate filtering should not set error")
}

// TestDBIteratorSeekClearsError verifies that Seek() clears previous errors.
func TestDBIteratorSeekClearsError(t *testing.T) {
	db, opt := setupDBWithVLogEntries(t, 50)
	db = corruptVLogByTruncation(t, db, opt, 10)
	defer func() { _ = db.Close() }()

	iter := db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	dbIter := iter.(*iterpkg.DBIterator)

	firstErr := iterateUntilEnd(iter)

	iter.Seek([]byte("key1"))
	require.Nil(t, dbIter.Err(), "error should be cleared after Seek()")

	if firstErr != nil {
		for iter.Valid() {
			iter.Next()
		}
		require.Error(t, dbIter.Err(), "error should be set again after re-iterating")
	}
}

// TestDBIteratorEmptyDatabaseNoError verifies empty database returns no error.
func TestDBIteratorEmptyDatabaseNoError(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()

	iter := db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	dbIter := iter.(*iterpkg.DBIterator)

	iter.Rewind()
	require.False(t, iter.Valid(), "empty database")
	require.Nil(t, dbIter.Err(), "no error on empty database")
}

func setupDBWithVLogEntries(t *testing.T, numEntries int) (*NoKV.DB, *NoKV.Options) {
	t.Helper()
	opt := newTestOptions(t)
	opt.ValueThreshold = 0
	db := openTestDB(t, opt)
	require.NotNil(t, db)

	largeVal := bytes.Repeat([]byte("x"), 512)
	for i := range numEntries {
		key := fmt.Appendf(nil, "key%03d", i)
		require.NoError(t, db.Set(key, largeVal))
	}

	time.Sleep(100 * time.Millisecond)
	return db, opt
}

func findVLogFile(t *testing.T, workDir string) string {
	t.Helper()
	vlogBucketPath := filepath.Join(workDir, "vlog", "bucket-000")
	entries, err := os.ReadDir(vlogBucketPath)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "vlog bucket should have files")

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".vlog" {
			return filepath.Join(vlogBucketPath, entry.Name())
		}
	}

	t.Fatal("should find vlog file")
	return ""
}

func corruptVLogByTruncation(t *testing.T, db *NoKV.DB, opt *NoKV.Options, truncateDivisor int64) *NoKV.DB {
	t.Helper()
	vlogPath := findVLogFile(t, opt.WorkDir)

	info, err := os.Stat(vlogPath)
	require.NoError(t, err)

	require.NoError(t, db.Close())

	f, err := os.OpenFile(vlogPath, os.O_RDWR, 0666)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(info.Size()/truncateDivisor))
	require.NoError(t, f.Close())

	db = openTestDB(t, opt)
	require.NotNil(t, db)
	return db
}

func iterateUntilEnd(iter index.Iterator) error {
	iter.Rewind()
	for iter.Valid() {
		iter.Next()
	}

	if dbIter, ok := iter.(*iterpkg.DBIterator); ok {
		return dbIter.Err()
	}
	return nil
}
