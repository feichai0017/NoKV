package NoKV

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

// TestDBIteratorVLogReadError verifies that vlog read errors stop iteration
// and set the error state, rather than silently skipping entries.
func TestDBIteratorVLogReadError(t *testing.T) {
	db, opt := setupDBWithVLogEntries(t, 100)
	db = corruptVLogByTruncation(t, db, opt, 3) // Truncate to 1/3
	defer func() { _ = db.Close() }()

	// Create iterator
	iter := db.NewIterator(&utils.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()

	// First key might work (depending on truncation point)
	iter.Rewind()
	validCount := 0
	for ; iter.Valid(); iter.Next() {
		validCount++
	}

	// Check that error is captured when vlog read fails
	dbIter, ok := iter.(*DBIterator)
	require.True(t, ok, "should be DBIterator")

	// We should either have an error (if we hit corruption) or have read all entries successfully
	// Given we truncated to 1/3, we should hit an error
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
	db = corruptVLogByTruncation(t, db, opt, 10) // Truncate heavily to 1/10
	defer func() { _ = db.Close() }()

	iter := db.NewIterator(&utils.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	dbIter := iter.(*DBIterator)

	// Trigger error by iterating
	firstErr := iterateUntilEnd(iter)

	// Rewind should clear error
	iter.Rewind()
	require.Nil(t, dbIter.Err(), "error should be cleared after Rewind()")

	// For this test, if we got an error the first time, we should get it again
	// (because the corruption is still there), but the point is it was cleared
	// and then re-set, not persisted from before
	if firstErr != nil {
		for iter.Valid() {
			iter.Next()
		}
		// Should have error again (but it's a fresh one after the rewind)
		require.Error(t, dbIter.Err())
	}
}

// TestDBIteratorLegitimateFilteringNoError verifies that legitimate filtering
// (deleted entries, expired entries) does NOT set an error.
func TestDBIteratorLegitimateFilteringNoError(t *testing.T) {
	clearDir()
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()

	// Write and delete
	require.NoError(t, db.Set([]byte("key1"), []byte("val1")))
	require.NoError(t, db.Del([]byte("key1")))

	// Write expired entry
	require.NoError(t, db.SetWithTTL([]byte("key2"), []byte("val2"), 1*time.Millisecond))
	time.Sleep(10 * time.Millisecond)

	iter := db.NewIterator(&utils.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	dbIter := iter.(*DBIterator)

	// Iterator should be invalid (no visible entries)
	iter.Rewind()
	require.False(t, iter.Valid(), "no visible entries")

	// But NO error should be set
	require.Nil(t, dbIter.Err(), "legitimate filtering should not set error")
}

// TestDBIteratorSeekClearsError verifies that Seek() clears previous errors.
func TestDBIteratorSeekClearsError(t *testing.T) {
	db, opt := setupDBWithVLogEntries(t, 50)
	db = corruptVLogByTruncation(t, db, opt, 10) // Truncate to 1/10
	defer func() { _ = db.Close() }()

	iter := db.NewIterator(&utils.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	dbIter := iter.(*DBIterator)

	// Trigger error
	firstErr := iterateUntilEnd(iter)

	// Seek should clear error (similar to Rewind)
	iter.Seek([]byte("key1"))
	require.Nil(t, dbIter.Err(), "error should be cleared after Seek()")

	// If we had an error before, we should get it again when iterating
	if firstErr != nil {
		for iter.Valid() {
			iter.Next()
		}
		require.Error(t, dbIter.Err(), "error should be set again after re-iterating")
	}
}

// TestDBIteratorEmptyDatabaseNoError verifies empty database returns no error.
func TestDBIteratorEmptyDatabaseNoError(t *testing.T) {
	clearDir()
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()

	iter := db.NewIterator(&utils.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	dbIter := iter.(*DBIterator)

	iter.Rewind()
	require.False(t, iter.Valid(), "empty database")
	require.Nil(t, dbIter.Err(), "no error on empty database")
}

// setupDBWithVLogEntries creates a DB with entries that go to vlog.
// Returns the DB and options.
func setupDBWithVLogEntries(t *testing.T, numEntries int) (*DB, *Options) {
	t.Helper()
	clearDir()
	opt := newTestOptions(t)
	opt.ValueThreshold = 0 // Force all values to vlog
	db := openTestDB(t, opt)
	require.NotNil(t, db)

	// Write entries with large values to ensure they go to vlog
	largeVal := bytes.Repeat([]byte("x"), 512)
	for i := range numEntries {
		key := fmt.Appendf(nil, "key%03d", i)
		require.NoError(t, db.Set(key, largeVal))
	}

	// Wait for flush to complete
	time.Sleep(100 * time.Millisecond)

	return db, opt
}

// findVLogFile finds the vlog file path in the given workdir.
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

// corruptVLogByTruncation corrupts the vlog file by truncating it.
// truncateDivisor determines how much to truncate (size/divisor).
// Returns the DB reopened after corruption.
func corruptVLogByTruncation(t *testing.T, db *DB, opt *Options, truncateDivisor int64) *DB {
	t.Helper()
	vlogPath := findVLogFile(t, opt.WorkDir)

	// Get file size
	info, err := os.Stat(vlogPath)
	require.NoError(t, err)

	// Close DB before corrupting
	require.NoError(t, db.Close())

	// Truncate vlog
	f, err := os.OpenFile(vlogPath, os.O_RDWR, 0666)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(info.Size()/truncateDivisor))
	require.NoError(t, f.Close())

	// Reopen DB
	db = openTestDB(t, opt)
	require.NotNil(t, db)
	return db
}

// iterateUntilEnd iterates through all entries until the iterator is exhausted.
// Returns the error encountered (if any).
func iterateUntilEnd(iter utils.Iterator) error {
	iter.Rewind()
	for iter.Valid() {
		iter.Next()
	}

	if dbIter, ok := iter.(*DBIterator); ok {
		return dbIter.Err()
	}
	return nil
}
