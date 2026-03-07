package NoKV

import (
	"bytes"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

func newDeleteRangeTestOptions(dir string) *Options {
	cfg := *opt
	cfg.WorkDir = dir
	return &cfg
}

func mustSet(t *testing.T, db *DB, key, value []byte) {
	t.Helper()
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
}

func mustDeleteRange(t *testing.T, db *DB, start, end []byte) {
	t.Helper()
	if err := db.DeleteRange(start, end); err != nil {
		t.Fatal(err)
	}
}

func mustDel(t *testing.T, db *DB, key []byte) {
	t.Helper()
	if err := db.Del(key); err != nil {
		t.Fatal(err)
	}
}

func mustClose(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestDeleteRangeCore tests basic functionality, boundaries, lexicographic ordering,
// empty ranges, and write-after-delete scenarios.
func TestDeleteRangeCore(t *testing.T) {
	opt := newDeleteRangeTestOptions(t.TempDir())
	db := Open(opt)
	defer func() { mustClose(t, db) }()

	// Test 1: Basic deletion with [start, end) semantics
	mustSet(t, db, []byte("a"), []byte("1"))
	mustSet(t, db, []byte("b"), []byte("2"))
	mustSet(t, db, []byte("c"), []byte("3"))

	if err := db.DeleteRange([]byte("a"), []byte("c")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get([]byte("a")); err != utils.ErrKeyNotFound {
		t.Error("start key should be deleted")
	}
	if _, err := db.Get([]byte("b")); err != utils.ErrKeyNotFound {
		t.Error("middle key should be deleted")
	}
	if e, err := db.Get([]byte("c")); err != nil || !bytes.Equal(e.Value, []byte("3")) {
		t.Error("end key should not be deleted (exclusive)")
	}

	// Test 2: Lexicographic ordering
	mustSet(t, db, []byte("key1"), []byte("v1"))
	mustSet(t, db, []byte("key10"), []byte("v10"))
	mustSet(t, db, []byte("key2"), []byte("v2"))

	mustDeleteRange(t, db, []byte("key1"), []byte("key2"))

	if _, err := db.Get([]byte("key1")); err != utils.ErrKeyNotFound {
		t.Error("key1 should be deleted")
	}
	if _, err := db.Get([]byte("key10")); err != utils.ErrKeyNotFound {
		t.Error("key10 should be deleted (lexicographically between key1 and key2)")
	}
	if _, err := db.Get([]byte("key2")); err != nil {
		t.Error("key2 should exist (exclusive end)")
	}

	// Test 3: Empty range (no keys in range)
	mustSet(t, db, []byte("x"), []byte("1"))
	mustSet(t, db, []byte("z"), []byte("2"))

	if err := db.DeleteRange([]byte("xa"), []byte("xz")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get([]byte("x")); err != nil {
		t.Error("key before range should exist")
	}
	if _, err := db.Get([]byte("z")); err != nil {
		t.Error("key after range should exist")
	}

	// Test 4: Write after delete
	mustSet(t, db, []byte("rewrite"), []byte("old"))
	mustDeleteRange(t, db, []byte("rewrite"), []byte("rewritf"))

	if _, err := db.Get([]byte("rewrite")); err != utils.ErrKeyNotFound {
		t.Error("key should be deleted")
	}

	mustSet(t, db, []byte("rewrite"), []byte("new"))
	if e, err := db.Get([]byte("rewrite")); err != nil || !bytes.Equal(e.Value, []byte("new")) {
		t.Error("key should have new value after rewrite")
	}
}

// TestDeleteRangeValidation tests error handling for invalid inputs.
func TestDeleteRangeValidation(t *testing.T) {
	opt := newDeleteRangeTestOptions(t.TempDir())
	db := Open(opt)
	defer func() { mustClose(t, db) }()

	// Inverted range
	if err := db.DeleteRange([]byte("z"), []byte("a")); err != utils.ErrInvalidRequest {
		t.Errorf("expected invalid request for inverted range, got %v", err)
	}

	// Equal keys
	if err := db.DeleteRange([]byte("a"), []byte("a")); err != utils.ErrInvalidRequest {
		t.Errorf("expected invalid request for equal keys, got %v", err)
	}

	// Empty key
	if err := db.DeleteRange([]byte(""), []byte("a")); err != utils.ErrEmptyKey {
		t.Errorf("expected empty key error, got %v", err)
	}
}

// TestDeleteRangeCF tests column family isolation.
func TestDeleteRangeCF(t *testing.T) {
	opt := newDeleteRangeTestOptions(t.TempDir())
	db := Open(opt)
	defer func() { mustClose(t, db) }()

	defaultEntry := kv.NewInternalEntry(kv.CFDefault, []byte("key1"), nonTxnMaxVersion, []byte("val1"), 0, 0)
	lockEntry := kv.NewInternalEntry(kv.CFLock, []byte("key1"), nonTxnMaxVersion, []byte("lock1"), 0, 0)
	defer defaultEntry.DecrRef()
	defer lockEntry.DecrRef()
	if err := db.ApplyInternalEntries([]*kv.Entry{defaultEntry, lockEntry}); err != nil {
		t.Fatal(err)
	}

	if err := db.DeleteRangeCF(kv.CFDefault, []byte("key1"), []byte("key2")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.GetInternalEntry(kv.CFDefault, []byte("key1"), nonTxnMaxVersion); err != utils.ErrKeyNotFound {
		t.Error("default CF key should be deleted")
	}
	entry, err := db.GetInternalEntry(kv.CFLock, []byte("key1"), nonTxnMaxVersion)
	if err != nil {
		t.Error("lock CF key should still exist")
	} else {
		entry.DecrRef()
	}
}

// TestDeleteRangeComplex tests overlapping ranges and interaction with point deletes.
func TestDeleteRangeComplex(t *testing.T) {
	opt := newDeleteRangeTestOptions(t.TempDir())
	db := Open(opt)
	defer func() { mustClose(t, db) }()

	// Test 1: Overlapping ranges
	mustSet(t, db, []byte("a"), []byte("1"))
	mustSet(t, db, []byte("b"), []byte("2"))
	mustSet(t, db, []byte("c"), []byte("3"))
	mustSet(t, db, []byte("d"), []byte("4"))

	mustDeleteRange(t, db, []byte("a"), []byte("c"))
	mustDeleteRange(t, db, []byte("b"), []byte("d"))

	if _, err := db.Get([]byte("a")); err != utils.ErrKeyNotFound {
		t.Error("a should be deleted")
	}
	if _, err := db.Get([]byte("b")); err != utils.ErrKeyNotFound {
		t.Error("b should be deleted")
	}
	if _, err := db.Get([]byte("c")); err != utils.ErrKeyNotFound {
		t.Error("c should be deleted")
	}
	if _, err := db.Get([]byte("d")); err != nil {
		t.Error("d should exist")
	}

	// Test 2: Range delete over already deleted keys
	mustSet(t, db, []byte("x"), []byte("1"))
	mustSet(t, db, []byte("y"), []byte("2"))
	mustSet(t, db, []byte("z"), []byte("3"))

	mustDel(t, db, []byte("y"))

	if err := db.DeleteRange([]byte("x"), []byte("zz")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get([]byte("x")); err != utils.ErrKeyNotFound {
		t.Error("x should be deleted")
	}
	if _, err := db.Get([]byte("y")); err != utils.ErrKeyNotFound {
		t.Error("y should remain deleted")
	}
	if _, err := db.Get([]byte("z")); err != utils.ErrKeyNotFound {
		t.Error("z should be deleted")
	}
}

// TestDeleteRangeWithCompaction tests range deletion behavior during compaction.
func TestDeleteRangeWithCompaction(t *testing.T) {
	opt := newDeleteRangeTestOptions(t.TempDir())
	opt.MemTableSize = 1024
	db := Open(opt)
	defer func() { mustClose(t, db) }()

	for i := range 100 {
		key := []byte{byte('a' + i%26), byte(i)}
		mustSet(t, db, key, []byte("value"))
	}

	mustDeleteRange(t, db, []byte{byte('a')}, []byte{byte('m')})

	for i := range 100 {
		key := []byte{byte('a' + i%26), byte(i)}
		_, err := db.Get(key)
		if key[0] < 'm' {
			if err != utils.ErrKeyNotFound {
				t.Errorf("key %v should be deleted", key)
			}
		} else {
			if err != nil {
				t.Errorf("key %v should exist", key)
			}
		}
	}
}

// TestDeleteRangeWALRecovery tests that range tombstones are correctly recovered from WAL.
func TestDeleteRangeWALRecovery(t *testing.T) {
	dir := t.TempDir()
	opt := newDeleteRangeTestOptions(dir)

	db := Open(opt)
	mustSet(t, db, []byte("key1"), []byte("val1"))
	mustSet(t, db, []byte("key2"), []byte("val2"))
	mustSet(t, db, []byte("key3"), []byte("val3"))
	mustDeleteRange(t, db, []byte("key1"), []byte("key3"))
	mustClose(t, db)

	db = Open(opt)
	defer func() { mustClose(t, db) }()

	if _, err := db.Get([]byte("key1")); err != utils.ErrKeyNotFound {
		t.Error("key1 should be deleted after recovery")
	}
	if _, err := db.Get([]byte("key2")); err != utils.ErrKeyNotFound {
		t.Error("key2 should be deleted after recovery")
	}
	if _, err := db.Get([]byte("key3")); err != nil {
		t.Error("key3 should exist after recovery")
	}
}

// TestDeleteRangeVisibilityBug tests the bug where a newer point write
// gets incorrectly hidden by an older range tombstone when both use the same version.
func TestDeleteRangeVisibilityBug(t *testing.T) {
	opt := newDeleteRangeTestOptions(t.TempDir())
	db := Open(opt)
	defer func() { mustClose(t, db) }()

	mustSet(t, db, []byte("a1"), []byte("old"))
	mustDeleteRange(t, db, []byte("a0"), []byte("a9"))
	mustSet(t, db, []byte("a1"), []byte("new"))

	e, err := db.Get([]byte("a1"))
	if err != nil {
		t.Fatalf("expected key a1 to exist with value 'new', got error: %v", err)
	}
	if !bytes.Equal(e.Value, []byte("new")) {
		t.Errorf("expected value 'new', got '%s'", e.Value)
	}
}
