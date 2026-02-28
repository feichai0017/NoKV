package NoKV

import (
	"bytes"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

// TestDeleteRangeCore tests basic functionality, boundaries, lexicographic ordering,
// empty ranges, and write-after-delete scenarios.
func TestDeleteRangeCore(t *testing.T) {
	opt := getTestOptions(t.TempDir())
	db := Open(opt)
	defer db.Close()

	// Test 1: Basic deletion with [start, end) semantics
	db.Set([]byte("a"), []byte("1"))
	db.Set([]byte("b"), []byte("2"))
	db.Set([]byte("c"), []byte("3"))

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
	db.Set([]byte("key1"), []byte("v1"))
	db.Set([]byte("key10"), []byte("v10"))
	db.Set([]byte("key2"), []byte("v2"))

	db.DeleteRange([]byte("key1"), []byte("key2"))

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
	db.Set([]byte("x"), []byte("1"))
	db.Set([]byte("z"), []byte("2"))

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
	db.Set([]byte("rewrite"), []byte("old"))
	db.DeleteRange([]byte("rewrite"), []byte("rewritf"))

	if _, err := db.Get([]byte("rewrite")); err != utils.ErrKeyNotFound {
		t.Error("key should be deleted")
	}

	db.Set([]byte("rewrite"), []byte("new"))
	if e, err := db.Get([]byte("rewrite")); err != nil || !bytes.Equal(e.Value, []byte("new")) {
		t.Error("key should have new value after rewrite")
	}
}

// TestDeleteRangeValidation tests error handling for invalid inputs.
func TestDeleteRangeValidation(t *testing.T) {
	opt := getTestOptions(t.TempDir())
	db := Open(opt)
	defer db.Close()

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
	opt := getTestOptions(t.TempDir())
	db := Open(opt)
	defer db.Close()

	db.SetCF(kv.CFDefault, []byte("key1"), []byte("val1"))
	db.SetCF(kv.CFLock, []byte("key1"), []byte("lock1"))

	db.DeleteRangeCF(kv.CFDefault, []byte("key1"), []byte("key2"))

	if _, err := db.GetCF(kv.CFDefault, []byte("key1")); err != utils.ErrKeyNotFound {
		t.Error("default CF key should be deleted")
	}
	if _, err := db.GetCF(kv.CFLock, []byte("key1")); err != nil {
		t.Error("lock CF key should still exist")
	}
}

// TestDeleteRangeComplex tests overlapping ranges and interaction with point deletes.
func TestDeleteRangeComplex(t *testing.T) {
	opt := getTestOptions(t.TempDir())
	db := Open(opt)
	defer db.Close()

	// Test 1: Overlapping ranges
	db.Set([]byte("a"), []byte("1"))
	db.Set([]byte("b"), []byte("2"))
	db.Set([]byte("c"), []byte("3"))
	db.Set([]byte("d"), []byte("4"))

	db.DeleteRange([]byte("a"), []byte("c"))
	db.DeleteRange([]byte("b"), []byte("d"))

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
	db.Set([]byte("x"), []byte("1"))
	db.Set([]byte("y"), []byte("2"))
	db.Set([]byte("z"), []byte("3"))

	db.Del([]byte("y"))

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
	opt := getTestOptions(t.TempDir())
	opt.MemTableSize = 1024
	db := Open(opt)
	defer db.Close()

	for i := 0; i < 100; i++ {
		key := []byte{byte('a' + i%26), byte(i)}
		db.Set(key, []byte("value"))
	}

	db.DeleteRange([]byte{byte('a')}, []byte{byte('m')})

	for i := 0; i < 100; i++ {
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
	opt := getTestOptions(dir)

	db := Open(opt)
	db.Set([]byte("key1"), []byte("val1"))
	db.Set([]byte("key2"), []byte("val2"))
	db.Set([]byte("key3"), []byte("val3"))
	db.DeleteRange([]byte("key1"), []byte("key3"))
	db.Close()

	db = Open(opt)
	defer db.Close()

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
