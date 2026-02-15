package NoKV

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
	"github.com/stretchr/testify/require"
)

var DefaultIteratorOptions = IteratorOptions{
	Reverse:     false,
	AllVersions: false,
}

func runNoKVTest(t *testing.T, opts *Options, test func(t *testing.T, db *DB)) {
	dir, err := os.MkdirTemp("", "NoKV-test")
	require.NoError(t, err)
	defer removeDir(dir)
	if opts == nil {
		opts = getTestOptions(dir)
	} else {
		opts.WorkDir = dir
		opts.DetectConflicts = true

	}

	db := Open(opts)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	test(t, db)
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("Error while removing dir: %v\n", err)
	}
}

func getTestOptions(dir string) *Options {
	opt = &Options{
		WorkDir:             dir,
		SSTableMaxSz:        1 << 10,
		MemTableSize:        1 << 10,
		ValueLogFileSize:    1 << 20,
		ValueThreshold:      0,
		ValueLogBucketCount: 1,
		MaxBatchCount:       20,
		MaxBatchSize:        1 << 20,
		DetectConflicts:     true,
		HotRingEnabled:      true,
		HotRingBits:         8,
		HotRingTopK:         8,
	}
	return opt
}

func TestTxnSimple(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		txn := db.NewTransaction(true)

		for i := range 10 {
			k := fmt.Appendf(nil, "key=%d", i)
			v := fmt.Appendf(nil, "val=%d", i)
			require.NoError(t, txn.SetEntry(kv.NewEntry(k, v)))
		}

		item, err := txn.Get([]byte("key=8"))
		require.NoError(t, err)

		require.Equal(t, "val=8", string(item.Entry().Value))
		require.NoError(t, txn.Commit())
	})
}

func TestTxnGetEntryIsDetached(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte("detached-key"), []byte("detached-value"))
		}))

		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get([]byte("detached-key"))
			require.NoError(t, err)
			require.Equal(t, []byte("detached-value"), item.Entry().Value)

			// Txn.Get no longer returns pool-managed entries.
			item.Entry().DecrRef()
			require.Equal(t, []byte("detached-value"), item.Entry().Value)

			val, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, []byte("detached-value"), val)
			return nil
		}))
	})
}

func TestTxnReadAfterWrite(t *testing.T) {
	test := func(t *testing.T, db *DB) {
		var wg sync.WaitGroup
		N := 100
		wg.Add(N)
		for i := range N {
			go func(i int) {
				defer wg.Done()
				key := fmt.Appendf(nil, "key%d", i)
				err := db.Update(func(tx *Txn) error {
					return tx.SetEntry(kv.NewEntry(key, key))
				})
				require.NoError(t, err)
				err = db.View(func(tx *Txn) error {
					item, err := tx.Get(key)
					require.NoError(t, err)
					require.NoError(t, err)
					require.Equal(t, key, item.e.Key)
					return nil
				})
				require.NoError(t, err)
			}(i)
		}
		wg.Wait()
	}
	t.Run("disk mode", func(t *testing.T) {
		runNoKVTest(t, nil, func(t *testing.T, db *DB) {
			test(t, db)
		})
	})
}

func TestTxnVersions(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		k := []byte("key")
		for i := 1; i < 10; i++ {
			txn := db.NewTransaction(true)

			require.NoError(t, txn.SetEntry(kv.NewEntry(k, []byte(fmt.Sprintf("valversion=%d", i)))))
			require.NoError(t, txn.Commit())
			require.Equal(t, uint64(i), db.orc.readTs())
		}

		checkIterator := func(itr *TxnIterator, i int) {
			defer itr.Close()
			count := 0
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				require.Equal(t, k, item.Entry().Key)

				val := item.Entry().Value
				exp := fmt.Sprintf("valversion=%d", i)
				require.Equal(t, exp, string(val), "i=%d", i)
				count++
			}
			require.Equal(t, 1, count, "i=%d", i) // Should only loop once.
		}

		checkAllVersions := func(itr *TxnIterator, i int) {
			version := uint64(i)

			count := 0
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				require.Equal(t, k, item.Entry().Key)
				require.Equal(t, version, item.Entry().Version)

				val := item.Entry().Value
				exp := fmt.Sprintf("valversion=%d", version)
				require.Equal(t, exp, string(val), "v=%d", version)
				count++

				version--
			}
			require.Equal(t, i, count, "i=%d", i) // Should loop as many times as i.
		}

		for i := 1; i < 10; i++ {
			txn := db.NewTransaction(true)
			txn.readTs = uint64(i) // Read version at i.

			item, err := txn.Get(k)
			require.NoError(t, err)

			val := item.Entry().Value
			require.Equal(t, []byte(fmt.Sprintf("valversion=%d", i)), val,
				"Expected versions to match up at i=%d", i)

			// Try retrieving the latest version forward and reverse.
			itr := txn.NewIterator(DefaultIteratorOptions)
			checkIterator(itr, i)

			opt := DefaultIteratorOptions
			opt.Reverse = true
			itr = txn.NewIterator(opt)
			checkIterator(itr, i)

			// Now try retrieving all versions forward and reverse.
			opt = DefaultIteratorOptions
			opt.AllVersions = true
			itr = txn.NewIterator(opt)
			checkAllVersions(itr, i)
			itr.Close()

			opt = DefaultIteratorOptions
			opt.AllVersions = true
			opt.Reverse = true
			itr = txn.NewIterator(opt)
			checkAllVersions(itr, i)
			itr.Close()

			txn.Discard()
		}
		txn := db.NewTransaction(true)
		defer txn.Discard()
		item, err := txn.Get(k)
		require.NoError(t, err)

		val, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("valversion=9"), val)
	})
}

func TestTxnWriteSkew(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		// Accounts
		ax := []byte("x")
		ay := []byte("y")

		// Set balance to $100 in each account.
		txn := db.NewTransaction(true)
		defer txn.Discard()
		val := []byte(strconv.Itoa(100))
		require.NoError(t, txn.SetEntry(kv.NewEntry(ax, val)))
		require.NoError(t, txn.SetEntry(kv.NewEntry(ay, val)))
		require.NoError(t, txn.Commit())
		require.Equal(t, uint64(1), db.orc.readTs())

		getBal := func(txn *Txn, key []byte) (bal int) {
			item, err := txn.Get(key)
			require.NoError(t, err)

			val := item.Entry().Value
			bal, err = strconv.Atoi(string(val))
			require.NoError(t, err)
			return bal
		}

		// Start two transactions, each would read both accounts and deduct from one account.
		txn1 := db.NewTransaction(true)

		sum := getBal(txn1, ax)
		sum += getBal(txn1, ay)
		require.Equal(t, 200, sum)
		require.NoError(t, txn1.SetEntry(kv.NewEntry(ax, []byte("0")))) // Deduct 100 from ax.

		// Let's read this back.
		sum = getBal(txn1, ax)
		require.Equal(t, 0, sum)
		sum += getBal(txn1, ay)
		require.Equal(t, 100, sum)
		// Don't commit yet.

		txn2 := db.NewTransaction(true)

		sum = getBal(txn2, ax)
		sum += getBal(txn2, ay)
		require.Equal(t, 200, sum)
		require.NoError(t, txn2.SetEntry(kv.NewEntry(ay, []byte("0")))) // Deduct 100 from ay.

		// Let's read this back.
		sum = getBal(txn2, ax)
		require.Equal(t, 100, sum)
		sum += getBal(txn2, ay)
		require.Equal(t, 100, sum)

		// Commit both now.
		require.NoError(t, txn1.Commit())
		require.Error(t, txn2.Commit()) // This should fail.

		require.Equal(t, uint64(2), db.orc.readTs())
	})
}

func TestConflict(t *testing.T) {
	key := []byte("foo")
	setCount := uint32(0)

	testAndSet := func(wg *sync.WaitGroup, db *DB) {
		defer wg.Done()
		txn := db.NewTransaction(true)
		defer txn.Discard()

		_, err := txn.Get(key)
		if err == utils.ErrKeyNotFound {
			// Unset the error.
			err = nil
			require.NoError(t, txn.Set(key, []byte("AA")))
			txn.CommitWith(func(err error) {
				if err == nil {
					require.LessOrEqual(t, uint32(1), atomic.AddUint32(&setCount, 1))
				} else {
					require.Error(t, err, utils.ErrConflict)
				}
			})
		}
		require.NoError(t, err)
	}

	testAndSetItr := func(wg *sync.WaitGroup, db *DB) {
		defer wg.Done()
		txn := db.NewTransaction(true)
		defer txn.Discard()

		iopt := DefaultIteratorOptions
		it := txn.NewIterator(iopt)

		found := false
		for it.Seek(key); it.Valid(); it.Next() {
			found = true
		}
		it.Close()

		if !found {
			require.NoError(t, txn.Set(key, []byte("AA")))
			txn.CommitWith(func(err error) {
				if err == nil {
					require.LessOrEqual(t, atomic.AddUint32(&setCount, 1), uint32(1))
				} else {
					require.Error(t, err, utils.ErrConflict)
				}
			})
		}
	}

	runTest := func(t *testing.T, fn func(wg *sync.WaitGroup, db *DB)) {
		loop := 10
		numGo := 16 // This many concurrent transactions.
		for range loop {
			var wg sync.WaitGroup
			wg.Add(numGo)
			setCount = 0
			runNoKVTest(t, nil, func(t *testing.T, db *DB) {
				for range numGo {
					go fn(&wg, db)
				}
				wg.Wait()
			})
			require.Equal(t, uint32(1), atomic.LoadUint32(&setCount))
		}
	}

	t.Run("TxnGet", func(t *testing.T) {
		runTest(t, testAndSet)
	})

	t.Run("ItrSeek", func(t *testing.T) {
		runTest(t, testAndSetItr)
	})
}

func TestTxnCommitRollsBackOnValueLogError(t *testing.T) {
	cfg := *opt
	cfg.WorkDir = t.TempDir()
	cfg.ValueLogFileSize = 256
	rotatePath := filepath.Join(cfg.WorkDir, "vlog", "bucket-000", "00001.vlog")
	injected := fmt.Errorf("rotate openfile injected")
	cfg.FS = vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpOpenFile, rotatePath, injected),
	))
	db := Open(&cfg)
	defer func() { _ = db.Close() }()

	head := db.vlog.managers[0].Head()

	err := db.Update(func(txn *Txn) error {
		return txn.Set([]byte("txn-key"), bytes.Repeat([]byte("v"), 512))
	})
	if err == nil {
		t.Fatalf("expected error from value log failure")
	}

	require.Equal(t, head, db.vlog.managers[0].Head())
	_, err = db.Get([]byte("txn-key"))
	require.Equal(t, utils.ErrKeyNotFound, err)
}

func TestTxnIteratorBasic(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		txn := db.NewTransaction(true)
		keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		for i, k := range keys {
			require.NoError(t, txn.Set(k, []byte{byte('v' + i)}))
		}
		require.NoError(t, txn.Commit())

		readTxn := db.NewTransaction(false)
		it := readTxn.NewIterator(DefaultIteratorOptions)
		it.Rewind()
		var got [][]byte
		for it.Valid() {
			got = append(got, append([]byte(nil), it.Item().Entry().Key...))
			it.Next()
		}
		it.Close()
		require.ElementsMatch(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, got)
		readTxn.Discard()
	})
}

func TestTxnIteratorSeekAndReverse(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		txn := db.NewTransaction(true)
		keys := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
		for _, k := range keys {
			require.NoError(t, txn.Set(k, []byte("v")))
		}
		require.NoError(t, txn.Commit())

		// Forward seek
		readTxn := db.NewTransaction(false)
		it := readTxn.NewIterator(DefaultIteratorOptions)
		it.Seek([]byte("b"))
		var forward []byte
		for it.Valid() {
			forward = append(forward, it.Item().Entry().Key...)
			it.Next()
		}
		it.Close()
		readTxn.Discard()

		require.Equal(t, []byte("bcd"), forward)

		// Reverse
		readTxn2 := db.NewTransaction(false)
		revOpt := DefaultIteratorOptions
		revOpt.Reverse = true
		it2 := readTxn2.NewIterator(revOpt)
		it2.Rewind()
		var reverse []byte
		for it2.Valid() {
			reverse = append(reverse, it2.Item().Entry().Key...)
			it2.Next()
		}
		it2.Close()
		readTxn2.Discard()

		require.Equal(t, []byte("abcd"), reverse)
	})
}

func TestTxnMetricsCounters(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		// Simple read and write to bump counters.
		_ = db.View(func(tx *Txn) error {
			_, _ = tx.Get([]byte("missing"))
			return nil
		})
		_ = db.Update(func(tx *Txn) error {
			return tx.Set([]byte("m"), []byte("v"))
		})

		snap := db.orc.txnMetricsSnapshot()
		require.GreaterOrEqual(t, snap.Started, uint64(2))
	})
}

// TestTxnEmptyCommitLeak verifies that committing a transaction with no pending writes
// correctly decrements the active transaction counter and cleans up resources.
// It covers both synchronous Commit() and asynchronous CommitWith() paths.
func TestTxnEmptyCommitLeak(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		// 1. Test synchronous Commit() path
		txn := db.NewTransaction(true)
		snap := db.orc.txnMetricsSnapshot()
		require.Equal(t, int64(1), snap.Active, "Expected 1 active txn after creation")

		err := txn.Commit()
		require.NoError(t, err)

		snap = db.orc.txnMetricsSnapshot()
		require.Equal(t, int64(0), snap.Active, "Commit() on empty txn should decrement Active counter")

		// 2. Test asynchronous CommitWith() path
		txn2 := db.NewTransaction(true)
		snap = db.orc.txnMetricsSnapshot()
		require.Equal(t, int64(1), snap.Active, "Expected 1 active txn for second transaction")

		done := make(chan struct{})
		txn2.CommitWith(func(err error) {
			require.NoError(t, err)
			close(done)
		})

		// Use select with timeout to avoid hanging the test suite indefinitely.
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Test timed out waiting for CommitWith callback")
		}

		snap = db.orc.txnMetricsSnapshot()
		require.Equal(t, int64(0), snap.Active, "CommitWith() on empty txn should decrement Active counter")
	})
}
