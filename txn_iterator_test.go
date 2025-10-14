package NoKV

import (
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestTxnIteratorSnapshotIsolation(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Update(func(tx *Txn) error {
			return tx.SetEntry(utils.NewEntry([]byte("key1"), []byte("v1")))
		}))

		roTxn := db.NewTransaction(false)
		defer roTxn.Discard()

		require.NoError(t, db.Update(func(tx *Txn) error {
			return tx.SetEntry(utils.NewEntry([]byte("key1"), []byte("v2")))
		}))

		it := roTxn.NewIterator(IteratorOptions{})
		defer it.Close()

		it.Seek([]byte("key1"))
		require.True(t, it.Valid())
		require.Equal(t, "v1", string(it.Item().Entry().Value))
	})
}

func TestTxnIteratorSeesPendingWrites(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Update(func(tx *Txn) error {
			return tx.SetEntry(utils.NewEntry([]byte("key2"), []byte("base")))
		}))

		txn := db.NewTransaction(true)
		defer txn.Discard()

		require.NoError(t, txn.SetEntry(utils.NewEntry([]byte("key2"), []byte("override"))))

		it := txn.NewIterator(IteratorOptions{})
		defer it.Close()

		it.Seek([]byte("key2"))
		require.True(t, it.Valid())
		require.Equal(t, "override", string(it.Item().Entry().Value))

		it.Next()
		require.False(t, it.Valid())
	})
}

func TestTxnIteratorAllVersionsAndSinceTs(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		key := []byte("key3")
		for i := 0; i < 3; i++ {
			val := []byte(fmt.Sprintf("v%d", i+1))
			require.NoError(t, db.Update(func(tx *Txn) error {
				return tx.SetEntry(utils.NewEntry(key, val))
			}))
		}

		ro := db.NewTransaction(false)
		defer ro.Discard()

		it := ro.NewKeyIterator(key, IteratorOptions{})
		defer it.Close()

		it.Seek(key)
		var versions []uint64
		var values []string
		for ; it.Valid(); it.Next() {
			values = append(values, string(it.Item().Entry().Value))
			versions = append(versions, it.Item().Entry().Version)
		}
		require.Equal(t, []string{"v3", "v2", "v1"}, values)

		if len(versions) < 2 {
			t.Fatalf("expected multiple versions, got %v", versions)
		}

		opt := IteratorOptions{SinceTs: versions[1]}
		it2 := ro.NewIterator(opt)
		defer it2.Close()

		it2.Seek(key)
		require.True(t, it2.Valid())
		require.Equal(t, "v3", string(it2.Item().Entry().Value))
		it2.Next()
		require.False(t, it2.Valid())
	})
}
