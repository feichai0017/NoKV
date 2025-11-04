package NoKV

import (
	"expvar"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestTxnConflictMetrics(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		key := []byte("conflict-key")
		txn1 := db.NewTransaction(true)
		txn2 := db.NewTransaction(true)

		_, err := txn2.Get(key)
		require.ErrorIs(t, err, utils.ErrKeyNotFound)

		require.NoError(t, txn1.SetEntry(kv.NewEntry(key, []byte("v1"))))
		require.NoError(t, txn1.Commit())

		require.NoError(t, txn2.SetEntry(kv.NewEntry(key, []byte("v2"))))
		err = txn2.Commit()
		require.ErrorIs(t, err, utils.ErrConflict)

		metrics := db.orc.txnMetricsSnapshot()
		require.GreaterOrEqual(t, metrics.Conflicts, uint64(1))
	})
}

func TestTxnMetricsTrackLongRunningTxn(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		baseline := db.Info().Snapshot()

		txn := db.NewTransaction(true)

		activeSnap := db.Info().Snapshot()
		require.Equal(t, baseline.TxnsActive+1, activeSnap.TxnsActive)
		require.GreaterOrEqual(t, activeSnap.TxnsStarted, baseline.TxnsStarted+1)

		require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("long-txn"), []byte("value"))))
		require.NoError(t, txn.Commit())

		committedSnap := db.Info().Snapshot()
		require.Equal(t, baseline.TxnsActive, committedSnap.TxnsActive)
		require.GreaterOrEqual(t, committedSnap.TxnsCommitted, baseline.TxnsCommitted+1)

		db.stats.collect()
		require.Equal(t, int64(committedSnap.TxnsActive), expvar.Get("NoKV.Txns.Active").(*expvar.Int).Value())
		require.Equal(t, int64(committedSnap.TxnsStarted), expvar.Get("NoKV.Txns.Started").(*expvar.Int).Value())
		require.Equal(t, int64(committedSnap.TxnsCommitted), expvar.Get("NoKV.Txns.Committed").(*expvar.Int).Value())
	})
}
