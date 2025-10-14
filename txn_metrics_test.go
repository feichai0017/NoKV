package NoKV

import (
	"testing"

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

		require.NoError(t, txn1.SetEntry(utils.NewEntry(key, []byte("v1"))))
		require.NoError(t, txn1.Commit())

		require.NoError(t, txn2.SetEntry(utils.NewEntry(key, []byte("v2"))))
		err = txn2.Commit()
		require.ErrorIs(t, err, utils.ErrConflict)

		metrics := db.orc.txnMetricsSnapshot()
		require.GreaterOrEqual(t, metrics.Conflicts, uint64(1))
	})
}
