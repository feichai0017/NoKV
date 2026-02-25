package lsm

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableDecrRefUnderflow(t *testing.T) {
	tbl := &table{fid: 1, ref: 2}

	// 2 -> 1 should be a normal decrement without panic.
	require.NoError(t, tbl.DecrRef())
	require.Equal(t, int32(1), atomic.LoadInt32(&tbl.ref))

	// Avoid the 1->0 path in this unit test because Delete() expects a real table handle.
	atomic.StoreInt32(&tbl.ref, 0)
	require.PanicsWithValue(t, "table.DecrRef: refcount underflow (double release)", func() {
		_ = tbl.DecrRef()
	})
}
