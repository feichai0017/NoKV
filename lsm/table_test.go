package lsm

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableDecrRefUnderflow(t *testing.T) {
	tbl := &table{fid: 1, ref: 2}
	require.NoError(t, tbl.DecrRef())
	require.Equal(t, int32(1), atomic.LoadInt32(&tbl.ref))

	// Avoid the 1->0 path in this unit test (which requires a real table handle).
	atomic.StoreInt32(&tbl.ref, 0)
	require.PanicsWithError(t, fmt.Errorf("table refcount underflow: fid %d, current_ref %d", tbl.fid, int32(0)).Error(), func() {
		_ = tbl.DecrRef()
	})
}
