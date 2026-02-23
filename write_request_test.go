package NoKV

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestDecrRefUnderflowPanics(t *testing.T) {
	req := &request{}
	require.PanicsWithValue(t, "request.DecrRef: refcount underflow", func() {
		req.DecrRef() // 0 -> -1, should panic without touching requestPool.
	})
}
