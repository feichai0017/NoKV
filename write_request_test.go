package NoKV

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestDecrRefUnderflowPanics(t *testing.T) {
	req := &request{}
	req.IncrRef()
	req.DecrRef() // 1 -> 0, normal release

	require.Panics(t, func() {
		req.DecrRef() // 0 -> -1, should panic
	})
}
