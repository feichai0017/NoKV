package NoKV

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestDecrRefUnderflowPanics(t *testing.T) {
	req := &request{}
	require.Panics(t, func() {
		req.DecrRef() // ref=0, should panic on underflow.
	})
}
