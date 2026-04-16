package runtime

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestDecrRefUnderflowPanics(t *testing.T) {
	req := &Request{}
	require.Panics(t, func() {
		req.DecrRef()
	})
}
