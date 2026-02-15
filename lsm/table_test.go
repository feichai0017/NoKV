package lsm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableDecrRefUnderflow(t *testing.T) {
	tbl := &table{
		fid: 1,
		ref: 0,
	}

	require.Panics(t, func() {
		_ = tbl.DecrRef()
	}, "Should panic when decr ref on zero count")
}
