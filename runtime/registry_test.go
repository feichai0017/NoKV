package runtime

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testModule struct {
	closed *int
}

func (m *testModule) Close() {
	if m == nil || m.closed == nil {
		return
	}
	*m.closed = *m.closed + 1
}

func TestRegistryRegisterCountAndClear(t *testing.T) {
	var r Registry
	require.Equal(t, 0, r.Count())
	require.True(t, r.Cleared())

	count1 := 0
	count2 := 0
	r.Register(&testModule{closed: &count1})
	r.Register(&testModule{closed: &count2})

	require.Equal(t, 2, r.Count())
	require.False(t, r.Cleared())

	r.CloseAll()

	require.Equal(t, 1, count1)
	require.Equal(t, 1, count2)
	require.Equal(t, 0, r.Count())
	require.True(t, r.Cleared())
}

func TestRegistryUnregister(t *testing.T) {
	var r Registry
	count := 0
	module := &testModule{closed: &count}

	r.Register(module)
	require.Equal(t, 1, r.Count())

	r.Unregister(module)
	require.Equal(t, 0, r.Count())

	r.CloseAll()
	require.Equal(t, 0, count)
}
