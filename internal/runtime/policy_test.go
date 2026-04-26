package runtime

import (
	"math"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/thermos"
	"github.com/stretchr/testify/require"
)

func TestCFHotKey(t *testing.T) {
	key := []byte("hot-key")
	require.Equal(t, string(key), CFHotKey(kv.CFDefault, key))
	require.Equal(t, string(key), CFHotKey(kv.ColumnFamily(0), key))

	encoded := CFHotKey(kv.CFLock, key)
	require.Len(t, encoded, len(key)+1)
	require.Equal(t, byte(kv.CFLock), encoded[0])
	require.Equal(t, string(key), encoded[1:])
}

func TestShouldThrottleHotWrite(t *testing.T) {
	ring := thermos.NewRotatingThermos(8, nil)
	key := []byte("hot")

	require.True(t, ShouldThrottleHotWrite(ring, 1, kv.CFDefault, key))
	require.False(t, ShouldThrottleHotWrite(nil, 1, kv.CFDefault, key))
	require.False(t, ShouldThrottleHotWrite(ring, 0, kv.CFDefault, key))
}

func TestNormalizeWriteThrottleState(t *testing.T) {
	require.Equal(t, lsm.WriteThrottleNone, NormalizeWriteThrottleState(lsm.WriteThrottleNone))
	require.Equal(t, lsm.WriteThrottleSlowdown, NormalizeWriteThrottleState(lsm.WriteThrottleSlowdown))
	require.Equal(t, lsm.WriteThrottleStop, NormalizeWriteThrottleState(lsm.WriteThrottleStop))
	require.Equal(t, lsm.WriteThrottleNone, NormalizeWriteThrottleState(lsm.WriteThrottleState(99)))
}

func TestSlowdownDelay(t *testing.T) {
	require.Zero(t, SlowdownDelay(0, 1))
	require.Zero(t, SlowdownDelay(128, 0))
	require.Equal(t, time.Second, SlowdownDelay(1024, 1024))
	require.Equal(t, time.Duration(math.MaxInt64), SlowdownDelay(math.MaxInt64, 1))
}
