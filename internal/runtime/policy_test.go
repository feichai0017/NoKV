package runtime

import (
	"math"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/hotring"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
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
	ring := hotring.NewRotatingHotRing(8, nil)
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

func TestWALGCPolicyRaftPointers(t *testing.T) {
	policy := &WALGCPolicy{
		raftPointers: func() map[uint64]localmeta.RaftLogPointer {
			return map[uint64]localmeta.RaftLogPointer{
				1: {GroupID: 1, Segment: 4, Offset: 128},
				2: {GroupID: 2, Segment: 2, Offset: 64},
			}
		},
	}

	require.True(t, policy.CanRemoveSegment(1), "segment 1 is older than every raft pointer")
	require.False(t, policy.CanRemoveSegment(2), "segment 2 is still referenced by group 2")
	require.False(t, policy.CanRemoveSegment(4), "segment 4 is still referenced by group 1")
}

func TestWALGCPolicyRaftSegmentIndex(t *testing.T) {
	policy := &WALGCPolicy{
		raftPointers: func() map[uint64]localmeta.RaftLogPointer {
			return map[uint64]localmeta.RaftLogPointer{
				1: {GroupID: 1, SegmentIndex: 5},
			}
		},
	}

	require.True(t, policy.CanRemoveSegment(4))
	require.False(t, policy.CanRemoveSegment(5))
	require.False(t, policy.CanRemoveSegment(6))
}

func TestWALGCPolicyWarnsForRaftRecords(t *testing.T) {
	warned := false
	policy := &WALGCPolicy{
		segmentMetrics: func(segmentID uint32) wal.RecordMetrics {
			require.Equal(t, uint32(9), segmentID)
			return wal.RecordMetrics{RaftEntries: 1, RaftStates: 2, RaftSnapshots: 3}
		},
		warn: func(_ string, _ ...any) {
			warned = true
		},
	}

	require.True(t, policy.CanRemoveSegment(9))
	require.True(t, warned)
}
