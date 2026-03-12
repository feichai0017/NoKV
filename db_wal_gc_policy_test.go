package NoKV

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/wal"
)

func TestDBWALGCPolicyRaftPointers(t *testing.T) {
	policy := &dbWALGCPolicy{
		raftPointers: func() map[uint64]manifest.RaftLogPointer {
			return map[uint64]manifest.RaftLogPointer{
				1: {GroupID: 1, Segment: 4, Offset: 128},
				2: {GroupID: 2, Segment: 2, Offset: 64},
			}
		},
	}

	require.True(t, policy.CanRemoveSegment(1), "segment 1 is older than every raft pointer")
	require.False(t, policy.CanRemoveSegment(2), "segment 2 is still referenced by group 2")
	require.False(t, policy.CanRemoveSegment(4), "segment 4 is still referenced by group 1")
}

func TestDBWALGCPolicyRaftSegmentIndex(t *testing.T) {
	policy := &dbWALGCPolicy{
		raftPointers: func() map[uint64]manifest.RaftLogPointer {
			return map[uint64]manifest.RaftLogPointer{
				1: {GroupID: 1, SegmentIndex: 5},
			}
		},
	}

	require.True(t, policy.CanRemoveSegment(4))
	require.False(t, policy.CanRemoveSegment(5))
	require.False(t, policy.CanRemoveSegment(6))
}

func TestDBWALGCPolicyWarnsForRaftRecords(t *testing.T) {
	warned := false
	policy := &dbWALGCPolicy{
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
