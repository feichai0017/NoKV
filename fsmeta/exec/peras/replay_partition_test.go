package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestSplitReplayPlanByFSMetaBucket(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftKey := fsmetaInodeKeyForBucket(t, mount, 1)
	rightKey := fsmetaInodeKeyForBucket(t, mount, 2)
	plan := ReplayPlan{
		EpochID: 1,
		Operations: []ReplayOperation{
			{OpID: opID("client", 1), Kind: fsmeta.OperationUpdateInode, Mutations: []ReplayMutation{{Key: leftKey, Value: []byte("a")}}},
			{OpID: opID("client", 2), Kind: fsmeta.OperationUpdateInode, Mutations: []ReplayMutation{{Key: rightKey, Value: []byte("b")}}},
			{OpID: opID("client", 3), Kind: fsmeta.OperationUpdateInode, Mutations: []ReplayMutation{{Key: leftKey, Value: []byte("c")}}},
		},
	}

	out, err := SplitReplayPlanByFSMetaBucket(plan)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, []OperationID{opID("client", 1), opID("client", 3)}, replayPlanOpIDs(out[0]))
	require.Equal(t, []OperationID{opID("client", 2)}, replayPlanOpIDs(out[1]))
}

func TestSplitReplayPlanByFSMetaBucketRejectsOneOperationAcrossBuckets(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftKey := fsmetaInodeKeyForBucket(t, mount, 1)
	rightKey := fsmetaInodeKeyForBucket(t, mount, 2)
	_, err := SplitReplayPlanByFSMetaBucket(ReplayPlan{
		EpochID: 1,
		Operations: []ReplayOperation{{
			OpID: opID("client", 1),
			Kind: fsmeta.OperationUpdateInode,
			Mutations: []ReplayMutation{
				{Key: leftKey, Value: []byte("a")},
				{Key: rightKey, Value: []byte("b")},
			},
		}},
	})
	require.ErrorIs(t, err, ErrInvalidPerasSegment)
}

func TestSplitReplayPlanByFSMetaBucketLeavesNonFSMetaTestPlansWhole(t *testing.T) {
	plan := ReplayPlan{
		EpochID: 1,
		Operations: []ReplayOperation{{
			OpID:      opID("client", 1),
			Kind:      fsmeta.OperationCreate,
			Mutations: []ReplayMutation{{Key: []byte("raw-key"), Value: []byte("value")}},
		}},
	}
	out, err := SplitReplayPlanByFSMetaBucket(plan)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, replayPlanOpIDs(plan), replayPlanOpIDs(out[0]))
}

func replayPlanOpIDs(plan ReplayPlan) []OperationID {
	out := make([]OperationID, 0, len(plan.Operations))
	for _, op := range plan.Operations {
		out = append(out, op.OpID)
	}
	return out
}

func fsmetaInodeKeyForBucket(t *testing.T, mount fsmeta.MountIdentity, bucket fsmeta.AffinityBucket) []byte {
	t.Helper()
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) != bucket {
			continue
		}
		key, err := fsmeta.EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		return key
	}
	t.Fatalf("no inode found for bucket %d", bucket)
	return nil
}
