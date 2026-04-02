package rootraft

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"testing"

	"github.com/stretchr/testify/require"
)

func testDescriptor(id uint64, start, end string) descriptor.Descriptor {
	d := descriptor.Descriptor{
		RegionID: id,
		StartKey: []byte(start),
		EndKey:   []byte(end),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		State:    metaregion.ReplicaStateRunning,
	}
	d.EnsureHash()
	return d
}

func TestStateMachineAppliesDescriptorLifecycle(t *testing.T) {
	sm := NewStateMachine(Checkpoint{})
	boot := testDescriptor(7, "a", "z")
	commit := sm.ApplyCommand(rootpkg.Cursor{Term: 1, Index: 1}, command{kind: commandKindEvent, event: rootpkg.RegionBootstrapped(boot)})
	require.Equal(t, uint64(1), commit.State.ClusterEpoch)

	splitLeft := testDescriptor(7, "a", "m")
	splitRight := testDescriptor(8, "m", "z")
	commit = sm.ApplyCommand(rootpkg.Cursor{Term: 1, Index: 2}, command{kind: commandKindEvent, event: rootpkg.RegionSplitCommitted(7, []byte("m"), splitLeft, splitRight)})
	require.Equal(t, uint64(2), commit.State.ClusterEpoch)

	snap := sm.Snapshot()
	require.Contains(t, snap.Descriptors, uint64(7))
	require.Contains(t, snap.Descriptors, uint64(8))
	require.Equal(t, []byte("m"), snap.Descriptors[8].StartKey)

	commit = sm.ApplyCommand(rootpkg.Cursor{Term: 1, Index: 3}, command{kind: commandKindEvent, event: rootpkg.RegionTombstoned(8)})
	require.Equal(t, uint64(3), commit.State.ClusterEpoch)
	snap = sm.Snapshot()
	_, ok := snap.Descriptors[8]
	require.False(t, ok)
}

func TestSingleNodeRootAppendAndFence(t *testing.T) {
	r, err := OpenSingleNode(Config{NodeID: 1})
	require.NoError(t, err)

	desc := testDescriptor(11, "", "")
	commit, err := r.Append(rootpkg.RegionDescriptorPublished(desc))
	require.NoError(t, err)
	require.Equal(t, uint64(1), commit.State.ClusterEpoch)

	events, tail, err := r.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, commit.Cursor, tail)

	fence, err := r.FenceAllocator(rootpkg.AllocatorKindID, 99)
	require.NoError(t, err)
	require.Equal(t, uint64(99), fence)
	fence, err = r.FenceAllocator(rootpkg.AllocatorKindID, 7)
	require.NoError(t, err)
	require.Equal(t, uint64(99), fence)
}
