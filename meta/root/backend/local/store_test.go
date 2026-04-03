package local

import (
	"os"
	"path/filepath"
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootfile "github.com/feichai0017/NoKV/meta/root/storage/file"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestStoreAppendReadAndReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)

	state, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, rootstate.State{}, state)

	commit, err := store.Append(
		rootevent.StoreJoined(1, "s1"),
		rootevent.RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("z"))),
		rootevent.RegionSplitCommitted(10, []byte("m"), testDescriptor(11, []byte("a"), []byte("m")), testDescriptor(12, []byte("m"), []byte("z"))),
	)
	require.NoError(t, err)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 3}, commit.Cursor)
	require.Equal(t, uint64(1), commit.State.MembershipEpoch)
	require.Equal(t, uint64(2), commit.State.ClusterEpoch)

	events, tail, err := store.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, commit.Cursor, tail)
	require.Equal(t, rootevent.KindStoreJoined, events[0].Kind)
	require.Equal(t, uint64(1), events[0].StoreMembership.StoreID)
	require.Equal(t, rootevent.KindRegionDescriptorPublished, events[1].Kind)
	require.Equal(t, uint64(10), events[1].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, []byte("m"), events[2].RangeSplit.SplitKey)
	require.Equal(t, uint64(11), events[2].RangeSplit.Left.RegionID)
	require.Equal(t, uint64(12), events[2].RangeSplit.Right.RegionID)

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	state, err = reopened.Current()
	require.NoError(t, err)
	require.Equal(t, commit.State, state)
	events, tail, err = reopened.ReadSince(rootstate.Cursor{Term: 1, Index: 1})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, commit.Cursor, tail)
}

func TestStoreFenceAllocatorPersistsWithoutEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)

	fence, err := store.FenceAllocator(rootpkg.AllocatorKindID, 10)
	require.NoError(t, err)
	require.Equal(t, uint64(10), fence)
	fence, err = store.FenceAllocator(rootpkg.AllocatorKindID, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(10), fence)
	fence, err = store.FenceAllocator(rootpkg.AllocatorKindTSO, 22)
	require.NoError(t, err)
	require.Equal(t, uint64(22), fence)

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	state, err := reopened.Current()
	require.NoError(t, err)
	require.Equal(t, uint64(10), state.IDFence)
	require.Equal(t, uint64(22), state.TSOFence)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, state.LastCommitted)
}

func TestStoreIgnoresTruncatedLogTail(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)
	_, err = store.Append(rootevent.StoreJoined(1, "s1"))
	require.NoError(t, err)

	f, err := os.OpenFile(filepath.Join(dir, rootfile.LogFileName), os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{1, 2, 3, 4, 5})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	events, tail, err := reopened.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 0)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, tail)
}

func TestStoreReplaysLogAfterStaleCheckpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)
	commit, err := store.Append(rootevent.PeerAdded(1, 2, 3, testDescriptor(1, []byte("a"), []byte("z"))))
	require.NoError(t, err)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, commit.Cursor)

	payload, err := proto.Marshal(&metapb.RootState{})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, rootfile.CheckpointFileName), payload, 0o644))

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	state, err := reopened.Current()
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.ClusterEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, state.LastCommitted)
}

func TestStoreLoadsLegacyRootStateCheckpoint(t *testing.T) {
	dir := t.TempDir()
	payload, err := proto.Marshal(&metapb.RootState{
		ClusterEpoch:    7,
		MembershipEpoch: 3,
		LastCommitted:   &metapb.RootCursor{Term: 1, Index: 4},
		IdFence:         11,
		TsoFence:        22,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, rootfile.CheckpointFileName), payload, 0o644))

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	state, err := reopened.Current()
	require.NoError(t, err)
	require.Equal(t, uint64(7), state.ClusterEpoch)
	require.Equal(t, uint64(3), state.MembershipEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 4}, state.LastCommitted)
	require.Equal(t, uint64(11), state.IDFence)
	require.Equal(t, uint64(22), state.TSOFence)
}

func TestStoreCompactsPhysicalLogAndKeepsRecentTail(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)

	total := maxRetainedRecords + 8
	for i := 0; i < total; i++ {
		_, err := store.Append(rootevent.RegionDescriptorPublished(testDescriptor(uint64(100+i), []byte{byte('a' + i%26)}, []byte{byte('b' + i%26)})))
		require.NoError(t, err)
	}

	reopened, err := Open(dir, nil)
	require.NoError(t, err)

	tailCursor := rootstate.Cursor{Term: 1, Index: uint64(total - maxRetainedRecords)}
	events, tail, err := reopened.ReadSince(tailCursor)
	require.NoError(t, err)
	require.Len(t, events, maxRetainedRecords)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: uint64(total)}, tail)

	events, tail, err = reopened.ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, total)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: uint64(total)}, tail)
}

func testDescriptor(regionID uint64, start, end []byte) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID:  regionID,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:     []metaregion.Peer{{StoreID: 1, PeerID: regionID*10 + 1}, {StoreID: 2, PeerID: regionID*10 + 2}},
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}
