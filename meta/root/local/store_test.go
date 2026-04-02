package local

import (
	"os"
	"path/filepath"
	"testing"

	rootpkg "github.com/feichai0017/NoKV/meta/root"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestStoreAppendReadAndReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)

	state, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, rootpkg.State{}, state)

	commit, err := store.Append(
		rootpkg.Event{Kind: rootpkg.EventKindStoreJoined, StoreMembership: &rootpkg.StoreMembership{StoreID: 1, Address: "s1"}},
		rootpkg.Event{Kind: rootpkg.EventKindRegionSplitCommitted, RangeSplit: &rootpkg.RangeSplit{ParentRegionID: 10, LeftRegionID: 11, RightRegionID: 12, SplitKey: []byte("m")}},
		rootpkg.Event{Kind: rootpkg.EventKindPlacementPolicyChanged, PlacementPolicy: &rootpkg.PlacementPolicy{Name: "default", Version: 7}},
	)
	require.NoError(t, err)
	require.Equal(t, rootpkg.Cursor{Term: 1, Index: 3}, commit.Cursor)
	require.Equal(t, uint64(1), commit.State.MembershipEpoch)
	require.Equal(t, uint64(1), commit.State.ClusterEpoch)
	require.Equal(t, uint64(7), commit.State.PolicyVersion)

	events, tail, err := store.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, commit.Cursor, tail)
	require.Equal(t, rootpkg.EventKindStoreJoined, events[0].Kind)
	require.Equal(t, uint64(1), events[0].StoreMembership.StoreID)
	require.Equal(t, []byte("m"), events[1].RangeSplit.SplitKey)
	require.Equal(t, uint64(7), events[2].PlacementPolicy.Version)

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	state, err = reopened.Current()
	require.NoError(t, err)
	require.Equal(t, commit.State, state)
	events, tail, err = reopened.ReadSince(rootpkg.Cursor{Term: 1, Index: 1})
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
	require.Equal(t, rootpkg.Cursor{}, state.LastCommitted)
}

func TestStoreIgnoresTruncatedLogTail(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)
	_, err = store.Append(rootpkg.Event{Kind: rootpkg.EventKindStoreJoined, StoreMembership: &rootpkg.StoreMembership{StoreID: 1, Address: "s1"}})
	require.NoError(t, err)

	f, err := os.OpenFile(filepath.Join(dir, LogFileName), os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{1, 2, 3, 4, 5})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	events, tail, err := reopened.ReadSince(rootpkg.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, rootpkg.Cursor{Term: 1, Index: 1}, tail)
}

func TestStoreReplaysLogAfterStaleCheckpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, nil)
	require.NoError(t, err)
	commit, err := store.Append(rootpkg.Event{Kind: rootpkg.EventKindPeerAdded, PeerChange: &rootpkg.PeerChange{RegionID: 1, StoreID: 2, PeerID: 3}})
	require.NoError(t, err)
	require.Equal(t, rootpkg.Cursor{Term: 1, Index: 1}, commit.Cursor)

	payload, err := proto.Marshal(&metapb.RootState{})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, CheckpointFileName), payload, 0o644))

	reopened, err := Open(dir, nil)
	require.NoError(t, err)
	state, err := reopened.Current()
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.ClusterEpoch)
	require.Equal(t, rootpkg.Cursor{Term: 1, Index: 1}, state.LastCommitted)
}
