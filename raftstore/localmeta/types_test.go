package localmeta

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/stretchr/testify/require"
)

func TestLocalMetaCloneHelpers(t *testing.T) {
	meta := &RegionMeta{
		ID:       7,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 2},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 11}},
		State:    RegionStateRunning,
	}
	metaClone := CloneRegionMetaPtr(meta)
	require.NotNil(t, metaClone)
	require.Equal(t, *meta, *metaClone)
	metaClone.StartKey[0] = 'b'
	metaClone.Peers[0].PeerID = 22
	require.Equal(t, []byte("a"), meta.StartKey)
	require.Equal(t, uint64(11), meta.Peers[0].PeerID)
	require.Nil(t, CloneRegionMetaPtr(nil))

	ptrs := map[uint64]RaftLogPointer{
		1: {GroupID: 1, Segment: 2},
	}
	ptrClone := CloneRaftPointers(ptrs)
	require.Equal(t, ptrs, ptrClone)
	ptrClone[1] = RaftLogPointer{}
	require.Equal(t, uint32(2), ptrs[1].Segment)
	require.Nil(t, CloneRaftPointers(nil))

	event := PendingRootEvent{
		Sequence: 3,
		Event:    rootevent.RegionTombstoned(3),
	}
	eventClone := ClonePendingRootEvents(map[uint64]PendingRootEvent{event.Sequence: event})
	require.Equal(t, event.Event.Kind, eventClone[event.Sequence].Event.Kind)
	eventClone[event.Sequence] = PendingRootEvent{Sequence: event.Sequence, Event: rootevent.StoreJoined(8)}
	require.Equal(t, rootevent.KindRegionTombstoned, event.Event.Kind)
	require.Nil(t, ClonePendingRootEvents(nil))

	op := PendingSchedulerOperation{
		Kind:         PendingSchedulerOperationLeaderTransfer,
		RegionID:     9,
		SourcePeerID: 101,
		TargetPeerID: 202,
	}
	opsClone := ClonePendingSchedulerOperations(map[string]PendingSchedulerOperation{
		PendingSchedulerOperationKey(op): op,
	})
	require.Equal(t, op, opsClone[PendingSchedulerOperationKey(op)])
	require.Equal(t, "leader-transfer:9", PendingSchedulerOperationKey(op))
	require.Equal(t, "unknown:1", pendingSchedulerOperationKey(PendingSchedulerOperationUnknown, 1))
	require.Equal(t, "unknown", PendingSchedulerOperationUnknown.String())
	require.Nil(t, ClonePendingSchedulerOperations(nil))

	blocked := BlockedRootEvent{
		Sequence:     4,
		Event:        rootevent.RegionTombstoned(4),
		TransitionID: "tid",
		LastError:    "boom",
	}
	blockedClone := CloneBlockedRootEvents(map[uint64]BlockedRootEvent{blocked.Sequence: blocked})
	require.Equal(t, blocked.TransitionID, blockedClone[blocked.Sequence].TransitionID)
	blockedClone[blocked.Sequence] = BlockedRootEvent{Sequence: blocked.Sequence, TransitionID: "changed"}
	require.Equal(t, "tid", blocked.TransitionID)
	require.Nil(t, CloneBlockedRootEvents(nil))
}

func TestPendingSchedulerOperationKindFromProto(t *testing.T) {
	require.Equal(t, PendingSchedulerOperationLeaderTransfer, pendingSchedulerOperationKindFromPB(uint32(PendingSchedulerOperationLeaderTransfer)))
	require.Equal(t, PendingSchedulerOperationUnknown, pendingSchedulerOperationKindFromPB(99))
}
