package store

import (
	"context"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"testing"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

func TestRegionManagerLoadAndMeta(t *testing.T) {
	rm := newRegionManager(nil, nil, nil)
	snapshot := map[uint64]localmeta.RegionMeta{
		1: {
			ID:       1,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}
	rm.loadBootstrapSnapshot(snapshot)

	meta, ok := rm.meta(1)
	if !ok {
		t.Fatalf("expected region meta present")
	}
	meta.StartKey[0] = 'x'
	meta2, _ := rm.meta(1)
	if meta2.StartKey[0] != 'a' {
		t.Fatalf("expected deep copy of meta")
	}
}

func TestRegionManagerPeerTracking(t *testing.T) {
	rm := newRegionManager(nil, nil, nil)
	fakePeer := &peer.Peer{}
	rm.setPeer(2, fakePeer)
	if got := rm.peer(2); got != fakePeer {
		t.Fatalf("expected peer to be retrievable")
	}

	rm.setPeer(2, nil)
	if p := rm.peer(2); p != nil {
		t.Fatalf("expected peer cleared")
	}
}

func TestRegionManagerRemove(t *testing.T) {
	rm := newRegionManager(nil, nil, nil)
	requireNoError(t, rm.applyRegionMeta(localmeta.RegionMeta{ID: 3}, true))
	rm.setPeer(3, &peer.Peer{})

	requireNoError(t, rm.applyRegionRemoval(3, true))
	if _, ok := rm.meta(3); ok {
		t.Fatalf("meta should be removed")
	}
	if p := rm.peer(3); p != nil {
		t.Fatalf("peer should be removed")
	}
}

func TestRegionManagerListMetas(t *testing.T) {
	rm := newRegionManager(nil, nil, nil)
	rm.loadBootstrapSnapshot(map[uint64]localmeta.RegionMeta{
		4: {ID: 4},
		5: {ID: 5},
	})
	metas := rm.listMetas()
	if len(metas) != 2 {
		t.Fatalf("expected two metas, got %d", len(metas))
	}
}

func TestRegionManagerPublishesCatalogRootEvents(t *testing.T) {
	var events []regionEvent
	rm := newRegionManager(nil, nil, func(ev regionEvent) {
		events = append(events, ev)
	})

	requireNoError(t, rm.applyRegionMeta(localmeta.RegionMeta{
		ID:       9,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
	}, true))
	if len(events) != 1 || events[0].root.Kind != rootevent.KindRegionBootstrap {
		t.Fatalf("expected bootstrap root event, got %+v", events)
	}

	events = nil
	requireNoError(t, rm.applyRegionState(9, metaregion.ReplicaStateRemoving))
	if len(events) != 1 || events[0].root.Kind != rootevent.KindRegionDescriptorPublished {
		t.Fatalf("expected descriptor publish root event, got %+v", events)
	}

	events = nil
	requireNoError(t, rm.applyRegionRemoval(9, true))
	if len(events) != 2 ||
		events[0].root.Kind != rootevent.KindRegionDescriptorPublished ||
		events[1].root.Kind != rootevent.KindRegionTombstoned {
		t.Fatalf("expected tombstone root event, got %+v", events)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStoreAndRouterHelpers(t *testing.T) {
	store := NewStore(Config{})
	if store.Router() == nil {
		t.Fatalf("expected router to be non-nil")
	}

	snap := store.RegionSnapshot()
	if len(snap.Regions) != 0 {
		t.Fatalf("expected empty region snapshot")
	}
	if store.RegionMetrics() == nil {
		t.Fatalf("expected region metrics to be initialized")
	}

	router := NewRouter()
	if err := router.SendRaft(1, myraft.Message{To: 1}); err == nil {
		t.Fatalf("expected SendRaft to fail for missing peer")
	}
	if err := router.SendPropose(1, []byte("data")); err == nil {
		t.Fatalf("expected SendPropose to fail for missing peer")
	}
	if err := router.SendCommand(1, nil); err == nil {
		t.Fatalf("expected SendCommand to fail for nil request")
	}
	if err := router.SendCommand(1, &raftcmdpb.RaftCmdRequest{Header: &raftcmdpb.CmdHeader{RegionId: 1}}); err == nil {
		t.Fatalf("expected SendCommand to fail for missing peer")
	}
	if err := router.SendTick(1); err == nil {
		t.Fatalf("expected SendTick to fail for missing peer")
	}
}

func TestStoreReadCommandValidation(t *testing.T) {
	store := NewStore(Config{})

	if _, err := store.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{}); err == nil {
		t.Fatalf("expected missing region id error")
	}

	req := &raftcmdpb.RaftCmdRequest{Header: &raftcmdpb.CmdHeader{RegionId: 1}}
	resp, err := store.ReadCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || resp.RegionError == nil || resp.RegionError.RegionNotFound == nil {
		t.Fatalf("expected region not found error")
	}
}

func TestStoreReadCommandStoreNotMatch(t *testing.T) {
	store := NewStore(Config{StoreID: 7})

	req := &raftcmdpb.RaftCmdRequest{Header: &raftcmdpb.CmdHeader{RegionId: 1, StoreId: 9}}
	resp, err := store.ReadCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || resp.RegionError == nil || resp.RegionError.StoreNotMatch == nil {
		t.Fatalf("expected store not match error")
	}
	if resp.RegionError.StoreNotMatch.RequestStoreId != 9 || resp.RegionError.StoreNotMatch.ActualStoreId != 7 {
		t.Fatalf("unexpected store mismatch payload: %+v", resp.RegionError.StoreNotMatch)
	}
}

func TestStoreStepErrors(t *testing.T) {
	var nilStore *Store
	if err := nilStore.Step(myraft.Message{To: 1}); err == nil {
		t.Fatalf("expected error for nil store")
	}

	store := NewStore(Config{})
	if err := store.Step(myraft.Message{}); err == nil {
		t.Fatalf("expected error for missing recipient")
	}
}

func TestReadOnlyRequestPredicate(t *testing.T) {
	if isReadOnlyRequest(nil) {
		t.Fatalf("expected nil request to be read-only false")
	}
	readReq := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		{CmdType: raftcmdpb.CmdType_CMD_GET},
		{CmdType: raftcmdpb.CmdType_CMD_SCAN},
	}}
	if !isReadOnlyRequest(readReq) {
		t.Fatalf("expected read-only request to return true")
	}
	writeReq := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		{CmdType: raftcmdpb.CmdType_CMD_PREWRITE},
	}}
	if isReadOnlyRequest(writeReq) {
		t.Fatalf("expected write request to return false")
	}
}
