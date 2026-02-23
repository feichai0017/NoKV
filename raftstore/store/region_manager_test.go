package store

import (
	"testing"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

func TestRegionManagerLoadAndMeta(t *testing.T) {
	rm := newRegionManager(nil, RegionHooks{})
	snapshot := map[uint64]manifest.RegionMeta{
		1: {
			ID:       1,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}
	rm.loadSnapshot(snapshot)

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
	rm := newRegionManager(nil, RegionHooks{})
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
	rm := newRegionManager(nil, RegionHooks{})
	requireNoError(t, rm.updateRegion(manifest.RegionMeta{ID: 3}))
	rm.setPeer(3, &peer.Peer{})

	requireNoError(t, rm.removeRegion(3))
	if _, ok := rm.meta(3); ok {
		t.Fatalf("meta should be removed")
	}
	if p := rm.peer(3); p != nil {
		t.Fatalf("peer should be removed")
	}
}

func TestRegionManagerListMetas(t *testing.T) {
	rm := newRegionManager(nil, RegionHooks{})
	rm.loadSnapshot(map[uint64]manifest.RegionMeta{
		4: {ID: 4},
		5: {ID: 5},
	})
	metas := rm.listMetas()
	if len(metas) != 2 {
		t.Fatalf("expected two metas, got %d", len(metas))
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStoreAndRouterHelpers(t *testing.T) {
	store := NewStore(nil)
	if store.Router() == nil {
		t.Fatalf("expected router to be non-nil")
	}
	factory := func(cfg *peer.Config) (*peer.Peer, error) {
		return nil, nil
	}
	store.SetPeerFactory(factory)
	if store.peerFactory == nil {
		t.Fatalf("expected peer factory to be set")
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
	if err := router.SendCommand(1, &pb.RaftCmdRequest{Header: &pb.CmdHeader{RegionId: 1}}); err == nil {
		t.Fatalf("expected SendCommand to fail for missing peer")
	}
	if err := router.SendTick(1); err == nil {
		t.Fatalf("expected SendTick to fail for missing peer")
	}
}

func TestStoreReadCommandValidation(t *testing.T) {
	store := NewStore(nil)

	if _, err := store.ReadCommand(&pb.RaftCmdRequest{}); err == nil {
		t.Fatalf("expected missing region id error")
	}

	req := &pb.RaftCmdRequest{Header: &pb.CmdHeader{RegionId: 1}}
	resp, err := store.ReadCommand(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || resp.RegionError == nil || resp.RegionError.EpochNotMatch == nil {
		t.Fatalf("expected epoch not match error")
	}
}

func TestStoreStepErrors(t *testing.T) {
	var nilStore *Store
	if err := nilStore.Step(myraft.Message{To: 1}); err == nil {
		t.Fatalf("expected error for nil store")
	}

	store := NewStore(nil)
	if err := store.Step(myraft.Message{}); err == nil {
		t.Fatalf("expected error for missing recipient")
	}
}

func TestReadOnlyRequestPredicate(t *testing.T) {
	if isReadOnlyRequest(nil) {
		t.Fatalf("expected nil request to be read-only false")
	}
	readReq := &pb.RaftCmdRequest{Requests: []*pb.Request{
		{CmdType: pb.CmdType_CMD_GET},
		{CmdType: pb.CmdType_CMD_SCAN},
	}}
	if !isReadOnlyRequest(readReq) {
		t.Fatalf("expected read-only request to return true")
	}
	writeReq := &pb.RaftCmdRequest{Requests: []*pb.Request{
		{CmdType: pb.CmdType_CMD_PREWRITE},
	}}
	if isReadOnlyRequest(writeReq) {
		t.Fatalf("expected write request to return false")
	}
}
