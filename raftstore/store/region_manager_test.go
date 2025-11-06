package store

import (
	"testing"

	"github.com/feichai0017/NoKV/manifest"
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
