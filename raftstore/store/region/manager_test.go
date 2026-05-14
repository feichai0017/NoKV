// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package region

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestManagerLoadAndMeta(t *testing.T) {
	m := NewManager(nil, nil, nil)
	snapshot := map[uint64]localmeta.RegionMeta{
		1: {
			ID:       1,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}
	m.LoadBootstrap(snapshot)

	meta, ok := m.Meta(1)
	if !ok {
		t.Fatalf("expected region meta present")
	}
	meta.StartKey[0] = 'x'
	meta2, _ := m.Meta(1)
	if meta2.StartKey[0] != 'a' {
		t.Fatalf("expected deep copy of meta")
	}
}

func TestManagerPeerTracking(t *testing.T) {
	m := NewManager(nil, nil, nil)
	fakePeer := &peer.Peer{}
	m.SetPeer(2, fakePeer)
	if got := m.Peer(2); got != fakePeer {
		t.Fatalf("expected peer to be retrievable")
	}

	m.SetPeer(2, nil)
	if p := m.Peer(2); p != nil {
		t.Fatalf("expected peer cleared")
	}
}

func TestManagerRemove(t *testing.T) {
	m := NewManager(nil, nil, nil)
	requireNoError(t, m.Apply(localmeta.RegionMeta{ID: 3}, true))
	m.SetPeer(3, &peer.Peer{})

	requireNoError(t, m.Remove(3, true))
	if _, ok := m.Meta(3); ok {
		t.Fatalf("meta should be removed")
	}
	if p := m.Peer(3); p != nil {
		t.Fatalf("peer should be removed")
	}
}

func TestManagerListMetas(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.LoadBootstrap(map[uint64]localmeta.RegionMeta{
		4: {ID: 4},
		5: {ID: 5},
	})
	metas := m.Metas()
	if len(metas) != 2 {
		t.Fatalf("expected two metas, got %d", len(metas))
	}
}

func TestManagerPublishesCatalogRootEvents(t *testing.T) {
	var events []rootevent.Event
	m := NewManager(nil, nil, func(ev rootevent.Event) {
		events = append(events, ev)
	})

	requireNoError(t, m.Apply(localmeta.RegionMeta{
		ID:       9,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
	}, true))
	if len(events) != 1 || events[0].Kind != rootevent.KindRegionBootstrap {
		t.Fatalf("expected bootstrap root event, got %+v", events)
	}

	events = nil
	requireNoError(t, m.ApplyState(9, metaregion.ReplicaStateRemoving))
	if len(events) != 1 || events[0].Kind != rootevent.KindRegionDescriptorPublished {
		t.Fatalf("expected descriptor publish root event, got %+v", events)
	}

	events = nil
	requireNoError(t, m.Remove(9, true))
	if len(events) != 2 ||
		events[0].Kind != rootevent.KindRegionDescriptorPublished ||
		events[1].Kind != rootevent.KindRegionTombstoned {
		t.Fatalf("expected tombstone root event, got %+v", events)
	}
}
