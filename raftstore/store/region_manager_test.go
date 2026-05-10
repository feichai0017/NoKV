package store

import (
	"testing"

	myraft "github.com/feichai0017/NoKV/raft"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"github.com/feichai0017/NoKV/raftstore/store/router"
)

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

	rt := router.New()
	if err := rt.SendRaft(1, myraft.Message{To: 1}); err == nil {
		t.Fatalf("expected SendRaft to fail for missing peer")
	}
	if err := rt.SendPropose(1, []byte("data")); err == nil {
		t.Fatalf("expected SendPropose to fail for missing peer")
	}
	if err := rt.SendCommand(1, nil); err == nil {
		t.Fatalf("expected SendCommand to fail for nil request")
	}
	if err := rt.SendCommand(1, &raftcmdpb.RaftCmdRequest{Header: &raftcmdpb.CmdHeader{RegionId: 1}}); err == nil {
		t.Fatalf("expected SendCommand to fail for missing peer")
	}
	if err := rt.SendTick(1); err == nil {
		t.Fatalf("expected SendTick to fail for missing peer")
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
