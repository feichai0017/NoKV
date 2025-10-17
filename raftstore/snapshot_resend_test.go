package raftstore

import (
	"testing"

	myraft "github.com/feichai0017/NoKV/raft"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

func TestSnapshotResendQueueRecordAndTake(t *testing.T) {
	q := newSnapshotResendQueue()
	if _, ok := q.peek(); ok {
		t.Fatalf("expected queue to be empty initially")
	}

	snap := myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 12,
			Term:  3,
		},
		Data: []byte("snapshot-data"),
	}
	q.record(snap)
	if got, ok := q.peek(); !ok {
		t.Fatalf("expected snapshot to be present after record")
	} else if got.Metadata.Index != snap.Metadata.Index || got.Metadata.Term != snap.Metadata.Term {
		t.Fatalf("unexpected snapshot metadata: %+v", got.Metadata)
	}
	taken, ok := q.take()
	if !ok {
		t.Fatalf("expected to pop snapshot from queue")
	}
	if taken.Metadata.Index != snap.Metadata.Index || taken.Metadata.Term != snap.Metadata.Term {
		t.Fatalf("unexpected snapshot popped: %+v", taken.Metadata)
	}
	if _, ok := q.take(); ok {
		t.Fatalf("queue should be empty after take")
	}
}
