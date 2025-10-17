package raftstore

import (
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
)

// snapshotResendQueue retains the most recent snapshot per peer so that it can
// be resent to slow followers after connectivity resumes. The queue stores at
// most one snapshot because newer snapshots supersede older ones.
type snapshotResendQueue struct {
	mu   sync.Mutex
	snap myraft.Snapshot
	set  bool
}

func newSnapshotResendQueue() *snapshotResendQueue {
	return &snapshotResendQueue{}
}

func (q *snapshotResendQueue) record(snap myraft.Snapshot) {
	if myraft.IsEmptySnap(snap) {
		return
	}
	q.mu.Lock()
	q.snap = snap
	q.set = true
	q.mu.Unlock()
}

func (q *snapshotResendQueue) take() (myraft.Snapshot, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.set {
		return myraft.Snapshot{}, false
	}
	snap := q.snap
	q.snap = myraft.Snapshot{}
	q.set = false
	return snap, true
}

func (q *snapshotResendQueue) peek() (myraft.Snapshot, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.set {
		return myraft.Snapshot{}, false
	}
	return q.snap, true
}

func (q *snapshotResendQueue) clear() {
	q.mu.Lock()
	q.snap = myraft.Snapshot{}
	q.set = false
	q.mu.Unlock()
}
