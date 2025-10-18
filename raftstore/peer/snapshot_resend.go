package peer

import (
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
)

// snapshotResendQueue retains the most recent snapshot per peer so that it can
// be resent to slow followers after connectivity resumes.
type snapshotResendQueue struct {
	mu       sync.Mutex
	pendings map[uint64]myraft.Message
}

func newSnapshotResendQueue() *snapshotResendQueue {
	return &snapshotResendQueue{pendings: make(map[uint64]myraft.Message)}
}

func (q *snapshotResendQueue) record(msg myraft.Message) {
	if q == nil || msg.Type != myraft.MsgSnapshot || msg.To == 0 || myraft.IsEmptySnap(msg.Snapshot) {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pendings == nil {
		q.pendings = make(map[uint64]myraft.Message)
	}
	cpy := msg
	q.pendings[msg.To] = cpy
}

func (q *snapshotResendQueue) pendingFor(to uint64) (myraft.Message, bool) {
	if q == nil || to == 0 {
		return myraft.Message{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	msg, ok := q.pendings[to]
	return msg, ok
}

func (q *snapshotResendQueue) drop(to uint64) {
	if q == nil || to == 0 {
		return
	}
	q.mu.Lock()
	delete(q.pendings, to)
	q.mu.Unlock()
}

func (q *snapshotResendQueue) forEach(fn func(myraft.Message)) {
	if q == nil || fn == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, msg := range q.pendings {
		fn(msg)
	}
}

func (q *snapshotResendQueue) first() (myraft.Message, bool) {
	if q == nil {
		return myraft.Message{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, msg := range q.pendings {
		return msg, true
	}
	return myraft.Message{}, false
}

func (q *snapshotResendQueue) clearAll() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.pendings = make(map[uint64]myraft.Message)
	q.mu.Unlock()
}
