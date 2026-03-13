package peer

import (
	"fmt"
	"sync"

	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/wal"
)

type raftLogTracker struct {
	mu      sync.Mutex
	wal     *wal.Manager
	groupID uint64

	lastPointer raftmeta.RaftLogPointer
	lastError   error
	injected    bool
}

func newRaftLogTracker(walMgr *wal.Manager, groupID uint64) *raftLogTracker {
	if walMgr == nil {
		return nil
	}
	return &raftLogTracker{wal: walMgr, groupID: groupID}
}

func (r *raftLogTracker) capturePointer(ptr raftmeta.RaftLogPointer) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastPointer = ptr
}

func (r *raftLogTracker) injectFailure(stage string) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.injected {
		return nil
	}
	r.lastError = fmt.Errorf("raftstore: injected failure at %s", stage)
	return r.lastError
}

func (r *raftLogTracker) setInjected(flag bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.injected = flag
	if !flag {
		r.lastError = nil
	}
	r.mu.Unlock()
}

func (r *raftLogTracker) Info() *RaftLogInfo {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return &RaftLogInfo{
		Pointer:  r.lastPointer,
		LastErr:  r.lastError,
		Injected: r.injected,
	}
}

type RaftLogInfo struct {
	Pointer  raftmeta.RaftLogPointer
	LastErr  error
	Injected bool
}
