package store

import (
	"fmt"
	"sync"
	"sync/atomic"

	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"

	"github.com/feichai0017/NoKV/engine/kv"
	myraft "github.com/feichai0017/NoKV/raft"
)

const defaultApplyObserverBuffer = 256

// ApplyEventSource identifies which committed raft command made keys visible.
type ApplyEventSource uint8

const (
	ApplyEventSourceCommit ApplyEventSource = iota + 1
	ApplyEventSourceResolveLock
)

// ApplyEvent is emitted after a raft command is successfully applied and makes
// one or more MVCC keys visible. Observers must treat the event as immutable.
type ApplyEvent struct {
	RegionID      uint64
	Term          uint64
	Index         uint64
	Source        ApplyEventSource
	CommitVersion uint64
	Keys          [][]byte
}

// ApplyObserver consumes post-apply events. Store dispatch to observers is
// non-blocking; a slow observer drops events instead of blocking raft apply.
type ApplyObserver interface {
	OnApply(ApplyEvent)
}

// ApplyObserverRegistration unregisters a store apply observer.
type ApplyObserverRegistration interface {
	Close()
}

type applyObserverRuntime struct {
	mu        sync.RWMutex
	next      uint64
	observers map[uint64]*registeredApplyObserver
	dropped   atomic.Uint64
	closed    bool
}

type registeredApplyObserver struct {
	id       uint64
	runtime  *applyObserverRuntime
	observer ApplyObserver
	ch       chan ApplyEvent
	once     sync.Once
}

func newApplyObserverRuntime() *applyObserverRuntime {
	return &applyObserverRuntime{observers: make(map[uint64]*registeredApplyObserver)}
}

func (r *applyObserverRuntime) register(observer ApplyObserver, capacity int) (ApplyObserverRegistration, error) {
	if observer == nil {
		return nil, fmt.Errorf("raftstore/store: nil apply observer")
	}
	if capacity <= 0 {
		capacity = defaultApplyObserverBuffer
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("raftstore/store: apply observer runtime closed")
	}
	r.next++
	reg := &registeredApplyObserver{
		id:       r.next,
		runtime:  r,
		observer: observer,
		ch:       make(chan ApplyEvent, capacity),
	}
	r.observers[reg.id] = reg
	go reg.run()
	return reg, nil
}

func (r *applyObserverRuntime) emit(evt ApplyEvent) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}
	for _, observer := range r.observers {
		select {
		case observer.ch <- evt:
		default:
			r.dropped.Add(1)
		}
	}
}

func (r *applyObserverRuntime) droppedCount() uint64 {
	if r == nil {
		return 0
	}
	return r.dropped.Load()
}

func (r *applyObserverRuntime) close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	for id, observer := range r.observers {
		delete(r.observers, id)
		close(observer.ch)
	}
}

func (r *registeredApplyObserver) run() {
	for evt := range r.ch {
		r.observer.OnApply(evt)
	}
}

func (r *registeredApplyObserver) Close() {
	if r == nil || r.runtime == nil {
		return
	}
	r.once.Do(func() {
		r.runtime.mu.Lock()
		defer r.runtime.mu.Unlock()
		if current := r.runtime.observers[r.id]; current == r {
			delete(r.runtime.observers, r.id)
			close(r.ch)
		}
	})
}

// RegisterApplyObserver registers one store-level post-apply observer.
func (s *Store) RegisterApplyObserver(observer ApplyObserver, capacity int) (ApplyObserverRegistration, error) {
	if s == nil {
		return nil, errNilStore
	}
	if s.observers == nil {
		s.observers = newApplyObserverRuntime()
	}
	return s.observers.register(observer, capacity)
}

// DroppedApplyObserverEvents returns how many observer events were dropped
// because registered observers could not keep up with raft apply.
func (s *Store) DroppedApplyObserverEvents() uint64 {
	if s == nil {
		return 0
	}
	return s.observers.droppedCount()
}

func (s *Store) emitApplyEvents(entry myraft.Entry, req *raftcmdpb.RaftCmdRequest, resp *raftcmdpb.RaftCmdResponse) {
	if s == nil || s.observers == nil {
		return
	}
	for _, evt := range applyEventsFromCommand(entry, req, resp) {
		s.observers.emit(evt)
	}
}

func applyEventsFromCommand(entry myraft.Entry, req *raftcmdpb.RaftCmdRequest, resp *raftcmdpb.RaftCmdResponse) []ApplyEvent {
	if req == nil || resp == nil {
		return nil
	}
	regionID := req.GetHeader().GetRegionId()
	responses := resp.GetResponses()
	var out []ApplyEvent
	for i, request := range req.GetRequests() {
		if request == nil {
			continue
		}
		var response *raftcmdpb.Response
		if i < len(responses) {
			response = responses[i]
		}
		switch request.GetCmdType() {
		case raftcmdpb.CmdType_CMD_COMMIT:
			commit := request.GetCommit()
			if commit == nil || len(commit.GetKeys()) == 0 || response == nil || response.GetCommit() == nil {
				continue
			}
			if response.GetCommit().GetError() != nil {
				continue
			}
			out = append(out, ApplyEvent{
				RegionID:      regionID,
				Term:          entry.Term,
				Index:         entry.Index,
				Source:        ApplyEventSourceCommit,
				CommitVersion: commit.GetCommitVersion(),
				Keys:          cloneApplyKeys(commit.GetKeys()),
			})
		case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
			resolve := request.GetResolveLock()
			if resolve == nil || resolve.GetCommitVersion() == 0 || len(resolve.GetKeys()) == 0 || response == nil || response.GetResolveLock() == nil {
				continue
			}
			if response.GetResolveLock().GetError() != nil {
				continue
			}
			out = append(out, ApplyEvent{
				RegionID:      regionID,
				Term:          entry.Term,
				Index:         entry.Index,
				Source:        ApplyEventSourceResolveLock,
				CommitVersion: resolve.GetCommitVersion(),
				Keys:          cloneApplyKeys(resolve.GetKeys()),
			})
		}
	}
	return out
}

func cloneApplyKeys(keys [][]byte) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) == 0 {
			out = append(out, nil)
			continue
		}
		out = append(out, kv.SafeCopy(nil, key))
	}
	return out
}
