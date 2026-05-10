package store

import (
	myraft "github.com/feichai0017/NoKV/raft"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"github.com/feichai0017/NoKV/raftstore/store/observer"
)

// Public type aliases over the observer subpackage. Existing external
// callers (raftstore/kv, fsmeta integration, raftstore/integration tests)
// keep importing storepkg.ApplyEvent / ApplyObserver / etc unchanged.
type (
	ApplyEvent                = observer.Event
	ApplyEventSource          = observer.Source
	ApplyObserver             = observer.Observer
	ApplyObserverRegistration = observer.Registration
)

// Observer event sources, re-exported for callers that match on the
// Source field.
const (
	ApplyEventSourceCommit      = observer.SourceCommit
	ApplyEventSourceResolveLock = observer.SourceResolveLock
)

// RegisterApplyObserver registers one store-level post-apply observer.
func (s *Store) RegisterApplyObserver(o ApplyObserver, capacity int) (ApplyObserverRegistration, error) {
	if s == nil {
		return nil, errNilStore
	}
	if s.observers == nil {
		s.observers = observer.New()
	}
	return s.observers.Register(o, capacity)
}

// DroppedApplyObserverEvents returns how many observer events were dropped
// because registered observers could not keep up with raft apply.
func (s *Store) DroppedApplyObserverEvents() uint64 {
	if s == nil {
		return 0
	}
	return s.observers.Dropped()
}

func (s *Store) emitApplyEvents(entry myraft.Entry, req *raftcmdpb.RaftCmdRequest, resp *raftcmdpb.RaftCmdResponse) {
	if s == nil {
		return
	}
	for _, evt := range observer.EventsFromCommand(entry, req, resp) {
		if s.regionStats != nil {
			var keyBytes uint64
			for _, key := range evt.Keys {
				keyBytes += uint64(len(key))
			}
			s.regionStats.RecordApply(evt.RegionID, keyBytes, evt.AtomicMutate)
		}
		if s.observers == nil {
			continue
		}
		s.observers.Emit(evt)
	}
}
