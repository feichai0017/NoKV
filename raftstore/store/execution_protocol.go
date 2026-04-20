package store

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	metaregion "github.com/feichai0017/NoKV/meta/region"
)

// AdmissionClass identifies the request class entering the execution plane.
type AdmissionClass uint8

const (
	AdmissionClassUnknown AdmissionClass = iota
	AdmissionClassRead
	AdmissionClassWrite
	AdmissionClassTopology
)

func (c AdmissionClass) String() string {
	switch c {
	case AdmissionClassRead:
		return "read"
	case AdmissionClassWrite:
		return "write"
	case AdmissionClassTopology:
		return "topology"
	default:
		return "unknown"
	}
}

// AdmissionReason explains why one request was accepted or rejected locally.
type AdmissionReason uint8

const (
	AdmissionReasonUnknown AdmissionReason = iota
	AdmissionReasonAccepted
	AdmissionReasonInvalid
	AdmissionReasonStoreNotMatch
	AdmissionReasonNotHosted
	AdmissionReasonEpochMismatch
	AdmissionReasonKeyNotInRegion
	AdmissionReasonNotLeader
	AdmissionReasonCanceled
	AdmissionReasonTimedOut
)

func (r AdmissionReason) String() string {
	switch r {
	case AdmissionReasonAccepted:
		return "accepted"
	case AdmissionReasonInvalid:
		return "invalid"
	case AdmissionReasonStoreNotMatch:
		return "store-not-match"
	case AdmissionReasonNotHosted:
		return "not-hosted"
	case AdmissionReasonEpochMismatch:
		return "epoch-mismatch"
	case AdmissionReasonKeyNotInRegion:
		return "key-not-in-region"
	case AdmissionReasonNotLeader:
		return "not-leader"
	case AdmissionReasonCanceled:
		return "canceled"
	case AdmissionReasonTimedOut:
		return "timed-out"
	default:
		return "unknown"
	}
}

// Admission records one local execution-plane admission decision.
type Admission struct {
	Class     AdmissionClass  `json:"class"`
	Reason    AdmissionReason `json:"reason"`
	Accepted  bool            `json:"accepted"`
	RegionID  uint64          `json:"region_id,omitempty"`
	PeerID    uint64          `json:"peer_id,omitempty"`
	RequestID uint64          `json:"request_id,omitempty"`
	Detail    string          `json:"detail,omitempty"`
	At        time.Time       `json:"at"`
}

// ExecutionOutcome describes the store-local lifecycle of one topology target.
type ExecutionOutcome uint8

const (
	ExecutionOutcomeUnknown ExecutionOutcome = iota
	ExecutionOutcomeRejected
	ExecutionOutcomeQueued
	ExecutionOutcomeProposed
	ExecutionOutcomeApplied
	ExecutionOutcomeFailed
)

func (o ExecutionOutcome) String() string {
	switch o {
	case ExecutionOutcomeRejected:
		return "rejected"
	case ExecutionOutcomeQueued:
		return "queued"
	case ExecutionOutcomeProposed:
		return "proposed"
	case ExecutionOutcomeApplied:
		return "applied"
	case ExecutionOutcomeFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// PublishState describes the control-plane publication boundary for one
// topology execution target.
type PublishState uint8

const (
	PublishStateUnknown PublishState = iota
	PublishStateNotRequired
	PublishStatePlannedPublished
	PublishStateTerminalPending
	PublishStateTerminalPublished
	PublishStateTerminalFailed
	PublishStateTerminalBlocked
)

func (s PublishState) String() string {
	switch s {
	case PublishStateNotRequired:
		return "not-required"
	case PublishStatePlannedPublished:
		return "planned-published"
	case PublishStateTerminalPending:
		return "terminal-pending"
	case PublishStateTerminalPublished:
		return "terminal-published"
	case PublishStateTerminalFailed:
		return "terminal-failed"
	case PublishStateTerminalBlocked:
		return "terminal-blocked"
	default:
		return "unknown"
	}
}

// TopologyExecution captures the current execution-plane view of one rooted
// topology transition target.
type TopologyExecution struct {
	TransitionID string           `json:"transition_id"`
	RegionID     uint64           `json:"region_id,omitempty"`
	Action       string           `json:"action,omitempty"`
	Outcome      ExecutionOutcome `json:"outcome"`
	Publish      PublishState     `json:"publish"`
	LastError    string           `json:"last_error,omitempty"`
	UpdatedAt    time.Time        `json:"updated_at"`
}

// RestartState classifies whether the local durable state is sufficient for one
// safe store restart.
type RestartState uint8

const (
	RestartStateUnknown RestartState = iota
	RestartStateReady
	RestartStateDegraded
)

func (s RestartState) String() string {
	switch s {
	case RestartStateReady:
		return "ready"
	case RestartStateDegraded:
		return "degraded"
	default:
		return "unknown"
	}
}

// RestartStatus summarizes the current store-local restart posture.
const executionTransitionRetention = 256

type RestartStatus struct {
	State                   RestartState `json:"state"`
	RegionCount             int          `json:"region_count"`
	RaftGroupCount          int          `json:"raft_group_count"`
	MissingRaftPointer      []uint64     `json:"missing_raft_pointer,omitempty"`
	PendingRootEventCount   int          `json:"pending_root_event_count,omitempty"`
	BlockedRootEventCount   int          `json:"blocked_root_event_count,omitempty"`
	PendingSchedulerOpCount int          `json:"pending_scheduler_operation_count,omitempty"`
}

type executionRuntime struct {
	mu            sync.RWMutex
	lastAdmission Admission
	transitions   map[string]TopologyExecution
}

func newExecutionRuntime() *executionRuntime {
	return &executionRuntime{
		transitions: make(map[string]TopologyExecution),
	}
}

func (s *Store) recordAdmission(admission Admission) {
	if s == nil || s.exec == nil {
		return
	}
	if admission.At.IsZero() {
		admission.At = time.Now()
	}
	s.exec.mu.Lock()
	s.exec.lastAdmission = admission
	s.exec.mu.Unlock()
}

// LastAdmission returns the most recent locally observed admission decision.
func (s *Store) LastAdmission() Admission {
	if s == nil || s.exec == nil {
		return Admission{}
	}
	s.exec.mu.RLock()
	defer s.exec.mu.RUnlock()
	return s.exec.lastAdmission
}

func (s *Store) updateTopologyExecution(transitionID string, update func(*TopologyExecution)) {
	if s == nil || s.exec == nil || transitionID == "" {
		return
	}
	s.exec.mu.Lock()
	entry := s.exec.transitions[transitionID]
	if entry.TransitionID == "" {
		entry.TransitionID = transitionID
	}
	update(&entry)
	entry.UpdatedAt = time.Now()
	s.exec.transitions[transitionID] = entry
	if len(s.exec.transitions) > executionTransitionRetention {
		s.exec.dropOldestTopologyExecutionLocked()
	}
	s.exec.mu.Unlock()
}

func (r *executionRuntime) dropOldestTopologyExecutionLocked() {
	if r == nil || len(r.transitions) <= executionTransitionRetention {
		return
	}
	var oldestID string
	var oldestAt time.Time
	for id, entry := range r.transitions {
		if oldestID == "" || entry.UpdatedAt.Before(oldestAt) || (entry.UpdatedAt.Equal(oldestAt) && id < oldestID) {
			oldestID = id
			oldestAt = entry.UpdatedAt
		}
	}
	if oldestID != "" {
		delete(r.transitions, oldestID)
	}
}

func (s *Store) recordTopologyQueued(target transitionTarget) {
	if target.TransitionID == "" {
		return
	}
	s.updateTopologyExecution(target.TransitionID, func(entry *TopologyExecution) {
		entry.RegionID = target.RegionID
		entry.Action = target.Action
		entry.Outcome = ExecutionOutcomeQueued
		entry.LastError = ""
		if s.schedulerClient() == nil {
			entry.Publish = PublishStateNotRequired
		}
	})
}

func (s *Store) recordTopologyProposed(target transitionTarget) {
	if target.TransitionID == "" {
		return
	}
	s.updateTopologyExecution(target.TransitionID, func(entry *TopologyExecution) {
		entry.RegionID = target.RegionID
		entry.Action = target.Action
		entry.Outcome = ExecutionOutcomeProposed
		entry.LastError = ""
		if s.schedulerClient() == nil {
			entry.Publish = PublishStateNotRequired
			return
		}
		entry.Publish = PublishStatePlannedPublished
	})
}

func (s *Store) recordTopologyRejected(target transitionTarget, err error) {
	if target.TransitionID == "" {
		return
	}
	s.updateTopologyExecution(target.TransitionID, func(entry *TopologyExecution) {
		entry.RegionID = target.RegionID
		entry.Action = target.Action
		entry.Outcome = ExecutionOutcomeRejected
		entry.LastError = errorString(err)
	})
}

func (s *Store) recordTopologyFailed(transitionID string, regionID uint64, action string, publish PublishState, err error) {
	if transitionID == "" {
		return
	}
	s.updateTopologyExecution(transitionID, func(entry *TopologyExecution) {
		if regionID != 0 {
			entry.RegionID = regionID
		}
		if action != "" {
			entry.Action = action
		}
		entry.Outcome = ExecutionOutcomeFailed
		if publish != PublishStateUnknown {
			entry.Publish = publish
		}
		entry.LastError = errorString(err)
	})
}

func (s *Store) recordTopologyApplied(term terminalOutcome) {
	if term.TransitionID == "" {
		return
	}
	s.updateTopologyExecution(term.TransitionID, func(entry *TopologyExecution) {
		if term.RegionID != 0 {
			entry.RegionID = term.RegionID
		}
		if term.Action != "" {
			entry.Action = term.Action
		}
		entry.Outcome = ExecutionOutcomeApplied
		entry.LastError = ""
		if s.schedulerClient() == nil || term.Event.Kind == 0 {
			entry.Publish = PublishStateNotRequired
			return
		}
		entry.Publish = PublishStateTerminalPending
	})
}

func (s *Store) recordTopologyPublished(event rootstateTransitionEvent) {
	if event.transitionID == "" {
		return
	}
	s.updateTopologyExecution(event.transitionID, func(entry *TopologyExecution) {
		entry.Outcome = ExecutionOutcomeApplied
		entry.Publish = PublishStateTerminalPublished
		entry.LastError = ""
	})
}

func (s *Store) recordTopologyPublishFailure(event rootstateTransitionEvent, err error) {
	if event.transitionID == "" {
		return
	}
	s.updateTopologyExecution(event.transitionID, func(entry *TopologyExecution) {
		entry.Outcome = ExecutionOutcomeApplied
		entry.Publish = PublishStateTerminalFailed
		entry.LastError = errorString(err)
	})
}

func (s *Store) recordTopologyPublishBlocked(event rootstateTransitionEvent, err error) {
	if event.transitionID == "" {
		return
	}
	s.updateTopologyExecution(event.transitionID, func(entry *TopologyExecution) {
		entry.Outcome = ExecutionOutcomeApplied
		entry.Publish = PublishStateTerminalBlocked
		entry.LastError = errorString(err)
	})
}

// TopologyExecution returns the tracked execution state for one transition id.
func (s *Store) TopologyExecution(transitionID string) (TopologyExecution, bool) {
	if s == nil || s.exec == nil || transitionID == "" {
		return TopologyExecution{}, false
	}
	s.exec.mu.RLock()
	defer s.exec.mu.RUnlock()
	entry, ok := s.exec.transitions[transitionID]
	return entry, ok
}

// TopologyExecutions returns a stable snapshot of tracked topology transitions.
func (s *Store) TopologyExecutions() []TopologyExecution {
	if s == nil || s.exec == nil {
		return nil
	}
	s.exec.mu.RLock()
	defer s.exec.mu.RUnlock()
	out := make([]TopologyExecution, 0, len(s.exec.transitions))
	for _, entry := range s.exec.transitions {
		out = append(out, entry)
	}
	slices.SortFunc(out, func(a, b TopologyExecution) int {
		if a.UpdatedAt.Before(b.UpdatedAt) {
			return -1
		}
		if b.UpdatedAt.Before(a.UpdatedAt) {
			return 1
		}
		switch {
		case a.TransitionID < b.TransitionID:
			return -1
		case a.TransitionID > b.TransitionID:
			return 1
		default:
			return 0
		}
	})
	return out
}

// RestartStatus returns the current store-local recovery posture derived from
// local metadata and raft replay pointers.
func (s *Store) RestartStatus() RestartStatus {
	if s == nil || s.regionMgr() == nil || s.regionMgr().localMeta == nil {
		return RestartStatus{State: RestartStateReady}
	}
	regions := s.regionMgr().localMeta.Snapshot()
	pointers := s.regionMgr().localMeta.RaftPointerSnapshot()
	out := RestartStatus{
		State:          RestartStateReady,
		RegionCount:    len(regions),
		RaftGroupCount: len(pointers),
	}
	if len(regions) > 0 {
		missing := make([]uint64, 0)
		for id, meta := range regions {
			if meta.State == metaregion.ReplicaStateTombstone {
				continue
			}
			if _, ok := pointers[id]; !ok {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			slices.Sort(missing)
			out.State = RestartStateDegraded
			out.MissingRaftPointer = missing
		}
	}
	out.PendingRootEventCount = len(s.regionMgr().localMeta.PendingRootEvents())
	out.BlockedRootEventCount = len(s.regionMgr().localMeta.BlockedRootEvents())
	out.PendingSchedulerOpCount = len(s.regionMgr().localMeta.PendingSchedulerOperations())
	if out.PendingRootEventCount > 0 || out.BlockedRootEventCount > 0 || out.PendingSchedulerOpCount > 0 {
		out.State = RestartStateDegraded
	}
	return out
}

func classifyContextAdmission(err error) AdmissionReason {
	switch {
	case errors.Is(err, context.Canceled):
		return AdmissionReasonCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return AdmissionReasonTimedOut
	default:
		return AdmissionReasonUnknown
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type rootstateTransitionEvent struct {
	transitionID string
}
