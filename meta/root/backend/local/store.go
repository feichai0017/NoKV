package local

import (
	"fmt"
	"strings"
	"sync"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	"github.com/feichai0017/NoKV/engine/vfs"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	rootfile "github.com/feichai0017/NoKV/meta/root/storage/file"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

const maxRetainedRecords = 64

// Store is a file-backed local metadata-root implementation.
//
// It is intentionally minimal: an append-only event log, a compact protobuf
// checkpoint, and an in-memory event index for ReadSince.
type Store struct {
	log rootstorage.VirtualLog

	mu           sync.RWMutex
	state        rootstate.State
	descs        map[uint64]descriptor.Descriptor
	pending      map[uint64]rootstate.PendingPeerChange
	pendingRange map[uint64]rootstate.PendingRangeChange
	records      []rootstorage.CommittedEvent
	retainFrom   rootstate.Cursor
}

// Open opens or creates a local metadata-root store in workdir.
func Open(workdir string, fs vfs.FS) (*Store, error) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, fmt.Errorf("meta/root/backend/local: workdir is required")
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(workdir, 0o755); err != nil {
		return nil, err
	}
	log := rootfile.NewStore(fs, workdir)
	bootstrap, err := rootmaterialize.LoadBootstrap(log)
	if err != nil {
		return nil, err
	}
	return &Store{
		log:          log,
		state:        bootstrap.Snapshot.State,
		descs:        bootstrap.Snapshot.Descriptors,
		pending:      bootstrap.Snapshot.PendingPeerChanges,
		pendingRange: bootstrap.Snapshot.PendingRangeChanges,
		records:      bootstrap.Tail.Records,
		retainFrom:   bootstrap.RetainFrom,
	}, nil
}

// Current returns the current compact root state.
func (s *Store) Current() (rootstate.State, error) {
	if s == nil {
		return rootstate.State{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state, nil
}

// Snapshot returns the compact rooted metadata snapshot.
func (s *Store) Snapshot() (rootstate.Snapshot, error) {
	if s == nil {
		return rootstate.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return rootstate.CloneSnapshot(rootstate.Snapshot{
		State:               s.state,
		Descriptors:         s.descs,
		PendingPeerChanges:  s.pending,
		PendingRangeChanges: s.pendingRange,
	}), nil
}

// ReadSince returns all events after cursor together with the current tail cursor.
func (s *Store) ReadSince(cursor rootstate.Cursor) ([]rootevent.Event, rootstate.Cursor, error) {
	if s == nil {
		return nil, rootstate.Cursor{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rootstate.CursorAfter(s.retainFrom, cursor) {
		return rootmaterialize.SnapshotDescriptorEvents(s.descs), s.state.LastCommitted, nil
	}
	out := make([]rootevent.Event, 0, len(s.records))
	for _, rec := range s.records {
		if rootstate.CursorAfter(rec.Cursor, cursor) {
			out = append(out, rootevent.CloneEvent(rec.Event))
		}
	}
	return out, s.state.LastCommitted, nil
}

// Append appends ordered metadata events and advances the compact root state.
func (s *Store) Append(events ...rootevent.Event) (rootstate.CommitInfo, error) {
	if s == nil || len(events) == 0 {
		state, _ := s.Current()
		return rootstate.CommitInfo{Cursor: state.LastCommitted, State: state}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(events...)
}

func (s *Store) appendLocked(events ...rootevent.Event) (rootstate.CommitInfo, error) {
	var next rootstate.Cursor
	state := s.state
	snapshot := rootstate.Snapshot{
		State:               state,
		Descriptors:         rootstate.CloneDescriptors(s.descs),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(s.pending),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(s.pendingRange),
	}
	records := make([]rootstorage.CommittedEvent, 0, len(events))
	for _, evt := range events {
		next = rootstate.NextCursor(snapshot.State.LastCommitted)
		rootstate.ApplyEventToSnapshot(&snapshot, next, evt)
		records = append(records, rootstorage.CommittedEvent{Cursor: next, Event: rootevent.CloneEvent(evt)})
	}
	logEnd, err := s.log.AppendCommitted(records...)
	if err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := s.log.SaveCheckpoint(rootstorage.Checkpoint{
		Snapshot:   rootstate.CloneSnapshot(snapshot),
		TailOffset: logEnd,
	}); err != nil {
		return rootstate.CommitInfo{}, err
	}
	s.state = snapshot.State
	s.descs = snapshot.Descriptors
	s.pending = snapshot.PendingPeerChanges
	s.pendingRange = snapshot.PendingRangeChanges
	s.records = append(s.records, records...)
	s.retainFrom = (rootstorage.CommittedTail{Records: s.records}).RetainFrom(snapshot.State.LastCommitted)
	s.maybeCompactLocked()
	return rootstate.CommitInfo{Cursor: snapshot.State.LastCommitted, State: snapshot.State}, nil
}

func (s *Store) ApplyCoordinatorLease(cmd rootstate.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error) {
	if s == nil {
		return rootstate.CoordinatorProtocolState{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Kind {
	case rootstate.CoordinatorLeaseCommandIssue:
		if err := rootstate.ValidateCoordinatorLeaseCampaign(s.state.CoordinatorLease, s.state.CoordinatorSeal, cmd.HolderID, cmd.PredecessorDigest, cmd.ExpiresUnixNano, cmd.NowUnixNano); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		if err := rootstate.ValidateCoordinatorLeaseSuccessorCoverageFrontiers(
			s.state.CoordinatorLease,
			s.state.CoordinatorSeal,
			cmd.HandoffFrontiers,
		); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		generation := rootstate.NextCoordinatorLeaseGeneration(s.state.CoordinatorLease, s.state.CoordinatorSeal, cmd.HolderID, cmd.NowUnixNano)
		event := rootevent.CoordinatorLeaseGranted(
			cmd.HolderID,
			cmd.ExpiresUnixNano,
			generation,
			rootstate.CoordinatorDutyMaskDefault,
			cmd.PredecessorDigest,
			rootstate.CloneDutyFrontiers(cmd.HandoffFrontiers),
		)
		commit, err := s.appendLocked(event)
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	case rootstate.CoordinatorLeaseCommandRelease:
		if err := rootstate.ValidateCoordinatorLeaseRelease(s.state.CoordinatorLease, cmd.HolderID, cmd.NowUnixNano); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		current := s.state.CoordinatorLease
		dutyMask := current.DutyMask
		if dutyMask == 0 {
			dutyMask = rootstate.CoordinatorDutyMaskDefault
		}
		event := rootevent.CoordinatorLeaseReleased(
			cmd.HolderID,
			cmd.NowUnixNano,
			current.CertGeneration,
			dutyMask,
			current.PredecessorDigest,
			rootstate.CloneDutyFrontiers(cmd.HandoffFrontiers),
		)
		commit, err := s.appendLocked(event)
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	default:
		return s.state.CoordinatorProtocol(), rootstate.ErrInvalidCoordinatorLease
	}
}

func (s *Store) ApplyCoordinatorClosure(cmd rootstate.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error) {
	if s == nil {
		return rootstate.CoordinatorProtocolState{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Kind {
	case rootstate.CoordinatorClosureCommandSeal:
		current := s.state.CoordinatorLease
		if s.state.CoordinatorSeal.CertGeneration != 0 &&
			s.state.CoordinatorSeal.CertGeneration == current.CertGeneration &&
			s.state.CoordinatorSeal.HolderID == strings.TrimSpace(cmd.HolderID) {
			return s.state.CoordinatorProtocol(), nil
		}
		if err := rootstate.ValidateCoordinatorLeaseSeal(current, cmd.HolderID); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		dutyMask := current.DutyMask
		if dutyMask == 0 {
			dutyMask = rootstate.CoordinatorDutyMaskDefault
		}
		commit, err := s.appendLocked(rootevent.CoordinatorLeaseSealed(
			cmd.HolderID,
			current.CertGeneration,
			dutyMask,
			rootstate.CloneDutyFrontiers(cmd.Frontiers),
		))
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	case rootstate.CoordinatorClosureCommandConfirm:
		if strings.TrimSpace(cmd.HolderID) == "" || strings.TrimSpace(cmd.HolderID) != s.state.CoordinatorLease.HolderID {
			return s.state.CoordinatorProtocol(), rootstate.ErrCoordinatorLeaseOwner
		}
		auditStatus, err := controlplane.ValidateClosureConfirmation(
			s.state.CoordinatorLease,
			controlplane.FrontiersFromState(s.state, controlplane.MaxDescriptorRevision(s.descs)),
			s.state.CoordinatorSeal,
			cmd.NowUnixNano,
		)
		if err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		if s.state.CoordinatorClosure.Stage >= rootstate.CoordinatorClosureStageConfirmed &&
			s.state.CoordinatorClosure.SealGeneration == auditStatus.SealGeneration &&
			s.state.CoordinatorClosure.SuccessorGeneration == s.state.CoordinatorLease.CertGeneration &&
			s.state.CoordinatorClosure.SealDigest == auditStatus.SealDigest {
			return s.state.CoordinatorProtocol(), nil
		}
		commit, err := s.appendLocked(rootevent.CoordinatorClosureConfirmed(cmd.HolderID, auditStatus.SealGeneration, s.state.CoordinatorLease.CertGeneration, auditStatus.SealDigest))
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	case rootstate.CoordinatorClosureCommandClose:
		if err := controlplane.ValidateClosureClose(s.state.CoordinatorLease, s.state.CoordinatorClosure, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		if s.state.CoordinatorClosure.Stage >= rootstate.CoordinatorClosureStageClosed {
			return s.state.CoordinatorProtocol(), nil
		}
		commit, err := s.appendLocked(rootevent.CoordinatorClosureClosed(
			cmd.HolderID,
			s.state.CoordinatorClosure.SealGeneration,
			s.state.CoordinatorClosure.SuccessorGeneration,
			s.state.CoordinatorClosure.SealDigest,
		))
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	case rootstate.CoordinatorClosureCommandReattach:
		if err := controlplane.ValidateClosureReattach(s.state.CoordinatorLease, s.state.CoordinatorClosure, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		if s.state.CoordinatorClosure.Stage >= rootstate.CoordinatorClosureStageReattached {
			return s.state.CoordinatorProtocol(), nil
		}
		commit, err := s.appendLocked(rootevent.CoordinatorClosureReattached(
			cmd.HolderID,
			s.state.CoordinatorClosure.SealGeneration,
			s.state.CoordinatorClosure.SuccessorGeneration,
			s.state.CoordinatorClosure.SealDigest,
		))
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	default:
		return s.state.CoordinatorProtocol(), rootstate.ErrCoordinatorLeaseAudit
	}
}

// FenceAllocator advances one global allocator fence monotonically.
func (s *Store) FenceAllocator(kind rootstate.AllocatorKind, min uint64) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	switch kind {
	case rootstate.AllocatorKindID:
		if state.IDFence >= min {
			return state.IDFence, nil
		}
		commit, err := s.Append(rootevent.IDAllocatorFenced(min))
		if err != nil {
			return 0, err
		}
		return commit.State.IDFence, nil
	case rootstate.AllocatorKindTSO:
		if state.TSOFence >= min {
			return state.TSOFence, nil
		}
		commit, err := s.Append(rootevent.TSOAllocatorFenced(min))
		if err != nil {
			return 0, err
		}
		return commit.State.TSOFence, nil
	default:
		return 0, fmt.Errorf("meta/root/backend/local: unknown allocator kind %d", kind)
	}
}

func (s *Store) Close() error { return nil }

func (s *Store) maybeCompactLocked() {
	if s == nil || len(s.records) <= maxRetainedRecords {
		return
	}
	snapshot := rootstate.Snapshot{
		State:               s.state,
		Descriptors:         rootstate.CloneDescriptors(s.descs),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(s.pending),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(s.pendingRange),
	}
	plan := rootstorage.PlanTailCompaction(s.records, s.state.LastCommitted, maxRetainedRecords)
	if !plan.Compacted {
		s.records = plan.Tail.Records
		s.retainFrom = plan.RetainFrom
		return
	}
	if err := s.log.InstallBootstrap(plan.Observed(snapshot)); err != nil {
		return
	}
	s.records = plan.Tail.Records
	s.retainFrom = plan.RetainFrom
}
