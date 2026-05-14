// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package replicated

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootfailpoints "github.com/feichai0017/NoKV/meta/root/failpoints"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	"google.golang.org/protobuf/proto"
)

const defaultRetainedRecords = 64

// Config wires one committed ordered log and checkpoint store into the
// replicated metadata-root backend. Replication and transport are supplied by
// the injected log implementation, not by this package.
type Config struct {
	Driver             Driver
	MaxRetainedRecords int
}

// Store hosts the rooted state machine on top of an injected committed log and
// checkpoint store. It is the future Delos-lite landing point for a replicated
// metadata backend, without baking protocol concerns into the root domain.
type Store struct {
	driver Driver

	mu                 sync.RWMutex
	state              rootstate.State
	stores             map[uint64]rootstate.StoreMembership
	snapshots          map[string]rootstate.SnapshotEpoch
	mounts             map[string]rootstate.MountRecord
	subtrees           map[string]rootstate.SubtreeAuthority
	quotas             map[string]rootstate.QuotaFence
	descs              map[uint64]topology.Descriptor
	pending            map[uint64]rootstate.PendingPeerChange
	pendingRange       map[uint64]rootstate.PendingRangeChange
	records            []rootstorage.CommittedEvent
	retainFrom         rootstate.Cursor
	maxRetainedRecords int

	eunomiaCompactDroppedRetirements  atomic.Uint64
	eunomiaCompactDroppedInheritances atomic.Uint64
}

func Open(cfg Config) (*Store, error) {
	if cfg.Driver == nil {
		return nil, errDriverRequired
	}
	if cfg.MaxRetainedRecords <= 0 {
		cfg.MaxRetainedRecords = defaultRetainedRecords
	}
	observed, err := observeDriverCommitted(cfg.Driver)
	if err != nil {
		return nil, err
	}
	bootstrap := rootmaterialize.BootstrapFromObserved(observed)
	stores := bootstrap.Snapshot.Stores
	if stores == nil {
		stores = make(map[uint64]rootstate.StoreMembership)
	}
	snapshots := bootstrap.Snapshot.SnapshotEpochs
	if snapshots == nil {
		snapshots = make(map[string]rootstate.SnapshotEpoch)
	}
	mounts := bootstrap.Snapshot.Mounts
	if mounts == nil {
		mounts = make(map[string]rootstate.MountRecord)
	}
	subtrees := bootstrap.Snapshot.Subtrees
	if subtrees == nil {
		subtrees = make(map[string]rootstate.SubtreeAuthority)
	}
	quotas := bootstrap.Snapshot.Quotas
	if quotas == nil {
		quotas = make(map[string]rootstate.QuotaFence)
	}
	return &Store{
		driver:             cfg.Driver,
		state:              bootstrap.Snapshot.State,
		stores:             stores,
		snapshots:          snapshots,
		mounts:             mounts,
		subtrees:           subtrees,
		quotas:             quotas,
		descs:              bootstrap.Snapshot.Descriptors,
		pending:            bootstrap.Snapshot.PendingPeerChanges,
		pendingRange:       bootstrap.Snapshot.PendingRangeChanges,
		records:            bootstrap.Tail.Records,
		retainFrom:         bootstrap.RetainFrom,
		maxRetainedRecords: cfg.MaxRetainedRecords,
	}, nil
}

func (s *Store) IsLeader() bool {
	if s == nil || s.driver == nil {
		return true
	}
	return s.driver.IsLeader()
}

func (s *Store) CanSubmitRootWrites() bool {
	return s.IsLeader()
}

func (s *Store) PrepareRootWrite(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensureFreshForRootWriteLocked(ctx)
}

func (s *Store) LeaderID() uint64 {
	if s == nil || s.driver == nil {
		return 0
	}
	return s.driver.LeaderID()
}

func (s *Store) Campaign() error {
	if s == nil || s.driver == nil {
		return nil
	}
	return s.driver.Campaign()
}

// Refresh reloads the rooted checkpoint plus retained committed tail from the
// backing driver. Followers use this to catch up their in-memory view without
// reopening the store.
func (s *Store) Refresh() error {
	if s == nil {
		return nil
	}
	observed, err := s.ObserveCommitted()
	if err != nil {
		return err
	}
	s.applyObserved(observed)
	return nil
}

// ObserveCommitted returns the current compact checkpoint plus retained
// committed tail observed from the replicated root substrate.
func (s *Store) ObserveCommitted() (rootstorage.ObservedCommitted, error) {
	if s == nil {
		return rootstorage.ObservedCommitted{}, nil
	}
	return observeDriverCommitted(s.driver)
}

func observeDriverCommitted(driver Driver) (rootstorage.ObservedCommitted, error) {
	if observer, ok := driver.(interface {
		ObserveCommitted() (rootstorage.ObservedCommitted, error)
	}); ok {
		return observer.ObserveCommitted()
	}
	return rootstorage.ObserveCommitted(driver, 0)
}

func (s *Store) Stats() map[string]any {
	if s == nil {
		return map[string]any{}
	}
	out := map[string]any{}
	if stats, ok := s.driver.(interface{ Stats() map[string]any }); ok {
		maps.Copy(out, stats.Stats())
	}
	out["eunomia_compact_dropped_retirements_total"] = s.eunomiaCompactDroppedRetirements.Load()
	out["eunomia_compact_dropped_inheritances_total"] = s.eunomiaCompactDroppedInheritances.Load()
	return out
}

// WaitForTail waits until the durable committed tail view changes past after.
// This is the current catch-up primitive for follower services until a more
// direct push/watch path exists.
func (s *Store) WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
	if s == nil {
		return rootstorage.TailAdvance{}, nil
	}
	return s.driver.WaitForTail(after, timeout)
}

func (s *Store) ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	if s == nil {
		return rootstorage.TailAdvance{}, nil
	}
	return s.driver.ObserveTail(after)
}

func (s *Store) TailNotify() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.driver.TailNotify()
}

// SubscribeTail returns one watch-like rooted tail subscription over the
// replicated metadata substrate.
func (s *Store) SubscribeTail(after rootstorage.TailToken) *rootstorage.TailSubscription {
	if s == nil {
		return nil
	}
	return rootstorage.NewWatchedTailSubscription(after, s.ObserveTail, s.TailNotify(), s.WaitForTail)
}

func (s *Store) Current() (rootstate.State, error) {
	if s == nil {
		return rootstate.State{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state, nil
}

func (s *Store) Snapshot() (rootstate.Snapshot, error) {
	if s == nil {
		return rootstate.Snapshot{Descriptors: make(map[uint64]topology.Descriptor)}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return rootstate.CloneSnapshot(rootstate.Snapshot{
		State:               s.state,
		Stores:              s.stores,
		SnapshotEpochs:      s.snapshots,
		Mounts:              s.mounts,
		Subtrees:            s.subtrees,
		Quotas:              s.quotas,
		Descriptors:         s.descs,
		PendingPeerChanges:  s.pending,
		PendingRangeChanges: s.pendingRange,
	}), nil
}

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

func (s *Store) Append(ctx context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error) {
	if s == nil || len(events) == 0 {
		state, _ := s.Current()
		return rootstate.CommitInfo{Cursor: state.LastCommitted, State: state}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return rootstate.CommitInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureFreshForRootWriteLocked(ctx); err != nil {
		return rootstate.CommitInfo{}, err
	}
	return s.appendLocked(ctx, events...)
}

func (s *Store) appendLocked(ctx context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error) {
	var next rootstate.Cursor
	snapshot := rootstate.Snapshot{
		State:               s.state,
		Stores:              rootstate.CloneStoreMemberships(s.stores),
		SnapshotEpochs:      rootstate.CloneSnapshotEpochs(s.snapshots),
		Mounts:              rootstate.CloneMounts(s.mounts),
		Subtrees:            rootstate.CloneSubtreeAuthorities(s.subtrees),
		Quotas:              rootstate.CloneQuotaFences(s.quotas),
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
	snapshot.State = s.compactEunomiaState(snapshot.State)
	logEnd, err := s.driver.AppendCommitted(ctx, records...)
	if err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := rootfailpoints.InjectAfterAppendCommittedBeforeCheckpoint(); err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := s.driver.SaveCheckpoint(rootstorage.Checkpoint{
		Snapshot:   rootstate.CloneSnapshot(snapshot),
		TailOffset: logEnd,
	}); err != nil {
		return rootstate.CommitInfo{}, err
	}
	s.state = snapshot.State
	s.stores = snapshot.Stores
	s.snapshots = snapshot.SnapshotEpochs
	s.mounts = snapshot.Mounts
	s.subtrees = snapshot.Subtrees
	s.quotas = snapshot.Quotas
	s.descs = snapshot.Descriptors
	s.pending = snapshot.PendingPeerChanges
	s.pendingRange = snapshot.PendingRangeChanges
	s.records = append(s.records, records...)
	s.retainFrom = (rootstorage.CommittedTail{Records: s.records}).RetainFrom(snapshot.State.LastCommitted)
	s.maybeCompactLocked()
	return rootstate.CommitInfo{Cursor: snapshot.State.LastCommitted, State: snapshot.State}, nil
}

func (s *Store) ApplyGrant(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	if s == nil {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, nil
	}
	switch cmd.Kind {
	case rootproto.GrantActIssue:
		if err := rootfailpoints.InjectBeforeApplyGrantIssue(); err != nil {
			return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, err
		}
	case rootproto.GrantActSeal, rootproto.GrantActRetireExpired, rootproto.GrantActInherit:
		if err := rootfailpoints.InjectBeforeApplyGrantRetirement(); err != nil {
			return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, err
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureFreshForRootWriteLocked(ctx); err != nil {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, err
	}

	switch cmd.Kind {
	case rootproto.GrantActIssue:
		state, cert, err := s.issueGrantLocked(ctx, cmd)
		return state, cert, err
	case rootproto.GrantActSeal:
		state, err := s.sealGrantLocked(ctx, cmd)
		return state, rootproto.GrantCertificate{}, err
	case rootproto.GrantActRetireExpired:
		state, err := s.retireExpiredGrantLocked(ctx, cmd)
		return state, rootproto.GrantCertificate{}, err
	case rootproto.GrantActInherit:
		state, err := s.inheritGrantLocked(ctx, cmd)
		return state, rootproto.GrantCertificate{}, err
	default:
		return s.state.Eunomia(), rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
	}
}

func (s *Store) ApplyPerasAuthority(ctx context.Context, cmd rootproto.PerasAuthorityCommand) (rootstate.State, rootproto.PerasAuthorityGrant, error) {
	if s == nil {
		return rootstate.State{}, rootproto.PerasAuthorityGrant{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return rootstate.State{}, rootproto.PerasAuthorityGrant{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureFreshForRootWriteLocked(ctx); err != nil {
		return rootstate.State{}, rootproto.PerasAuthorityGrant{}, err
	}

	switch cmd.Kind {
	case rootproto.PerasAuthorityActAcquire:
		return s.acquirePerasAuthorityLocked(ctx, cmd)
	case rootproto.PerasAuthorityActRetire:
		return s.retirePerasAuthorityLocked(ctx, cmd)
	case rootproto.PerasAuthorityActSeal:
		return s.sealPerasAuthorityLocked(ctx, cmd)
	default:
		return rootstate.CloneState(s.state), rootproto.PerasAuthorityGrant{}, rootstate.ErrInvalidGrant
	}
}

func (s *Store) acquirePerasAuthorityLocked(ctx context.Context, cmd rootproto.PerasAuthorityCommand) (rootstate.State, rootproto.PerasAuthorityGrant, error) {
	holderID := strings.TrimSpace(cmd.HolderID)
	if holderID == "" || cmd.ExpiresUnixNano <= cmd.NowUnixNano || !cmd.Scope.Valid() {
		return rootstate.CloneState(s.state), rootproto.PerasAuthorityGrant{}, rootstate.ErrInvalidGrant
	}
	request := rootproto.PerasAuthorityGrant{
		GrantID:         "request",
		EpochID:         1,
		HolderID:        holderID,
		Scope:           rootproto.ClonePerasAuthorityScope(cmd.Scope),
		ExpiresUnixNano: cmd.ExpiresUnixNano,
	}
	requestedGrantID := strings.TrimSpace(cmd.GrantID)
	if requestedGrantID != "" {
		if active, ok := activePerasGrantByID(s.state.ActivePerasGrants, requestedGrantID); ok &&
			active.HolderID == holderID &&
			active.ActiveAt(cmd.NowUnixNano) &&
			active.Covers(cmd.Scope, cmd.NowUnixNano) {
			return rootstate.CloneState(s.state), active, nil
		}
	}

	var renewal rootproto.PerasAuthorityGrant
	for _, active := range s.state.ActivePerasGrants {
		if !active.Overlaps(request) {
			continue
		}
		// Peras authority TTL is an admission freshness bound, not proof that
		// the holder's visible overlay has been durably drained. Keep overlapping
		// grants rooted until the holder explicitly retires them; otherwise old
		// pending segments can be cut off by a newer epoch and fail witness
		// authority checks.
		if active.HolderID != holderID {
			return rootstate.CloneState(s.state), rootproto.PerasAuthorityGrant{}, rootstate.ErrPrimacy
		}
		if active.ActiveAt(cmd.NowUnixNano) &&
			active.Covers(cmd.Scope, cmd.NowUnixNano) &&
			cmd.ExpiresUnixNano <= active.ExpiresUnixNano {
			return rootstate.CloneState(s.state), rootproto.ClonePerasAuthorityGrant(active), nil
		}
		if !renewal.Valid() {
			renewal = rootproto.ClonePerasAuthorityGrant(active)
		}
	}
	if renewal.Valid() {
		renewal.Scope = mergePerasAuthorityScopes(renewal.Scope, cmd.Scope)
		if cmd.ExpiresUnixNano > renewal.ExpiresUnixNano {
			renewal.ExpiresUnixNano = cmd.ExpiresUnixNano
		}
		commit, err := s.appendLocked(ctx, rootevent.PerasAuthorityGranted(renewal))
		if err != nil {
			return rootstate.State{}, rootproto.PerasAuthorityGrant{}, err
		}
		committed, ok := commit.State.ActivePerasGrantByID(renewal.GrantID)
		if !ok || !committed.Covers(cmd.Scope, cmd.NowUnixNano) {
			return rootstate.State{}, rootproto.PerasAuthorityGrant{}, rootstate.ErrFinality
		}
		return rootstate.CloneState(commit.State), committed, nil
	}

	epoch := s.state.PerasAuthorityEpoch + 1
	grantID := requestedGrantID
	if grantID == "" || activePerasGrantIDExists(s.state.ActivePerasGrants, grantID) {
		grantID = fmt.Sprintf("%s/%d", holderID, epoch)
	}
	grant := rootproto.PerasAuthorityGrant{
		GrantID:           grantID,
		EpochID:           epoch,
		HolderID:          holderID,
		Scope:             rootproto.ClonePerasAuthorityScope(cmd.Scope),
		ExpiresUnixNano:   cmd.ExpiresUnixNano,
		QuotaCreditBytes:  cmd.QuotaCreditBytes,
		QuotaCreditInodes: cmd.QuotaCreditInodes,
	}
	grant.PredecessorDigest = cmd.PredecessorDigest
	if grant.PredecessorDigest == ([32]byte{}) {
		if predecessor, ok := s.state.LatestPerasAuthoritySealFor(cmd.Scope); ok {
			grant.PredecessorDigest = predecessor.SegmentRoot
		}
	}
	commit, err := s.appendLocked(ctx, rootevent.PerasAuthorityGranted(grant))
	if err != nil {
		return rootstate.State{}, rootproto.PerasAuthorityGrant{}, err
	}
	committed, ok := commit.State.ActivePerasGrantByID(grantID)
	if !ok {
		return rootstate.State{}, rootproto.PerasAuthorityGrant{}, rootstate.ErrFinality
	}
	return rootstate.CloneState(commit.State), committed, nil
}

func (s *Store) retirePerasAuthorityLocked(ctx context.Context, cmd rootproto.PerasAuthorityCommand) (rootstate.State, rootproto.PerasAuthorityGrant, error) {
	holderID := strings.TrimSpace(cmd.HolderID)
	grantID := strings.TrimSpace(cmd.GrantID)
	if holderID == "" || grantID == "" {
		return rootstate.CloneState(s.state), rootproto.PerasAuthorityGrant{}, rootstate.ErrInvalidGrant
	}
	active, ok := activePerasGrantByID(s.state.ActivePerasGrants, grantID)
	if !ok {
		return rootstate.CloneState(s.state), rootproto.PerasAuthorityGrant{}, nil
	}
	if active.HolderID != holderID {
		return rootstate.CloneState(s.state), rootproto.PerasAuthorityGrant{}, rootstate.ErrPrimacy
	}
	commit, err := s.appendLocked(ctx, rootevent.PerasAuthorityRetired(active))
	if err != nil {
		return rootstate.State{}, rootproto.PerasAuthorityGrant{}, err
	}
	return rootstate.CloneState(commit.State), rootproto.PerasAuthorityGrant{}, nil
}

func (s *Store) sealPerasAuthorityLocked(ctx context.Context, cmd rootproto.PerasAuthorityCommand) (rootstate.State, rootproto.PerasAuthorityGrant, error) {
	holderID := strings.TrimSpace(cmd.HolderID)
	grantID := strings.TrimSpace(cmd.GrantID)
	if holderID == "" || grantID == "" || cmd.NowUnixNano <= 0 {
		return rootstate.CloneState(s.state), rootproto.PerasAuthorityGrant{}, rootstate.ErrInvalidGrant
	}
	active, ok := activePerasGrantByID(s.state.ActivePerasGrants, grantID)
	if !ok {
		return rootstate.CloneState(s.state), rootproto.PerasAuthorityGrant{}, rootstate.ErrPrimacy
	}
	if active.HolderID != holderID {
		return rootstate.CloneState(s.state), rootproto.PerasAuthorityGrant{}, rootstate.ErrPrimacy
	}
	seal := rootproto.PerasAuthoritySeal{
		GrantID:              active.GrantID,
		EpochID:              active.EpochID,
		HolderID:             active.HolderID,
		Scope:                rootproto.ClonePerasAuthorityScope(active.Scope),
		SegmentRoot:          cmd.SegmentRoot,
		SegmentPayloadDigest: cmd.SegmentPayloadDigest,
		OperationCount:       cmd.OperationCount,
		EntryCount:           cmd.EntryCount,
		SealedUnixNano:       cmd.NowUnixNano,
		InstallRegionID:      cmd.InstallRegionID,
		InstallTerm:          cmd.InstallTerm,
		InstallIndex:         cmd.InstallIndex,
		InstallVersion:       cmd.InstallVersion,
	}
	if !seal.Valid() {
		return rootstate.CloneState(s.state), rootproto.PerasAuthorityGrant{}, rootstate.ErrInvalidGrant
	}
	commit, err := s.appendLocked(ctx, rootevent.PerasAuthoritySealed(seal))
	if err != nil {
		return rootstate.State{}, rootproto.PerasAuthorityGrant{}, err
	}
	return rootstate.CloneState(commit.State), active, nil
}

func (s *Store) issueGrantLocked(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	holderID := strings.TrimSpace(cmd.HolderID)
	if holderID == "" || cmd.ExpiresUnixNano <= cmd.NowUnixNano || len(cmd.RequestedDuties) == 0 {
		return s.state.Eunomia(), rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
	}
	if !validDutyGrants(cmd.RequestedDuties) || !validAuthorityUsages(cmd.ExactUsages) {
		return s.state.Eunomia(), rootproto.GrantCertificate{}, rootstate.ErrDuty
	}
	requestedKeys := dutyKeysFromGrants(cmd.RequestedDuties)
	requestedGrantID := strings.TrimSpace(cmd.GrantID)
	if requestedGrantID != "" &&
		func() bool {
			active, ok := activeGrantByID(s.state.ActiveGrants, requestedGrantID)
			return ok &&
				active.HolderID == holderID &&
				dutyBoundsCovered(active.Duties, cmd.RequestedDuties)
		}() {
		active, _ := activeGrantByID(s.state.ActiveGrants, requestedGrantID)
		cert, err := signGrantCertificate(active)
		if err != nil {
			return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, err
		}
		return s.state.Eunomia(), cert, nil
	}
	events := make([]rootevent.Event, 0, 3)
	var predecessors []rootproto.GrantRetirement
	for _, retirement := range s.state.RetiredGrants {
		if strings.TrimSpace(retirement.InheritedByGrantID) != "" {
			continue
		}
		if !dutyBoundsOverlap(retirement.Bounds, requestedKeys) {
			continue
		}
		if !dutyBoundsCovered(cmd.RequestedDuties, retirement.Bounds) {
			return s.state.Eunomia(), rootproto.GrantCertificate{}, rootstate.ErrInheritance
		}
		predecessors = append(predecessors, retirement)
	}
	var active rootproto.AuthorityGrant
	for _, candidate := range s.state.ActiveGrants {
		if !grantOverlapsDutyKeys(candidate, requestedKeys) {
			continue
		}
		active = candidate
		if active.ActiveAt(cmd.NowUnixNano) && active.HolderID != holderID {
			return s.state.Eunomia(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		mode := rootproto.GrantRetirementExpiredBound
		bounds := append([]rootproto.DutyGrant(nil), active.Duties...)
		if active.ActiveAt(cmd.NowUnixNano) && active.HolderID == holderID {
			mode = rootproto.GrantRetirementSealedExact
			bounds = exactUsageBounds(active, cmd.ExactUsages)
		}
		retirement := rootproto.GrantRetirement{
			GrantID:  active.GrantID,
			HolderID: active.HolderID,
			Era:      active.Era,
			Mode:     mode,
			Bounds:   bounds,
		}
		if !dutyBoundsCovered(cmd.RequestedDuties, retirement.Bounds) {
			return s.state.Eunomia(), rootproto.GrantCertificate{}, rootstate.ErrInheritance
		}
		predecessors = append(predecessors, retirement)
		if mode == rootproto.GrantRetirementSealedExact {
			events = append(events, rootevent.GrantSealed(retirement))
		} else {
			events = append(events, rootevent.GrantRetired(retirement))
		}
	}
	era := nextGrantEra(s.state)
	grantID := requestedGrantID
	if grantID == "" || activeGrantIDExists(s.state.ActiveGrants, grantID) || retiredGrantIDExists(s.state.RetiredGrants, grantID) {
		grantID = fmt.Sprintf("%s/%d", holderID, era)
	}
	grant := rootproto.AuthorityGrant{
		GrantID:                grantID,
		HolderID:               holderID,
		Era:                    era,
		ExpiresUnixNano:        cmd.ExpiresUnixNano,
		Duties:                 append([]rootproto.DutyGrant(nil), cmd.RequestedDuties...),
		PredecessorRetirements: predecessors,
	}
	events = append(events, rootevent.GrantIssued(grant))
	commit, err := s.appendLocked(ctx, events...)
	if err != nil {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, err
	}
	committed, ok := commit.State.ActiveGrantByID(grantID)
	if !ok {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, rootstate.ErrFinality
	}
	cert, err := signGrantCertificate(committed)
	if err != nil {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, err
	}
	return commit.State.Eunomia(), cert, nil
}

func activeGrantIDExists(grants []rootproto.AuthorityGrant, grantID string) bool {
	_, ok := activeGrantByID(grants, grantID)
	return ok
}

func activePerasGrantIDExists(grants []rootproto.PerasAuthorityGrant, grantID string) bool {
	_, ok := activePerasGrantByID(grants, grantID)
	return ok
}

func activePerasGrantByID(grants []rootproto.PerasAuthorityGrant, grantID string) (rootproto.PerasAuthorityGrant, bool) {
	for _, grant := range grants {
		if grant.GrantID == grantID {
			return rootproto.ClonePerasAuthorityGrant(grant), true
		}
	}
	return rootproto.PerasAuthorityGrant{}, false
}

func mergePerasAuthorityScopes(left, right rootproto.PerasAuthorityScope) rootproto.PerasAuthorityScope {
	out := rootproto.ClonePerasAuthorityScope(left)
	if out.MountID == "" {
		out.MountID = right.MountID
	}
	if out.MountKeyID == 0 {
		out.MountKeyID = right.MountKeyID
	}
	out.Buckets = appendMissingUint16(out.Buckets, right.Buckets)
	out.Parents = appendMissingUint64(out.Parents, right.Parents)
	out.Inodes = appendMissingUint64(out.Inodes, right.Inodes)
	return out
}

func appendMissingUint16(out []uint16, values []uint16) []uint16 {
	for _, value := range values {
		seen := slices.Contains(out, value)
		if !seen {
			out = append(out, value)
		}
	}
	return out
}

func appendMissingUint64(out []uint64, values []uint64) []uint64 {
	for _, value := range values {
		seen := slices.Contains(out, value)
		if !seen {
			out = append(out, value)
		}
	}
	return out
}

func retiredGrantIDExists(retirements []rootproto.GrantRetirement, grantID string) bool {
	for _, retirement := range retirements {
		if retirement.GrantID == grantID {
			return true
		}
	}
	return false
}

func (s *Store) sealGrantLocked(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, error) {
	active, ok := activeGrantByID(s.state.ActiveGrants, strings.TrimSpace(cmd.GrantID))
	if !ok {
		return s.state.Eunomia(), rootstate.ErrPrimacy
	}
	if strings.TrimSpace(cmd.HolderID) != active.HolderID {
		return s.state.Eunomia(), rootstate.ErrPrimacy
	}
	if !validAuthorityUsages(cmd.ExactUsages) {
		return s.state.Eunomia(), rootstate.ErrDuty
	}
	bounds := exactUsageBounds(active, cmd.ExactUsages)
	if !dutyBoundsCovered(active.Duties, bounds) {
		return s.state.Eunomia(), rootstate.ErrInheritance
	}
	commit, err := s.appendLocked(ctx, rootevent.GrantSealed(rootproto.GrantRetirement{
		GrantID:  active.GrantID,
		HolderID: active.HolderID,
		Era:      active.Era,
		Mode:     rootproto.GrantRetirementSealedExact,
		Bounds:   bounds,
	}))
	if err != nil {
		return rootstate.EunomiaState{}, err
	}
	return commit.State.Eunomia(), nil
}

func (s *Store) retireExpiredGrantLocked(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, error) {
	active, ok := activeGrantByID(s.state.ActiveGrants, strings.TrimSpace(cmd.GrantID))
	if !ok {
		return s.state.Eunomia(), nil
	}
	if active.ExpiresUnixNano > cmd.NowUnixNano {
		return s.state.Eunomia(), rootstate.ErrPrimacy
	}
	commit, err := s.appendLocked(ctx, rootevent.GrantRetired(rootproto.GrantRetirement{
		GrantID:  active.GrantID,
		HolderID: active.HolderID,
		Era:      active.Era,
		Mode:     rootproto.GrantRetirementExpiredBound,
		Bounds:   append([]rootproto.DutyGrant(nil), active.Duties...),
	}))
	if err != nil {
		return rootstate.EunomiaState{}, err
	}
	return commit.State.Eunomia(), nil
}

func (s *Store) inheritGrantLocked(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, error) {
	if len(s.state.ActiveGrants) == 0 {
		return s.state.Eunomia(), rootstate.ErrPrimacy
	}
	events := make([]rootevent.Event, 0, len(cmd.PredecessorGrantIDs))
	seen := make(map[string]struct{}, len(cmd.PredecessorGrantIDs))
	for _, predecessor := range cmd.PredecessorGrantIDs {
		predecessor = strings.TrimSpace(predecessor)
		if predecessor == "" {
			continue
		}
		if _, duplicate := seen[predecessor]; duplicate {
			continue
		}
		seen[predecessor] = struct{}{}
		retirement, ok := findRetirementByGrantID(s.state.RetiredGrants, predecessor)
		if !ok {
			return s.state.Eunomia(), rootstate.ErrInheritance
		}
		successor, ok := successorGrantForRetirement(s.state.ActiveGrants, strings.TrimSpace(cmd.HolderID), retirement)
		if !ok {
			return s.state.Eunomia(), rootstate.ErrPrimacy
		}
		if retirement.InheritedByGrantID != "" {
			if retirement.InheritedByGrantID == successor.GrantID {
				continue
			}
			return s.state.Eunomia(), rootstate.ErrFinality
		}
		if !predecessorRetirementCovered(successor, retirement) {
			return s.state.Eunomia(), rootstate.ErrInheritance
		}
		events = append(events, rootevent.GrantInherited(rootproto.GrantInheritance{
			PredecessorGrantID: predecessor,
			SuccessorGrantID:   successor.GrantID,
		}))
	}
	if len(events) == 0 {
		return s.state.Eunomia(), nil
	}
	commit, err := s.appendLocked(ctx, events...)
	if err != nil {
		return rootstate.EunomiaState{}, err
	}
	return commit.State.Eunomia(), nil
}

func nextGrantEra(state rootstate.State) uint64 {
	// Compaction can remove inherited retirement records after their finality is
	// represented only by retired-era floors. New grants must still allocate above
	// those compact floors, otherwise a rebooted root could reuse an era that
	// clients already consider retired.
	var era uint64
	for _, floor := range state.RetiredEraFloors {
		if floor.RetiredEraFloor > era {
			era = floor.RetiredEraFloor
		}
	}
	for _, grant := range state.ActiveGrants {
		if grant.Era > era {
			era = grant.Era
		}
	}
	for _, retirement := range state.RetiredGrants {
		if retirement.Era > era {
			era = retirement.Era
		}
	}
	return era + 1
}

func exactUsageBounds(grant rootproto.AuthorityGrant, usages []rootproto.AuthorityUsage) []rootproto.DutyGrant {
	if len(usages) == 0 {
		return append([]rootproto.DutyGrant(nil), grant.Duties...)
	}
	out := make([]rootproto.DutyGrant, 0, len(usages))
	for _, usage := range usages {
		out = append(out, rootproto.DutyGrant{DutyID: usage.DutyID, Scope: usage.Scope, Bound: usage.Usage})
	}
	return out
}

func dutyBoundsCovered(grantBounds, usageBounds []rootproto.DutyGrant) bool {
	for _, usage := range usageBounds {
		grant, ok := findDutyGrant(grantBounds, usage.DutyID, usage.Scope)
		if !ok || !rootproto.DutyBoundCovers(grant.Bound, usage.Bound) {
			return false
		}
	}
	return true
}

func findDutyGrant(grants []rootproto.DutyGrant, duty rootproto.DutyID, scope rootproto.DutyScope) (rootproto.DutyGrant, bool) {
	for _, grant := range grants {
		if grant.DutyID == duty && rootproto.ScopeEqual(grant.Scope, scope) {
			return grant, true
		}
	}
	return rootproto.DutyGrant{}, false
}

func dutyKeysFromGrants(grants []rootproto.DutyGrant) []rootproto.DutyKey {
	out := make([]rootproto.DutyKey, 0, len(grants))
	for _, grant := range grants {
		out = append(out, grant.Key())
	}
	return out
}

func dutyBoundsOverlap(grants []rootproto.DutyGrant, keys []rootproto.DutyKey) bool {
	for _, grant := range grants {
		for _, key := range keys {
			if rootproto.DutyKeyEqual(grant.Key(), key) {
				return true
			}
		}
	}
	return false
}

func grantOverlapsDutyKeys(grant rootproto.AuthorityGrant, keys []rootproto.DutyKey) bool {
	return dutyBoundsOverlap(grant.Duties, keys)
}

func activeGrantByID(grants []rootproto.AuthorityGrant, grantID string) (rootproto.AuthorityGrant, bool) {
	for _, grant := range grants {
		if grant.GrantID == grantID {
			return grant, true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func successorGrantForRetirement(grants []rootproto.AuthorityGrant, holderID string, retirement rootproto.GrantRetirement) (rootproto.AuthorityGrant, bool) {
	for _, grant := range grants {
		if strings.TrimSpace(grant.HolderID) != holderID {
			continue
		}
		if dutyBoundsCovered(grant.Duties, retirement.Bounds) {
			return grant, true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func validDutyGrants(grants []rootproto.DutyGrant) bool {
	seen := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		if !rootproto.ValidateDutyGrant(grant) {
			return false
		}
		key := string(grant.DutyID) + "\x00" + dutyScopeKey(grant.Scope)
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}

func validAuthorityUsages(usages []rootproto.AuthorityUsage) bool {
	for _, usage := range usages {
		if !rootproto.ValidateAuthorityUsage(usage) {
			return false
		}
	}
	return true
}

func predecessorRetirementCovered(successor rootproto.AuthorityGrant, retirement rootproto.GrantRetirement) bool {
	for _, inherited := range successor.PredecessorRetirements {
		if inherited.GrantID == retirement.GrantID &&
			inherited.Era == retirement.Era &&
			dutyBoundsCovered(successor.Duties, retirement.Bounds) {
			return true
		}
	}
	return false
}

func findRetirementByGrantID(retirements []rootproto.GrantRetirement, grantID string) (rootproto.GrantRetirement, bool) {
	for _, retirement := range retirements {
		if retirement.GrantID == grantID {
			return retirement, true
		}
	}
	return rootproto.GrantRetirement{}, false
}

func dutyScopeKey(scope rootproto.DutyScope) string {
	return fmt.Sprintf("%d/%s/%d/%x/%x", scope.Kind, scope.MountID, scope.SubtreeRoot, scope.StartKey, scope.EndKey)
}

func signGrantCertificate(grant rootproto.AuthorityGrant) (rootproto.GrantCertificate, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(metawire.RootAuthorityGrantToProto(grant))
	if err != nil {
		return rootproto.GrantCertificate{}, err
	}
	signature := rootproto.SignGrantBytes(payload)
	if len(signature) == 0 {
		return rootproto.GrantCertificate{}, fmt.Errorf("root grant signing key is not configured")
	}
	return rootproto.GrantCertificate{
		Grant:       grant,
		SignerKeyID: rootproto.GrantSignerKeyID,
		Signature:   signature,
	}, nil
}

func (s *Store) FenceAllocator(ctx context.Context, kind rootstate.AllocatorKind, min uint64) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	if err := s.ensureFreshForRootWriteLocked(ctx); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	state := s.state
	s.mu.Unlock()
	switch kind {
	case rootstate.AllocatorKindID:
		if state.IDFence >= min {
			return state.IDFence, nil
		}
		commit, err := s.Append(ctx, rootevent.IDAllocatorFenced(min))
		if err != nil {
			return 0, err
		}
		return commit.State.IDFence, nil
	case rootstate.AllocatorKindTSO:
		if state.TSOFence >= min {
			return state.TSOFence, nil
		}
		commit, err := s.Append(ctx, rootevent.TSOAllocatorFenced(min))
		if err != nil {
			return 0, err
		}
		return commit.State.TSOFence, nil
	default:
		return 0, errUnknownAllocatorKind(uint32(kind))
	}
}

// InstallBootstrap replaces the rooted checkpoint and retained committed tail.
// It is the future landing point for replicated snapshot installation.
func (s *Store) InstallBootstrap(observed rootstorage.ObservedCommitted) error {
	if s == nil {
		return nil
	}
	if err := s.driver.InstallBootstrap(observed); err != nil {
		return err
	}
	s.applyObserved(observed)
	return nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if closer, ok := s.driver.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) maybeCompactLocked() {
	if s == nil {
		return
	}
	plan := rootstorage.PlanTailCompaction(s.records, s.state.LastCommitted, s.maxRetainedRecords)
	snapshotState := s.compactEunomiaState(s.state)
	snapshot := rootstate.Snapshot{
		State:               snapshotState,
		Stores:              rootstate.CloneStoreMemberships(s.stores),
		SnapshotEpochs:      rootstate.CloneSnapshotEpochs(s.snapshots),
		Mounts:              rootstate.CloneMounts(s.mounts),
		Subtrees:            rootstate.CloneSubtreeAuthorities(s.subtrees),
		Quotas:              rootstate.CloneQuotaFences(s.quotas),
		Descriptors:         rootstate.CloneDescriptors(s.descs),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(s.pending),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(s.pendingRange),
	}
	if !plan.Compacted {
		s.records = plan.Tail.Records
		s.retainFrom = plan.RetainFrom
		return
	}
	if err := s.driver.InstallBootstrap(plan.Observed(snapshot)); err != nil {
		return
	}
	s.state = snapshotState
	s.records = plan.Tail.Records
	s.retainFrom = plan.RetainFrom
}

func (s *Store) compactEunomiaState(state rootstate.State) rootstate.State {
	compacted := rootstate.CompactEunomiaState(state)
	if s != nil {
		if dropped := len(state.RetiredGrants) - len(compacted.RetiredGrants); dropped > 0 {
			s.eunomiaCompactDroppedRetirements.Add(uint64(dropped))
		}
		if dropped := len(state.GrantInheritances) - len(compacted.GrantInheritances); dropped > 0 {
			s.eunomiaCompactDroppedInheritances.Add(uint64(dropped))
		}
	}
	return compacted
}

func (s *Store) ensureFreshForRootWriteLocked(ctx context.Context) error {
	if s == nil || s.driver == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.driver.IsLeader() {
		return errRootWriteRequiresLeader(s.driver.LeaderID())
	}
	observed, err := observeDriverCommitted(s.driver)
	if err != nil {
		return err
	}
	if rootstate.CursorAfter(observed.LastCursor(), s.state.LastCommitted) {
		s.applyObservedLocked(observed)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.driver.IsLeader() {
		return errRootWriteRequiresLeader(s.driver.LeaderID())
	}
	return nil
}

func (s *Store) applyObserved(observed rootstorage.ObservedCommitted) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyObservedLocked(observed)
}

func (s *Store) applyObservedLocked(observed rootstorage.ObservedCommitted) {
	bootstrap := rootmaterialize.BootstrapFromObserved(observed)
	if !committedCursorReached(bootstrap.Snapshot.State.LastCommitted, s.state.LastCommitted) {
		return
	}
	s.state = bootstrap.Snapshot.State
	s.stores = bootstrap.Snapshot.Stores
	if s.stores == nil {
		s.stores = make(map[uint64]rootstate.StoreMembership)
	}
	s.snapshots = bootstrap.Snapshot.SnapshotEpochs
	if s.snapshots == nil {
		s.snapshots = make(map[string]rootstate.SnapshotEpoch)
	}
	s.mounts = bootstrap.Snapshot.Mounts
	if s.mounts == nil {
		s.mounts = make(map[string]rootstate.MountRecord)
	}
	s.subtrees = bootstrap.Snapshot.Subtrees
	if s.subtrees == nil {
		s.subtrees = make(map[string]rootstate.SubtreeAuthority)
	}
	s.quotas = bootstrap.Snapshot.Quotas
	if s.quotas == nil {
		s.quotas = make(map[string]rootstate.QuotaFence)
	}
	s.descs = bootstrap.Snapshot.Descriptors
	s.pending = bootstrap.Snapshot.PendingPeerChanges
	s.pendingRange = bootstrap.Snapshot.PendingRangeChanges
	s.records = bootstrap.Tail.Records
	s.retainFrom = bootstrap.RetainFrom
}
