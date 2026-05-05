package replicated

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
}

func Open(cfg Config) (*Store, error) {
	if cfg.Driver == nil {
		return nil, errDriverRequired
	}
	if cfg.MaxRetainedRecords <= 0 {
		cfg.MaxRetainedRecords = defaultRetainedRecords
	}
	observed, err := rootstorage.ObserveCommitted(cfg.Driver, 0)
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
	return rootstorage.ObserveCommitted(s.driver, 0)
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

func (s *Store) issueGrantLocked(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	holderID := strings.TrimSpace(cmd.HolderID)
	if holderID == "" || cmd.ExpiresUnixNano <= cmd.NowUnixNano || len(cmd.RequestedDuties) == 0 {
		return s.state.Eunomia(), rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
	}
	requestedGrantID := strings.TrimSpace(cmd.GrantID)
	if requestedGrantID != "" &&
		s.state.ActiveGrant.Present() &&
		s.state.ActiveGrant.GrantID == requestedGrantID &&
		s.state.ActiveGrant.HolderID == holderID &&
		dutyBoundsCovered(s.state.ActiveGrant.Duties, cmd.RequestedDuties) {
		cert, err := signGrantCertificate(s.state.ActiveGrant)
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
		if !dutyBoundsCovered(cmd.RequestedDuties, retirement.Bounds) {
			return s.state.Eunomia(), rootproto.GrantCertificate{}, rootstate.ErrInheritance
		}
		predecessors = append(predecessors, retirement)
	}
	active := s.state.ActiveGrant
	if active.Present() {
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
	if grantID == "" || grantID == active.GrantID || retiredGrantIDExists(s.state.RetiredGrants, grantID) {
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
	cert, err := signGrantCertificate(commit.State.ActiveGrant)
	if err != nil {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, err
	}
	return commit.State.Eunomia(), cert, nil
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
	active := s.state.ActiveGrant
	if !active.Present() {
		return s.state.Eunomia(), rootstate.ErrPrimacy
	}
	if strings.TrimSpace(cmd.HolderID) != active.HolderID {
		return s.state.Eunomia(), rootstate.ErrPrimacy
	}
	if cmd.GrantID != "" && cmd.GrantID != active.GrantID {
		return s.state.Eunomia(), rootstate.ErrPrimacy
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
	active := s.state.ActiveGrant
	if !active.Present() {
		return s.state.Eunomia(), nil
	}
	if cmd.GrantID != "" && cmd.GrantID != active.GrantID {
		return s.state.Eunomia(), rootstate.ErrPrimacy
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
	if !s.state.ActiveGrant.Present() {
		return s.state.Eunomia(), rootstate.ErrPrimacy
	}
	if strings.TrimSpace(cmd.HolderID) != s.state.ActiveGrant.HolderID {
		return s.state.Eunomia(), rootstate.ErrPrimacy
	}
	successor := s.state.ActiveGrant.GrantID
	events := make([]rootevent.Event, 0, len(cmd.PredecessorGrantIDs))
	for _, predecessor := range cmd.PredecessorGrantIDs {
		if strings.TrimSpace(predecessor) == "" {
			continue
		}
		events = append(events, rootevent.GrantInherited(rootproto.GrantInheritance{
			PredecessorGrantID: predecessor,
			SuccessorGrantID:   successor,
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
	era := state.ActiveGrant.Era
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
		grant, ok := findDutyGrant(grantBounds, usage.DutyID)
		if !ok || !dutyBoundCovers(grant.Bound, usage.Bound) {
			return false
		}
	}
	return true
}

func findDutyGrant(grants []rootproto.DutyGrant, duty rootproto.DutyID) (rootproto.DutyGrant, bool) {
	for _, grant := range grants {
		if grant.DutyID == duty {
			return grant, true
		}
	}
	return rootproto.DutyGrant{}, false
}

func dutyBoundCovers(grant, usage rootproto.DutyBound) bool {
	if grant.Kind != usage.Kind {
		return false
	}
	switch usage.Kind {
	case rootproto.DutyBoundMonotone:
		return usage.MonotoneUpper <= grant.MonotoneUpper
	case rootproto.DutyBoundVersion:
		return usage.DescriptorRevisionCeiling <= grant.DescriptorRevisionCeiling &&
			usage.MaxRootLag <= grant.MaxRootLag
	case rootproto.DutyBoundBudget:
		return usage.Budget <= grant.Budget
	case rootproto.DutyBoundEpoch:
		return usage.Epoch <= grant.Epoch
	default:
		return false
	}
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
	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
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
	plan := rootstorage.PlanTailCompaction(s.records, s.state.LastCommitted, s.maxRetainedRecords)
	if !plan.Compacted {
		s.records = plan.Tail.Records
		s.retainFrom = plan.RetainFrom
		return
	}
	if err := s.driver.InstallBootstrap(plan.Observed(snapshot)); err != nil {
		return
	}
	s.records = plan.Tail.Records
	s.retainFrom = plan.RetainFrom
}

func (s *Store) applyObserved(observed rootstorage.ObservedCommitted) {
	if s == nil {
		return
	}
	bootstrap := rootmaterialize.BootstrapFromObserved(observed)
	s.mu.Lock()
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
	s.mu.Unlock()
}
