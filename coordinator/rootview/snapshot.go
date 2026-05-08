package rootview

import (
	"math"
	"slices"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/meta/topology"
)

// AllocatorState captures persisted counters for ID and TSO allocators.
type AllocatorState struct {
	IDCurrent uint64
	TSCurrent uint64
}

type CatchUpState uint8

const (
	CatchUpStateUnspecified CatchUpState = iota
	CatchUpStateFresh
	CatchUpStateLagging
	CatchUpStateBootstrapRequired
	CatchUpStateUnavailable
)

func (s CatchUpState) String() string {
	switch s {
	case CatchUpStateFresh:
		return "fresh"
	case CatchUpStateLagging:
		return "lagging"
	case CatchUpStateBootstrapRequired:
		return "bootstrap_required"
	case CatchUpStateUnavailable:
		return "unavailable"
	default:
		return "unspecified"
	}
}

// Snapshot is the reconstructed Coordinator bootstrap catalog derived from durable
// metadata-root truth.
type Snapshot struct {
	ClusterEpoch        uint64
	RootToken           rootstorage.TailToken
	CatchUpState        CatchUpState
	Stores              map[uint64]rootstate.StoreMembership
	SnapshotEpochs      map[string]rootstate.SnapshotEpoch
	Mounts              map[string]rootstate.MountRecord
	Subtrees            map[string]rootstate.SubtreeAuthority
	Quotas              map[string]rootstate.QuotaFence
	Descriptors         map[uint64]topology.Descriptor
	PendingPeerChanges  map[uint64]rootstate.PendingPeerChange
	PendingRangeChanges map[uint64]rootstate.PendingRangeChange
	Allocator           AllocatorState
	ActiveGrants        []rootproto.AuthorityGrant
	RetiredGrants       []rootproto.GrantRetirement
	GrantInheritances   []rootproto.GrantInheritance
	RetiredEraFloor     uint64
}

func (s Snapshot) ActiveGrantFor(duty rootproto.DutyID, scope rootproto.DutyScope) (rootproto.AuthorityGrant, bool) {
	for _, grant := range s.ActiveGrants {
		if grant.CoversDutyKey(rootproto.DutyKey{DutyID: duty, Scope: scope}) {
			return cloneAuthorityGrant(grant), true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func (s Snapshot) ActiveGrantByID(grantID string) (rootproto.AuthorityGrant, bool) {
	for _, grant := range s.ActiveGrants {
		if grant.GrantID == grantID {
			return cloneAuthorityGrant(grant), true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func CloneSnapshot(snapshot Snapshot) Snapshot {
	return Snapshot{
		ClusterEpoch:        snapshot.ClusterEpoch,
		RootToken:           snapshot.RootToken,
		CatchUpState:        snapshot.CatchUpState,
		Stores:              rootstate.CloneStoreMemberships(snapshot.Stores),
		SnapshotEpochs:      rootstate.CloneSnapshotEpochs(snapshot.SnapshotEpochs),
		Mounts:              rootstate.CloneMounts(snapshot.Mounts),
		Subtrees:            rootstate.CloneSubtreeAuthorities(snapshot.Subtrees),
		Quotas:              rootstate.CloneQuotaFences(snapshot.Quotas),
		Descriptors:         rootstate.CloneDescriptors(snapshot.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(snapshot.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(snapshot.PendingRangeChanges),
		Allocator:           snapshot.Allocator,
		ActiveGrants:        cloneAuthorityGrants(snapshot.ActiveGrants),
		RetiredGrants:       append([]rootproto.GrantRetirement(nil), snapshot.RetiredGrants...),
		GrantInheritances:   append([]rootproto.GrantInheritance(nil), snapshot.GrantInheritances...),
		RetiredEraFloor:     snapshot.RetiredEraFloor,
	}
}

// PreserveNewerAuthorityState carries the locally authoritative grant lifecycle
// forward when an observed root snapshot is older than a just-applied grant
// response. Descriptors and allocator fences still come from observed; only the
// Eunomia authority mirror is protected against stale replacement.
func PreserveNewerAuthorityState(observed, current Snapshot) Snapshot {
	out := CloneSnapshot(observed)
	for _, currentGrant := range current.ActiveGrants {
		if !currentGrant.Present() || observedRetiresGrant(observed, currentGrant) {
			continue
		}
		observedGrant, ok := activeGrantOverlapping(out.ActiveGrants, currentGrant)
		if ok && !authorityGrantNewer(currentGrant, observedGrant) {
			continue
		}
		out.ActiveGrants = removeOverlappingActiveGrant(out.ActiveGrants, currentGrant)
		out.ActiveGrants = append(out.ActiveGrants, cloneAuthorityGrant(currentGrant))
	}
	out.RetiredGrants = mergeGrantRetirements(out.RetiredGrants, current.RetiredGrants)
	out.GrantInheritances = mergeGrantInheritances(out.GrantInheritances, current.GrantInheritances)
	if current.RetiredEraFloor > out.RetiredEraFloor {
		out.RetiredEraFloor = current.RetiredEraFloor
	}
	return out
}

func observedRetiresGrant(observed Snapshot, grant rootproto.AuthorityGrant) bool {
	for _, retirement := range observed.RetiredGrants {
		if retirement.GrantID == grant.GrantID && retirement.Era == grant.Era {
			return true
		}
	}
	return false
}

func activeGrantOverlapping(grants []rootproto.AuthorityGrant, needle rootproto.AuthorityGrant) (rootproto.AuthorityGrant, bool) {
	for _, grant := range grants {
		if authorityGrantsOverlap(grant, needle) {
			return grant, true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func authorityGrantNewer(current, observed rootproto.AuthorityGrant) bool {
	if current.Era != observed.Era {
		return current.Era > observed.Era
	}
	return rootstate.CursorAfter(current.IssuedAt, observed.IssuedAt)
}

func removeOverlappingActiveGrant(grants []rootproto.AuthorityGrant, needle rootproto.AuthorityGrant) []rootproto.AuthorityGrant {
	out := grants[:0]
	for _, grant := range grants {
		if !authorityGrantsOverlap(grant, needle) {
			out = append(out, grant)
		}
	}
	return out
}

func authorityGrantsOverlap(left, right rootproto.AuthorityGrant) bool {
	for _, a := range left.Duties {
		for _, b := range right.Duties {
			if rootproto.DutyKeyEqual(a.Key(), b.Key()) {
				return true
			}
		}
	}
	return false
}

func mergeGrantRetirements(observed, current []rootproto.GrantRetirement) []rootproto.GrantRetirement {
	out := append([]rootproto.GrantRetirement(nil), observed...)
	seen := make(map[string]struct{}, len(out))
	for _, retirement := range out {
		seen[retirement.GrantID] = struct{}{}
	}
	for _, retirement := range current {
		if _, ok := seen[retirement.GrantID]; ok {
			continue
		}
		seen[retirement.GrantID] = struct{}{}
		out = append(out, retirement)
	}
	return out
}

func mergeGrantInheritances(observed, current []rootproto.GrantInheritance) []rootproto.GrantInheritance {
	out := append([]rootproto.GrantInheritance(nil), observed...)
	seen := make(map[string]struct{}, len(out))
	for _, inheritance := range out {
		seen[inheritance.PredecessorGrantID] = struct{}{}
	}
	for _, inheritance := range current {
		if _, ok := seen[inheritance.PredecessorGrantID]; ok {
			continue
		}
		seen[inheritance.PredecessorGrantID] = struct{}{}
		out = append(out, inheritance)
	}
	return out
}

func SnapshotFromRoot(snapshot rootstate.Snapshot) Snapshot {
	return Snapshot{
		ClusterEpoch: snapshot.State.ClusterEpoch,
		RootToken: rootstorage.TailToken{
			Cursor:   snapshot.State.LastCommitted,
			Revision: 0,
		},
		CatchUpState:        CatchUpStateFresh,
		Stores:              rootstate.CloneStoreMemberships(snapshot.Stores),
		SnapshotEpochs:      rootstate.CloneSnapshotEpochs(snapshot.SnapshotEpochs),
		Mounts:              rootstate.CloneMounts(snapshot.Mounts),
		Subtrees:            rootstate.CloneSubtreeAuthorities(snapshot.Subtrees),
		Quotas:              rootstate.CloneQuotaFences(snapshot.Quotas),
		Descriptors:         rootstate.CloneDescriptors(snapshot.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(snapshot.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(snapshot.PendingRangeChanges),
		Allocator: AllocatorState{
			IDCurrent: snapshot.State.IDFence,
			TSCurrent: snapshot.State.TSOFence,
		},
		ActiveGrants:      cloneAuthorityGrants(snapshot.State.ActiveGrants),
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), snapshot.State.RetiredGrants...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), snapshot.State.GrantInheritances...),
		RetiredEraFloor:   snapshot.State.RetiredEraFloor,
	}
}

func (s Snapshot) RootSnapshot() rootstate.Snapshot {
	return rootstate.Snapshot{
		State: rootstate.State{
			ClusterEpoch:      s.ClusterEpoch,
			LastCommitted:     s.RootToken.Cursor,
			IDFence:           s.Allocator.IDCurrent,
			TSOFence:          s.Allocator.TSCurrent,
			ActiveGrants:      cloneAuthorityGrants(s.ActiveGrants),
			RetiredGrants:     append([]rootproto.GrantRetirement(nil), s.RetiredGrants...),
			GrantInheritances: append([]rootproto.GrantInheritance(nil), s.GrantInheritances...),
			RetiredEraFloor:   s.RetiredEraFloor,
		},
		Stores:              rootstate.CloneStoreMemberships(s.Stores),
		SnapshotEpochs:      rootstate.CloneSnapshotEpochs(s.SnapshotEpochs),
		Mounts:              rootstate.CloneMounts(s.Mounts),
		Subtrees:            rootstate.CloneSubtreeAuthorities(s.Subtrees),
		Quotas:              rootstate.CloneQuotaFences(s.Quotas),
		Descriptors:         rootstate.CloneDescriptors(s.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(s.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(s.PendingRangeChanges),
	}
}

func cloneAuthorityGrants(grants []rootproto.AuthorityGrant) []rootproto.AuthorityGrant {
	if len(grants) == 0 {
		return nil
	}
	out := make([]rootproto.AuthorityGrant, len(grants))
	for i, grant := range grants {
		grant.Duties = append([]rootproto.DutyGrant(nil), grant.Duties...)
		grant.PredecessorRetirements = append([]rootproto.GrantRetirement(nil), grant.PredecessorRetirements...)
		out[i] = grant
	}
	return out
}

func cloneAuthorityGrant(grant rootproto.AuthorityGrant) rootproto.AuthorityGrant {
	grant.Duties = append([]rootproto.DutyGrant(nil), grant.Duties...)
	grant.PredecessorRetirements = append([]rootproto.GrantRetirement(nil), grant.PredecessorRetirements...)
	return grant
}

// SnapshotRetentionFloor returns the oldest active fsmeta snapshot read version
// currently materialized in this root view.
func (s Snapshot) SnapshotRetentionFloor() (uint64, bool) {
	return rootstate.SnapshotRetentionFloor(s.SnapshotEpochs)
}

// SnapshotRetentionIndex returns active fsmeta snapshot read-version floors
// currently materialized in this root view.
func (s Snapshot) SnapshotRetentionIndex() rootstate.SnapshotRetentionIndex {
	return rootstate.SnapshotRetentionIndexFor(s.SnapshotEpochs)
}

// BootstrapInfo captures rooted Coordinator bootstrap results.
type BootstrapInfo struct {
	LoadedRegions int
	IDStart       uint64
	TSStart       uint64
	Snapshot      Snapshot
}

// ResolveAllocatorStarts raises starts to checkpoint+1 when needed.
func ResolveAllocatorStarts(idStart, tsStart uint64, state AllocatorState) (uint64, uint64) {
	nextID := state.IDCurrent
	if nextID < math.MaxUint64 {
		nextID++
	}
	if nextID > idStart {
		idStart = nextID
	}

	nextTS := state.TSCurrent
	if nextTS < math.MaxUint64 {
		nextTS++
	}
	if nextTS > tsStart {
		tsStart = nextTS
	}
	return idStart, tsStart
}

// RestoreDescriptors replays a rooted descriptor catalog into one runtime cluster view.
func RestoreDescriptors(apply func(topology.Descriptor) error, descriptors map[uint64]topology.Descriptor) (int, error) {
	if apply == nil || len(descriptors) == 0 {
		return 0, nil
	}
	ids := make([]uint64, 0, len(descriptors))
	for id := range descriptors {
		if id == 0 {
			continue
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)

	loaded := 0
	for _, id := range ids {
		desc := descriptors[id]
		if desc.RegionID == 0 {
			continue
		}
		if err := apply(desc); err != nil {
			return loaded, err
		}
		loaded++
	}
	return loaded, nil
}

// Bootstrap reconstructs one Coordinator runtime view from rooted durable metadata and
// resolves allocator starts against persisted fences.
func Bootstrap(store RootStorage, apply func(topology.Descriptor) error, idStart, tsStart uint64) (BootstrapInfo, error) {
	if store == nil {
		return BootstrapInfo{IDStart: idStart, TSStart: tsStart}, nil
	}
	snapshot, err := store.Load()
	if err != nil {
		return BootstrapInfo{}, err
	}
	loadedRegions, err := RestoreDescriptors(apply, snapshot.Descriptors)
	if err != nil {
		return BootstrapInfo{}, err
	}
	idStart, tsStart = ResolveAllocatorStarts(idStart, tsStart, snapshot.Allocator)
	return BootstrapInfo{
		LoadedRegions: loadedRegions,
		IDStart:       idStart,
		TSStart:       tsStart,
		Snapshot:      snapshot,
	}, nil
}
