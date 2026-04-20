package state

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

type Cursor = rootproto.Cursor

// State is the compact checkpointed state of the metadata root.
type State struct {
	ClusterEpoch       uint64
	MembershipEpoch    uint64
	LastCommitted      Cursor
	IDFence            uint64
	TSOFence           uint64
	CoordinatorLease   CoordinatorLease
	CoordinatorSeal    CoordinatorSeal
	CoordinatorClosure CoordinatorClosure
}

// CoordinatorLease is the compact control-plane owner lease stored in root
// truth. It is separate from raft leadership: it gates coordinator-only duties
// such as TSO, ID allocation, and scheduler ownership in separated deployments.
type CoordinatorLease struct {
	HolderID          string
	ExpiresUnixNano   int64
	CertGeneration    uint64
	IssuedCursor      Cursor
	DutyMask          uint32
	PredecessorDigest string
}

type CoordinatorSeal struct {
	HolderID       string
	CertGeneration uint64
	DutyMask       uint32
	Frontiers      CoordinatorDutyFrontiers
	SealedAtCursor Cursor
}

type CoordinatorClosure struct {
	HolderID            string
	SealGeneration      uint64
	SuccessorGeneration uint64
	SealDigest          string
	Stage               CoordinatorClosureStage
	ConfirmedAtCursor   Cursor
	ClosedAtCursor      Cursor
	ReattachedAtCursor  Cursor
}

const (
	CoordinatorDutyAllocID        = rootproto.CoordinatorDutyAllocID
	CoordinatorDutyTSO            = rootproto.CoordinatorDutyTSO
	CoordinatorDutyGetRegionByKey = rootproto.CoordinatorDutyGetRegionByKey
	CoordinatorDutyLeaseStart     = rootproto.CoordinatorDutyLeaseStart
)

const CoordinatorDutyMaskDefault = rootproto.CoordinatorDutyMaskDefault

type CoordinatorDutyFrontier = rootproto.CoordinatorDutyFrontier
type CoordinatorDutyFrontiers = rootproto.CoordinatorDutyFrontiers
type CoordinatorFrontierCoverage = rootproto.CoordinatorFrontierCoverage
type CoordinatorSuccessorCoverageStatus = rootproto.CoordinatorSuccessorCoverageStatus
type AuthorityHandoffRecord = rootproto.AuthorityHandoffRecord
type ContinuationWitness = rootproto.ContinuationWitness
type ClosureWitness = rootproto.ClosureWitness

func (l CoordinatorLease) ActiveAt(nowUnixNano int64) bool {
	return l.HolderID != "" && l.ExpiresUnixNano > nowUnixNano
}

func NewCoordinatorDutyFrontiers(entries ...CoordinatorDutyFrontier) CoordinatorDutyFrontiers {
	return rootproto.NewCoordinatorDutyFrontiers(entries...)
}

func CoordinatorDutyFrontiersFromMap(values map[uint32]uint64) CoordinatorDutyFrontiers {
	return rootproto.CoordinatorDutyFrontiersFromMap(values)
}

func CloneDutyFrontiers(frontiers CoordinatorDutyFrontiers) CoordinatorDutyFrontiers {
	return rootproto.CloneDutyFrontiers(frontiers)
}

func OrderedCoordinatorDutyMasks(dutyMask uint32, frontiers CoordinatorDutyFrontiers) []uint32 {
	return rootproto.OrderedCoordinatorDutyMasks(dutyMask, frontiers)
}

func CoordinatorSealRequiredFrontiers(seal CoordinatorSeal) CoordinatorDutyFrontiers {
	if seal.Frontiers.Len() == 0 {
		return rootproto.NewCoordinatorDutyFrontiers()
	}
	return seal.Frontiers
}

func NewContinuationWitness(dutyMask uint32, certGeneration, consumedFrontier uint64) ContinuationWitness {
	return rootproto.NewContinuationWitness(dutyMask, certGeneration, consumedFrontier)
}

type PendingPeerChangeKind uint8

const (
	PendingPeerChangeUnknown PendingPeerChangeKind = iota
	PendingPeerChangeAddition
	PendingPeerChangeRemoval
)

type PendingPeerChange struct {
	Kind    PendingPeerChangeKind
	StoreID uint64
	PeerID  uint64
	Base    descriptor.Descriptor
	Target  descriptor.Descriptor
}

type PendingRangeChangeKind uint8

const (
	PendingRangeChangeUnknown PendingRangeChangeKind = iota
	PendingRangeChangeSplit
	PendingRangeChangeMerge
)

type PendingRangeChange struct {
	Kind           PendingRangeChangeKind
	ParentRegionID uint64
	LeftRegionID   uint64
	RightRegionID  uint64
	BaseParent     descriptor.Descriptor
	BaseLeft       descriptor.Descriptor
	BaseRight      descriptor.Descriptor
	Left           descriptor.Descriptor
	Right          descriptor.Descriptor
	Merged         descriptor.Descriptor
}

// Snapshot is the compact materialized rooted metadata state used for bounded bootstrap and recovery.
type Snapshot struct {
	State               State
	Descriptors         map[uint64]descriptor.Descriptor
	PendingPeerChanges  map[uint64]PendingPeerChange
	PendingRangeChanges map[uint64]PendingRangeChange
}

// CommitInfo reports one successful root append together with the resulting compact root state.
type CommitInfo struct {
	Cursor Cursor
	State  State
}

func CloneSnapshot(snapshot Snapshot) Snapshot {
	state := snapshot.State
	state.CoordinatorSeal.Frontiers = CloneDutyFrontiers(state.CoordinatorSeal.Frontiers)
	out := Snapshot{
		State:               state,
		Descriptors:         CloneDescriptors(snapshot.Descriptors),
		PendingPeerChanges:  ClonePendingPeerChanges(snapshot.PendingPeerChanges),
		PendingRangeChanges: ClonePendingRangeChanges(snapshot.PendingRangeChanges),
	}
	return out
}

func CloneDescriptors(in map[uint64]descriptor.Descriptor) map[uint64]descriptor.Descriptor {
	if len(in) == 0 {
		return make(map[uint64]descriptor.Descriptor)
	}
	out := make(map[uint64]descriptor.Descriptor, len(in))
	for id, desc := range in {
		out[id] = desc.Clone()
	}
	return out
}

func ClonePendingPeerChanges(in map[uint64]PendingPeerChange) map[uint64]PendingPeerChange {
	if len(in) == 0 {
		return make(map[uint64]PendingPeerChange)
	}
	out := make(map[uint64]PendingPeerChange, len(in))
	for id, change := range in {
		out[id] = PendingPeerChange{
			Kind:    change.Kind,
			StoreID: change.StoreID,
			PeerID:  change.PeerID,
			Base:    change.Base.Clone(),
			Target:  change.Target.Clone(),
		}
	}
	return out
}

func ClonePendingRangeChanges(in map[uint64]PendingRangeChange) map[uint64]PendingRangeChange {
	if len(in) == 0 {
		return make(map[uint64]PendingRangeChange)
	}
	out := make(map[uint64]PendingRangeChange, len(in))
	for id, change := range in {
		out[id] = PendingRangeChange{
			Kind:           change.Kind,
			ParentRegionID: change.ParentRegionID,
			LeftRegionID:   change.LeftRegionID,
			RightRegionID:  change.RightRegionID,
			BaseParent:     change.BaseParent.Clone(),
			BaseLeft:       change.BaseLeft.Clone(),
			BaseRight:      change.BaseRight.Clone(),
			Left:           change.Left.Clone(),
			Right:          change.Right.Clone(),
			Merged:         change.Merged.Clone(),
		}
	}
	return out
}

// ApplyEventToState applies one rooted metadata event into compact root state.
func ApplyEventToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil {
		return
	}
	switch event.Kind {
	case rootevent.KindStoreJoined, rootevent.KindStoreLeft:
		state.MembershipEpoch++
	case rootevent.KindIDAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > state.IDFence {
			state.IDFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindTSOAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > state.TSOFence {
			state.TSOFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindCoordinatorLease:
		applyCoordinatorLeaseToState(state, cursor, event)
	case rootevent.KindCoordinatorSeal:
		applyCoordinatorSealToState(state, cursor, event)
	case rootevent.KindCoordinatorClosure:
		applyCoordinatorClosureToState(state, cursor, event)
	case rootevent.KindRegionBootstrap,
		rootevent.KindRegionDescriptorPublished,
		rootevent.KindRegionTombstoned,
		rootevent.KindRegionSplitPlanned,
		rootevent.KindRegionSplitCommitted,
		rootevent.KindRegionSplitCancelled,
		rootevent.KindRegionMergePlanned,
		rootevent.KindRegionMerged,
		rootevent.KindRegionMergeCancelled,
		rootevent.KindPeerAdditionPlanned,
		rootevent.KindPeerRemovalPlanned,
		rootevent.KindPeerAdded,
		rootevent.KindPeerRemoved,
		rootevent.KindPeerAdditionCancelled,
		rootevent.KindPeerRemovalCancelled:
		state.ClusterEpoch++
	}
	state.LastCommitted = cursor
}

func applyCoordinatorSealToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.CoordinatorSeal == nil {
		return
	}
	seal := event.CoordinatorSeal
	sealedAt := cursor
	if seal.SealedAtCursor.Term != 0 || seal.SealedAtCursor.Index != 0 {
		sealedAt = Cursor{Term: seal.SealedAtCursor.Term, Index: seal.SealedAtCursor.Index}
	}
	dutyMask := seal.DutyMask
	if dutyMask == 0 {
		dutyMask = state.CoordinatorLease.DutyMask
		if dutyMask == 0 {
			dutyMask = CoordinatorDutyMaskDefault
		}
	}
	state.CoordinatorSeal = CoordinatorSeal{
		HolderID:       seal.HolderID,
		CertGeneration: seal.CertGeneration,
		DutyMask:       dutyMask,
		Frontiers:      CloneDutyFrontiers(seal.Frontiers),
		SealedAtCursor: sealedAt,
	}
	state.CoordinatorClosure = CoordinatorClosure{}
}

func applyCoordinatorClosureToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.CoordinatorClosure == nil {
		return
	}
	closure := event.CoordinatorClosure
	confirmedAt := cursor
	if closure.ConfirmedAtCursor.Term != 0 || closure.ConfirmedAtCursor.Index != 0 {
		confirmedAt = Cursor{Term: closure.ConfirmedAtCursor.Term, Index: closure.ConfirmedAtCursor.Index}
	}
	closedAt := cursor
	if closure.ClosedAtCursor.Term != 0 || closure.ClosedAtCursor.Index != 0 {
		closedAt = Cursor{Term: closure.ClosedAtCursor.Term, Index: closure.ClosedAtCursor.Index}
	}
	reattachedAt := cursor
	if closure.ReattachedAtCursor.Term != 0 || closure.ReattachedAtCursor.Index != 0 {
		reattachedAt = Cursor{Term: closure.ReattachedAtCursor.Term, Index: closure.ReattachedAtCursor.Index}
	}
	state.CoordinatorClosure = CoordinatorClosure{
		HolderID:            closure.HolderID,
		SealGeneration:      closure.SealGeneration,
		SuccessorGeneration: closure.SuccessorGeneration,
		SealDigest:          closure.SealDigest,
		Stage:               closure.Stage,
		ConfirmedAtCursor:   confirmedAt,
		ClosedAtCursor:      closedAt,
		ReattachedAtCursor:  reattachedAt,
	}
}

func applyCoordinatorLeaseToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.CoordinatorLease == nil {
		return
	}
	lease := event.CoordinatorLease
	issuedCursor := state.CoordinatorLease.IssuedCursor
	if lease.IssuedCursor.Term != 0 || lease.IssuedCursor.Index != 0 {
		issuedCursor = Cursor{Term: lease.IssuedCursor.Term, Index: lease.IssuedCursor.Index}
	}
	if issuedCursor.Term == 0 && issuedCursor.Index == 0 {
		issuedCursor = cursor
	}
	if lease.CertGeneration == 0 || lease.CertGeneration != state.CoordinatorLease.CertGeneration {
		issuedCursor = cursor
	}
	dutyMask := lease.DutyMask
	if dutyMask == 0 {
		dutyMask = CoordinatorDutyMaskDefault
	}
	predecessorDigest := lease.PredecessorDigest
	if predecessorDigest == "" && lease.CertGeneration == state.CoordinatorLease.CertGeneration {
		predecessorDigest = state.CoordinatorLease.PredecessorDigest
	}
	state.CoordinatorLease = CoordinatorLease{
		HolderID:          lease.HolderID,
		ExpiresUnixNano:   lease.ExpiresUnixNano,
		CertGeneration:    lease.CertGeneration,
		IssuedCursor:      issuedCursor,
		DutyMask:          dutyMask,
		PredecessorDigest: predecessorDigest,
	}
	frontiers := CloneDutyFrontiers(lease.Frontiers)
	if frontier := frontiers.Frontier(CoordinatorDutyAllocID); frontier > state.IDFence {
		state.IDFence = frontier
	}
	if frontier := frontiers.Frontier(CoordinatorDutyTSO); frontier > state.TSOFence {
		state.TSOFence = frontier
	}
}

// NextCursor returns the next ordered root cursor.
func NextCursor(prev Cursor) Cursor {
	term := prev.Term
	if term == 0 {
		term = 1
	}
	return Cursor{Term: term, Index: prev.Index + 1}
}

// CursorAfter reports whether a is ordered strictly after b.
func CursorAfter(a, b Cursor) bool {
	if a.Term != b.Term {
		return a.Term > b.Term
	}
	return a.Index > b.Index
}
