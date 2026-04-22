// Package state holds the compact applied root state of the metadata
// kernel (State, CoordinatorLease/Seal/Closure, pending peer/range
// changes) and the ApplyEventToState / ApplyEventToSnapshot functions
// that drive a rooted event log into that state.
//
// This package is the only place where the meaning of a typed rooted
// event is codified. meta/root/replicated persists the events through a
// 3-peer raft quorum; callers under meta/root/server, meta/root/client,
// coordinator/, and raftstore/ consume the resulting State as truth.
//
// See docs/rooted_truth.md for the overall kernel design and
// spec/CCC.tla for the formal authority-handoff model.
package state

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

type Cursor = rootproto.Cursor

// AllocatorKind identifies one globally fenced allocator domain inside rooted
// metadata state.
type AllocatorKind uint8

const (
	AllocatorKindUnknown AllocatorKind = iota
	AllocatorKindID
	AllocatorKindTSO
)

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
	Frontiers      rootproto.CoordinatorDutyFrontiers
	SealedAtCursor Cursor
}

type CoordinatorClosure struct {
	HolderID            string
	SealGeneration      uint64
	SuccessorGeneration uint64
	SealDigest          string
	Stage               rootproto.CoordinatorClosureStage
	ConfirmedAtCursor   Cursor
	ClosedAtCursor      Cursor
	ReattachedAtCursor  Cursor
}

func (l CoordinatorLease) ActiveAt(nowUnixNano int64) bool {
	return l.HolderID != "" && l.ExpiresUnixNano > nowUnixNano
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

func MaxDescriptorRevision(descriptors map[uint64]descriptor.Descriptor) uint64 {
	var maxEpoch uint64
	for _, desc := range descriptors {
		if desc.RootEpoch > maxEpoch {
			maxEpoch = desc.RootEpoch
		}
	}
	return maxEpoch
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
	sealedAt := coalesceCursor(seal.SealedAtCursor, cursor)
	dutyMask := seal.DutyMask
	if dutyMask == 0 {
		dutyMask = state.CoordinatorLease.DutyMask
		if dutyMask == 0 {
			dutyMask = rootproto.CoordinatorDutyMaskDefault
		}
	}
	state.CoordinatorSeal = CoordinatorSeal{
		HolderID:       seal.HolderID,
		CertGeneration: seal.CertGeneration,
		DutyMask:       dutyMask,
		Frontiers:      seal.Frontiers,
		SealedAtCursor: sealedAt,
	}
	state.CoordinatorClosure = CoordinatorClosure{}
}

func applyCoordinatorClosureToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.CoordinatorClosure == nil {
		return
	}
	closure := event.CoordinatorClosure
	confirmedAt := coalesceCursor(closure.ConfirmedAtCursor, cursor)
	closedAt := coalesceCursor(closure.ClosedAtCursor, cursor)
	reattachedAt := coalesceCursor(closure.ReattachedAtCursor, cursor)
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
	issuedCursor = coalesceCursor(lease.IssuedCursor, issuedCursor)
	if issuedCursor.Term == 0 && issuedCursor.Index == 0 {
		issuedCursor = cursor
	}
	if lease.CertGeneration == 0 || lease.CertGeneration != state.CoordinatorLease.CertGeneration {
		issuedCursor = cursor
	}
	dutyMask := lease.DutyMask
	if dutyMask == 0 {
		dutyMask = rootproto.CoordinatorDutyMaskDefault
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
	frontiers := lease.Frontiers
	if frontier := frontiers.Frontier(rootproto.CoordinatorDutyAllocID); frontier > state.IDFence {
		state.IDFence = frontier
	}
	if frontier := frontiers.Frontier(rootproto.CoordinatorDutyTSO); frontier > state.TSOFence {
		state.TSOFence = frontier
	}
}

func coalesceCursor(eventCursor, fallback Cursor) Cursor {
	if eventCursor.Term != 0 || eventCursor.Index != 0 {
		return Cursor{Term: eventCursor.Term, Index: eventCursor.Index}
	}
	return fallback
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
