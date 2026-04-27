// Package state holds the compact applied root state of the metadata
// kernel (State, Tenure/Legacy/Handover, pending peer/range
// changes) and the ApplyEventToState / ApplyEventToSnapshot functions
// that drive a rooted event log into that state.
//
// This package is the only place where the meaning of a typed rooted
// event is codified. meta/root/replicated persists the events through a
// 3-peer raft quorum; callers under meta/root/server, meta/root/client,
// coordinator/, and raftstore/ consume the resulting State as truth.
//
// See docs/rooted_truth.md for the overall kernel design and
// docs/spec/Eunomia.tla for the formal authority-handoff model.
package state

import (
	"fmt"
	"maps"

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
	ClusterEpoch    uint64
	MembershipEpoch uint64
	LastCommitted   Cursor
	IDFence         uint64
	TSOFence        uint64
	Tenure          Tenure
	Legacy          Legacy
	Handover        Handover
}

// Tenure is the compact control-plane owner lease stored in root
// truth. It is separate from raft leadership: it gates coordinator-only duties
// such as TSO, ID allocation, and scheduler ownership in separated deployments.
type Tenure struct {
	HolderID        string
	ExpiresUnixNano int64
	Era             uint64
	IssuedAt        Cursor
	Mandate         uint32
	LineageDigest   string
}

type Legacy struct {
	HolderID  string
	Era       uint64
	Mandate   uint32
	Frontiers rootproto.MandateFrontiers
	SealedAt  Cursor
}

type Handover struct {
	HolderID     string
	LegacyEra    uint64
	SuccessorEra uint64
	LegacyDigest string
	Stage        rootproto.HandoverStage
	ConfirmedAt  Cursor
	ClosedAt     Cursor
	ReattachedAt Cursor
}

type StoreMembershipState uint8

const (
	StoreMembershipUnknown StoreMembershipState = iota
	StoreMembershipActive
	StoreMembershipRetired
)

// StoreMembership is the rooted membership truth for one store ID. Runtime
// liveness and network addresses are intentionally kept out of this record.
type StoreMembership struct {
	StoreID   uint64
	State     StoreMembershipState
	JoinedAt  Cursor
	RetiredAt Cursor
}

type MountState uint8

const (
	MountStateUnknown MountState = iota
	MountStateActive
	MountStateRetired
)

// MountRecord is rooted truth for one fsmeta mount. Runtime fsmeta sessions
// and cache state are intentionally excluded.
type MountRecord struct {
	MountID       string
	RootInode     uint64
	SchemaVersion uint32
	State         MountState
	RegisteredAt  Cursor
	RetiredAt     Cursor
}

type SubtreeAuthorityState uint8

const (
	SubtreeAuthorityUnknown SubtreeAuthorityState = iota
	SubtreeAuthorityActive
	SubtreeAuthorityHandoff
)

// SubtreeAuthority is rooted truth for one subtree authority era. The record is
// keyed by (mount, root inode); dentry mutations remain data-plane writes.
type SubtreeAuthority struct {
	SubtreeID              string
	Mount                  string
	RootInode              uint64
	AuthorityID            string
	Era                    uint64
	Frontier               uint64
	State                  SubtreeAuthorityState
	DeclaredAt             Cursor
	HandoffStartedAt       Cursor
	HandoffCompletedAt     Cursor
	PredecessorAuthorityID string
	PredecessorEra         uint64
	PredecessorFrontier    uint64
	SuccessorAuthorityID   string
	SuccessorEra           uint64
	InheritedFrontier      uint64
}

// QuotaFence is rooted quota truth for one mount or subtree. RootInode 0 means
// mount-wide.
type QuotaFence struct {
	SubjectID   string
	Mount       string
	RootInode   uint64
	LimitBytes  uint64
	LimitInodes uint64
	Era         uint64
	Frontier    uint64
	UpdatedAt   Cursor
}

func SubtreeAuthorityKey(mount string, rootInode uint64) string {
	if mount == "" || rootInode == 0 {
		return ""
	}
	return fmt.Sprintf("%s/%d", mount, rootInode)
}

func QuotaFenceKey(mount string, rootInode uint64) string {
	return rootevent.QuotaFenceID(mount, rootInode)
}

func SubtreeAuthorityID(mount string, rootInode, era uint64) string {
	if mount == "" || rootInode == 0 {
		return ""
	}
	return fmt.Sprintf("%s/%d#%d", mount, rootInode, era)
}

func (l Tenure) ActiveAt(nowUnixNano int64) bool {
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
	Stores              map[uint64]StoreMembership
	SnapshotEpochs      map[string]SnapshotEpoch
	Mounts              map[string]MountRecord
	Subtrees            map[string]SubtreeAuthority
	Quotas              map[string]QuotaFence
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
		Stores:              CloneStoreMemberships(snapshot.Stores),
		SnapshotEpochs:      CloneSnapshotEpochs(snapshot.SnapshotEpochs),
		Mounts:              CloneMounts(snapshot.Mounts),
		Subtrees:            CloneSubtreeAuthorities(snapshot.Subtrees),
		Quotas:              CloneQuotaFences(snapshot.Quotas),
		Descriptors:         CloneDescriptors(snapshot.Descriptors),
		PendingPeerChanges:  ClonePendingPeerChanges(snapshot.PendingPeerChanges),
		PendingRangeChanges: ClonePendingRangeChanges(snapshot.PendingRangeChanges),
	}
	return out
}

func CloneMounts(in map[string]MountRecord) map[string]MountRecord {
	if len(in) == 0 {
		return make(map[string]MountRecord)
	}
	out := make(map[string]MountRecord, len(in))
	maps.Copy(out, in)
	return out
}

func CloneSubtreeAuthorities(in map[string]SubtreeAuthority) map[string]SubtreeAuthority {
	if in == nil {
		return nil
	}
	if len(in) == 0 {
		return make(map[string]SubtreeAuthority)
	}
	out := make(map[string]SubtreeAuthority, len(in))
	maps.Copy(out, in)
	return out
}

func CloneQuotaFences(in map[string]QuotaFence) map[string]QuotaFence {
	if in == nil {
		return nil
	}
	if len(in) == 0 {
		return make(map[string]QuotaFence)
	}
	out := make(map[string]QuotaFence, len(in))
	maps.Copy(out, in)
	return out
}

type SnapshotEpoch struct {
	SnapshotID  string
	Mount       string
	RootInode   uint64
	ReadVersion uint64
	PublishedAt Cursor
}

func CloneSnapshotEpochs(in map[string]SnapshotEpoch) map[string]SnapshotEpoch {
	if len(in) == 0 {
		return make(map[string]SnapshotEpoch)
	}
	out := make(map[string]SnapshotEpoch, len(in))
	maps.Copy(out, in)
	return out
}

func CloneStoreMemberships(in map[uint64]StoreMembership) map[uint64]StoreMembership {
	if len(in) == 0 {
		return make(map[uint64]StoreMembership)
	}
	out := make(map[uint64]StoreMembership, len(in))
	maps.Copy(out, in)
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
	case rootevent.KindStoreJoined, rootevent.KindStoreRetired:
		if event.StoreMembership != nil && event.StoreMembership.StoreID != 0 {
			state.MembershipEpoch++
		}
	case rootevent.KindIDAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > state.IDFence {
			state.IDFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindTSOAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > state.TSOFence {
			state.TSOFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindTenure:
		applyTenureToState(state, cursor, event)
	case rootevent.KindLegacy:
		applyLegacyToState(state, cursor, event)
	case rootevent.KindHandover:
		applyHandoverToState(state, cursor, event)
	case rootevent.KindSnapshotEpochPublished,
		rootevent.KindSnapshotEpochRetired,
		rootevent.KindMountRegistered,
		rootevent.KindMountRetired,
		rootevent.KindSubtreeAuthorityDeclared,
		rootevent.KindSubtreeHandoffStarted,
		rootevent.KindSubtreeHandoffCompleted,
		rootevent.KindQuotaFenceUpdated:
		// Filesystem namespace authority events advance the root cursor but do
		// not mutate cluster topology or store membership epochs.
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

func applyLegacyToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.Legacy == nil {
		return
	}
	seal := event.Legacy
	sealedAt := coalesceCursor(seal.SealedAt, cursor)
	mandate := seal.Mandate
	if mandate == 0 {
		mandate = state.Tenure.Mandate
		if mandate == 0 {
			mandate = rootproto.MandateDefault
		}
	}
	state.Legacy = Legacy{
		HolderID:  seal.HolderID,
		Era:       seal.Era,
		Mandate:   mandate,
		Frontiers: seal.Frontiers,
		SealedAt:  sealedAt,
	}
	state.Handover = Handover{}
}

func applyHandoverToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.Handover == nil {
		return
	}
	handover := event.Handover
	confirmedAt := coalesceCursor(handover.ConfirmedAt, cursor)
	closedAt := coalesceCursor(handover.ClosedAt, cursor)
	reattachedAt := coalesceCursor(handover.ReattachedAt, cursor)
	state.Handover = Handover{
		HolderID:     handover.HolderID,
		LegacyEra:    handover.LegacyEra,
		SuccessorEra: handover.SuccessorEra,
		LegacyDigest: handover.LegacyDigest,
		Stage:        handover.Stage,
		ConfirmedAt:  confirmedAt,
		ClosedAt:     closedAt,
		ReattachedAt: reattachedAt,
	}
}

func applyTenureToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.Tenure == nil {
		return
	}
	lease := event.Tenure
	issuedAt := state.Tenure.IssuedAt
	issuedAt = coalesceCursor(lease.IssuedAt, issuedAt)
	if issuedAt.Term == 0 && issuedAt.Index == 0 {
		issuedAt = cursor
	}
	if lease.Era == 0 || lease.Era != state.Tenure.Era {
		issuedAt = cursor
	}
	mandate := lease.Mandate
	if mandate == 0 {
		mandate = rootproto.MandateDefault
	}
	lineageDigest := lease.LineageDigest
	if lineageDigest == "" && lease.Era == state.Tenure.Era {
		lineageDigest = state.Tenure.LineageDigest
	}
	state.Tenure = Tenure{
		HolderID:        lease.HolderID,
		ExpiresUnixNano: lease.ExpiresUnixNano,
		Era:             lease.Era,
		IssuedAt:        issuedAt,
		Mandate:         mandate,
		LineageDigest:   lineageDigest,
	}
	frontiers := lease.Frontiers
	if frontier := frontiers.Frontier(rootproto.MandateAllocID); frontier > state.IDFence {
		state.IDFence = frontier
	}
	if frontier := frontiers.Frontier(rootproto.MandateTSO); frontier > state.TSOFence {
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
