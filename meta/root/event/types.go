package event

import (
	"fmt"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/meta/topology"
)

// Kind identifies one globally ordered metadata mutation.
type Kind uint16

const (
	KindUnknown Kind = iota
	KindStoreJoined
	KindStoreRetired
	KindIDAllocatorFenced
	KindTSOAllocatorFenced
	KindRegionBootstrap
	KindRegionDescriptorPublished
	KindRegionTombstoned
	KindRegionSplitPlanned
	KindRegionSplitCommitted
	KindRegionSplitCancelled
	KindRegionMergePlanned
	KindRegionMerged
	KindRegionMergeCancelled
	KindPeerAdditionPlanned
	KindPeerRemovalPlanned
	KindPeerAdded
	KindPeerRemoved
	KindPeerAdditionCancelled
	KindPeerRemovalCancelled
	KindSnapshotEpochPublished
	KindSnapshotEpochRetired
	KindMountRegistered
	KindMountRetired
	KindSubtreeAuthorityDeclared
	KindSubtreeHandoffStarted
	KindSubtreeHandoffCompleted
	KindQuotaFenceUpdated
	KindGrantIssued
	KindGrantSealed
	KindGrantRetired
	KindGrantInherited
	KindCapsuleAuthorityGranted
	KindCapsuleAuthorityRetired
)

// StoreMembership describes one store membership change carried by a root event.
type StoreMembership struct {
	StoreID uint64
}

// SnapshotEpoch publishes one fsmeta subtree MVCC read epoch into rooted truth.
// It is an authority/retention claim, not a materialized filesystem snapshot.
type SnapshotEpoch struct {
	SnapshotID  string
	Mount       string
	MountKeyID  uint64
	RootInode   uint64
	ReadVersion uint64
	PublishedAt RootCursor
}

// Mount records one filesystem metadata mount lifecycle event.
type Mount struct {
	MountID       string
	MountKeyID    uint64
	RootInode     uint64
	SchemaVersion uint32
	RegisteredAt  RootCursor
	RetiredAt     RootCursor
}

// SubtreeAuthority records one rooted subtree authority event. Declare events
// use AuthorityID/Era/Frontier. Handoff-start events use Frontier as the
// predecessor frontier. Handoff-complete events use InheritedFrontier as the
// successor coverage frontier.
type SubtreeAuthority struct {
	SubtreeID              string
	Mount                  string
	RootInode              uint64
	AuthorityID            string
	Era                    uint64
	Frontier               uint64
	PredecessorAuthorityID string
	PredecessorEra         uint64
	PredecessorFrontier    uint64
	SuccessorAuthorityID   string
	SuccessorEra           uint64
	InheritedFrontier      uint64
	DeclaredAt             RootCursor
	HandoffStartedAt       RootCursor
	HandoffCompletedAt     RootCursor
}

// QuotaFence records one rooted quota limit for a mount or subtree. RootInode
// 0 means mount-wide. Zero limits mean "unlimited" for that dimension.
type QuotaFence struct {
	SubjectID   string
	Mount       string
	RootInode   uint64
	LimitBytes  uint64
	LimitInodes uint64
	Era         uint64
	Frontier    uint64
	UpdatedAt   RootCursor
}

// AllocatorFence raises one rooted allocator floor monotonically.
type AllocatorFence struct {
	Minimum uint64
}

type RootCursor = rootproto.Cursor

// RegionDescriptorRecord carries one descriptor snapshot into the root log.
type RegionDescriptorRecord struct {
	Descriptor topology.Descriptor
}

// RegionRemoval removes one region descriptor from the rooted topology view.
type RegionRemoval struct {
	RegionID uint64
}

// RangeSplit describes one split intent or committed split transition.
type RangeSplit struct {
	ParentRegionID uint64
	SplitKey       []byte
	Left           topology.Descriptor
	Right          topology.Descriptor
	BaseParent     topology.Descriptor
}

// RangeMerge describes one merge transition.
type RangeMerge struct {
	LeftRegionID  uint64
	RightRegionID uint64
	Merged        topology.Descriptor
	BaseLeft      topology.Descriptor
	BaseRight     topology.Descriptor
}

// PeerChange describes one region membership mutation.
type PeerChange struct {
	RegionID uint64
	StoreID  uint64
	PeerID   uint64
	Region   topology.Descriptor
	Base     topology.Descriptor
}

// Event is one globally ordered metadata-root mutation.
type Event struct {
	Kind Kind

	StoreMembership  *StoreMembership
	AllocatorFence   *AllocatorFence
	Grant            *rootproto.AuthorityGrant
	GrantRetirement  *rootproto.GrantRetirement
	GrantInheritance *rootproto.GrantInheritance
	CapsuleGrant     *rootproto.CapsuleAuthorityGrant
	SnapshotEpoch    *SnapshotEpoch
	Mount            *Mount
	SubtreeAuthority *SubtreeAuthority
	QuotaFence       *QuotaFence
	RegionDescriptor *RegionDescriptorRecord
	RegionRemoval    *RegionRemoval
	RangeSplit       *RangeSplit
	RangeMerge       *RangeMerge
	PeerChange       *PeerChange
}

func GrantIssued(grant rootproto.AuthorityGrant) Event {
	return Event{Kind: KindGrantIssued, Grant: &grant}
}

func GrantSealed(retirement rootproto.GrantRetirement) Event {
	retirement.Mode = rootproto.GrantRetirementSealedExact
	return Event{Kind: KindGrantSealed, GrantRetirement: &retirement}
}

func GrantRetired(retirement rootproto.GrantRetirement) Event {
	if retirement.Mode == rootproto.GrantRetirementUnspecified {
		retirement.Mode = rootproto.GrantRetirementExpiredBound
	}
	return Event{Kind: KindGrantRetired, GrantRetirement: &retirement}
}

func GrantInherited(inheritance rootproto.GrantInheritance) Event {
	return Event{Kind: KindGrantInherited, GrantInheritance: &inheritance}
}

func CapsuleAuthorityGranted(grant rootproto.CapsuleAuthorityGrant) Event {
	grant = rootproto.CloneCapsuleAuthorityGrant(grant)
	return Event{Kind: KindCapsuleAuthorityGranted, CapsuleGrant: &grant}
}

func CapsuleAuthorityRetired(grant rootproto.CapsuleAuthorityGrant) Event {
	grant = rootproto.CloneCapsuleAuthorityGrant(grant)
	return Event{Kind: KindCapsuleAuthorityRetired, CapsuleGrant: &grant}
}

func MountRegistered(mountID string, mountKeyID, rootInode uint64, schemaVersion uint32) Event {
	return Event{
		Kind: KindMountRegistered,
		Mount: &Mount{
			MountID:       mountID,
			MountKeyID:    mountKeyID,
			RootInode:     rootInode,
			SchemaVersion: schemaVersion,
		},
	}
}

func MountRetired(mountID string) Event {
	return Event{
		Kind:  KindMountRetired,
		Mount: &Mount{MountID: mountID},
	}
}

func SubtreeAuthorityDeclared(mount string, rootInode uint64, authorityID string, era, frontier uint64) Event {
	return Event{
		Kind: KindSubtreeAuthorityDeclared,
		SubtreeAuthority: &SubtreeAuthority{
			Mount:       mount,
			RootInode:   rootInode,
			AuthorityID: authorityID,
			Era:         era,
			Frontier:    frontier,
		},
	}
}

func SubtreeHandoffStarted(mount string, rootInode, frontier uint64) Event {
	return Event{
		Kind: KindSubtreeHandoffStarted,
		SubtreeAuthority: &SubtreeAuthority{
			Mount:     mount,
			RootInode: rootInode,
			Frontier:  frontier,
		},
	}
}

func SubtreeHandoffCompleted(mount string, rootInode, inheritedFrontier uint64) Event {
	return Event{
		Kind: KindSubtreeHandoffCompleted,
		SubtreeAuthority: &SubtreeAuthority{
			Mount:             mount,
			RootInode:         rootInode,
			InheritedFrontier: inheritedFrontier,
		},
	}
}

func QuotaFenceID(mount string, rootInode uint64) string {
	if mount == "" {
		return ""
	}
	if rootInode == 0 {
		return mount
	}
	return fmt.Sprintf("%s/%d", mount, rootInode)
}

func QuotaFenceUpdated(mount string, rootInode, limitBytes, limitInodes, era, frontier uint64) Event {
	return Event{
		Kind: KindQuotaFenceUpdated,
		QuotaFence: &QuotaFence{
			SubjectID:   QuotaFenceID(mount, rootInode),
			Mount:       mount,
			RootInode:   rootInode,
			LimitBytes:  limitBytes,
			LimitInodes: limitInodes,
			Era:         era,
			Frontier:    frontier,
		},
	}
}

func StoreJoined(storeID uint64) Event {
	return Event{Kind: KindStoreJoined, StoreMembership: &StoreMembership{StoreID: storeID}}
}

func StoreRetired(storeID uint64) Event {
	return Event{Kind: KindStoreRetired, StoreMembership: &StoreMembership{StoreID: storeID}}
}

func SnapshotEpochID(mount string, rootInode, readVersion uint64) string {
	return fmt.Sprintf("%s/%d/%d", mount, rootInode, readVersion)
}

func SnapshotEpochPublished(mount string, mountKeyID, rootInode, readVersion uint64) Event {
	return Event{
		Kind: KindSnapshotEpochPublished,
		SnapshotEpoch: &SnapshotEpoch{
			SnapshotID:  SnapshotEpochID(mount, rootInode, readVersion),
			Mount:       mount,
			MountKeyID:  mountKeyID,
			RootInode:   rootInode,
			ReadVersion: readVersion,
		},
	}
}

func SnapshotEpochRetired(mount string, mountKeyID, rootInode, readVersion uint64) Event {
	return Event{
		Kind: KindSnapshotEpochRetired,
		SnapshotEpoch: &SnapshotEpoch{
			SnapshotID:  SnapshotEpochID(mount, rootInode, readVersion),
			Mount:       mount,
			MountKeyID:  mountKeyID,
			RootInode:   rootInode,
			ReadVersion: readVersion,
		},
	}
}

func IDAllocatorFenced(min uint64) Event {
	return Event{Kind: KindIDAllocatorFenced, AllocatorFence: &AllocatorFence{Minimum: min}}
}

func TSOAllocatorFenced(min uint64) Event {
	return Event{Kind: KindTSOAllocatorFenced, AllocatorFence: &AllocatorFence{Minimum: min}}
}

func RegionBootstrapped(desc topology.Descriptor) Event {
	return Event{Kind: KindRegionBootstrap, RegionDescriptor: &RegionDescriptorRecord{Descriptor: desc}}
}

func RegionDescriptorPublished(desc topology.Descriptor) Event {
	return Event{Kind: KindRegionDescriptorPublished, RegionDescriptor: &RegionDescriptorRecord{Descriptor: desc}}
}

func RegionTombstoned(regionID uint64) Event {
	return Event{Kind: KindRegionTombstoned, RegionRemoval: &RegionRemoval{RegionID: regionID}}
}

func RegionSplitPlanned(parentRegionID uint64, splitKey []byte, left, right topology.Descriptor) Event {
	return Event{
		Kind: KindRegionSplitPlanned,
		RangeSplit: &RangeSplit{
			ParentRegionID: parentRegionID,
			SplitKey:       append([]byte(nil), splitKey...),
			Left:           left,
			Right:          right,
		},
	}
}

func RegionSplitCommitted(parentRegionID uint64, splitKey []byte, left, right topology.Descriptor) Event {
	return Event{
		Kind: KindRegionSplitCommitted,
		RangeSplit: &RangeSplit{
			ParentRegionID: parentRegionID,
			SplitKey:       append([]byte(nil), splitKey...),
			Left:           left,
			Right:          right,
		},
	}
}

func RegionSplitCancelled(parentRegionID uint64, splitKey []byte, left, right, base topology.Descriptor) Event {
	return Event{
		Kind: KindRegionSplitCancelled,
		RangeSplit: &RangeSplit{
			ParentRegionID: parentRegionID,
			SplitKey:       append([]byte(nil), splitKey...),
			Left:           left,
			Right:          right,
			BaseParent:     base,
		},
	}
}

func RegionMergePlanned(leftRegionID, rightRegionID uint64, merged topology.Descriptor) Event {
	return Event{
		Kind: KindRegionMergePlanned,
		RangeMerge: &RangeMerge{
			LeftRegionID:  leftRegionID,
			RightRegionID: rightRegionID,
			Merged:        merged,
		},
	}
}

func RegionMerged(leftRegionID, rightRegionID uint64, merged topology.Descriptor) Event {
	return Event{
		Kind: KindRegionMerged,
		RangeMerge: &RangeMerge{
			LeftRegionID:  leftRegionID,
			RightRegionID: rightRegionID,
			Merged:        merged,
		},
	}
}

func RegionMergeCancelled(leftRegionID, rightRegionID uint64, merged, baseLeft, baseRight topology.Descriptor) Event {
	return Event{
		Kind: KindRegionMergeCancelled,
		RangeMerge: &RangeMerge{
			LeftRegionID:  leftRegionID,
			RightRegionID: rightRegionID,
			Merged:        merged,
			BaseLeft:      baseLeft,
			BaseRight:     baseRight,
		},
	}
}

func PeerAdded(regionID, storeID, peerID uint64, region topology.Descriptor) Event {
	return Event{
		Kind: KindPeerAdded,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
		},
	}
}

func PeerAdditionPlanned(regionID, storeID, peerID uint64, region topology.Descriptor) Event {
	return Event{
		Kind: KindPeerAdditionPlanned,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
		},
	}
}

func PeerAdditionCancelled(regionID, storeID, peerID uint64, region, base topology.Descriptor) Event {
	return Event{
		Kind: KindPeerAdditionCancelled,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
			Base:     base,
		},
	}
}

func PeerRemovalPlanned(regionID, storeID, peerID uint64, region topology.Descriptor) Event {
	return Event{
		Kind: KindPeerRemovalPlanned,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
		},
	}
}

func PeerRemovalCancelled(regionID, storeID, peerID uint64, region, base topology.Descriptor) Event {
	return Event{
		Kind: KindPeerRemovalCancelled,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
			Base:     base,
		},
	}
}

func PeerRemoved(regionID, storeID, peerID uint64, region topology.Descriptor) Event {
	return Event{
		Kind: KindPeerRemoved,
		PeerChange: &PeerChange{
			RegionID: regionID,
			StoreID:  storeID,
			PeerID:   peerID,
			Region:   region,
		},
	}
}
