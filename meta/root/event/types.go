// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

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
)

// StoreMembership describes one store membership change carried by a root event.
type StoreMembership struct {
	StoreID uint64
}

// SnapshotEpoch publishes one fsmeta subtree MVCC read epoch into rooted truth.
// It is an authority/retention claim, not a materialized filesystem snapshot.
type SnapshotEpoch struct {
	SnapshotID      string
	Mount           string
	MountKeyID      uint64
	RootInode       uint64
	ReadVersion     uint64
	PublishedAt     RootCursor
	RuntimeEvidence []rootproto.SnapshotEvidenceRef
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
	SnapshotEpoch    *SnapshotEpoch
	Mount            *Mount
	SubtreeAuthority *SubtreeAuthority
	QuotaFence       *QuotaFence
	RegionDescriptor *RegionDescriptorRecord
	RegionRemoval    *RegionRemoval
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
	return SnapshotEpochPublishedWithRuntimeEvidence(mount, mountKeyID, rootInode, readVersion, nil)
}

func SnapshotEpochPublishedWithRuntimeEvidence(mount string, mountKeyID, rootInode, readVersion uint64, refs []rootproto.SnapshotEvidenceRef) Event {
	return Event{
		Kind: KindSnapshotEpochPublished,
		SnapshotEpoch: &SnapshotEpoch{
			SnapshotID:      SnapshotEpochID(mount, rootInode, readVersion),
			Mount:           mount,
			MountKeyID:      mountKeyID,
			RootInode:       rootInode,
			ReadVersion:     readVersion,
			RuntimeEvidence: rootproto.CloneSnapshotEvidenceRefs(refs),
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
