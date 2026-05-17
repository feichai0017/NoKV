// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package protocol

import "slices"

// VisibleAuthorityScope is the root-facing scope for one visible namespace
// authority. Empty slice dimensions are wildcards owned by the grant.
type VisibleAuthorityScope struct {
	MountID    string
	MountKeyID uint64
	Buckets    []uint16
	Parents    []uint64
	Inodes     []uint64
}

func (s VisibleAuthorityScope) Valid() bool {
	return s.MountKeyID != 0
}

// VisibleAuthorityGrant is the rooted authority object for fsmeta visible
// holders. It is separate from AuthorityGrant, which grants coordinator service
// duties such as TSO and region lookup.
type VisibleAuthorityGrant struct {
	GrantID           string
	EpochID           uint64
	HolderID          string
	Scope             VisibleAuthorityScope
	ExpiresUnixNano   int64
	PredecessorDigest [32]byte
	QuotaCreditBytes  int64
	QuotaCreditInodes int64
	RootClusterEpoch  uint64
	IssuedRootToken   AuthorityRootToken
}

// VisibleAuthoritySeal is rooted evidence that one visible segment for an
// authority grant has been installed through raftstore. Segment bytes remain
// in raftstore's catalog; root records only the digest frontier.
type VisibleAuthoritySeal struct {
	GrantID              string
	EpochID              uint64
	HolderID             string
	Scope                VisibleAuthorityScope
	SegmentRoot          [32]byte
	SegmentPayloadDigest [32]byte
	OperationCount       uint64
	EntryCount           uint64
	SealedUnixNano       int64
	InstallRegionID      uint64
	InstallTerm          uint64
	InstallIndex         uint64
	InstallVersion       uint64
}

type VisibleAuthorityAct uint8

const (
	VisibleAuthorityActUnknown VisibleAuthorityAct = iota
	VisibleAuthorityActAcquire
	VisibleAuthorityActRetire
	VisibleAuthorityActSeal
)

type VisibleAuthorityCommand struct {
	Kind              VisibleAuthorityAct
	HolderID          string
	GrantID           string
	Scope             VisibleAuthorityScope
	ExpiresUnixNano   int64
	NowUnixNano       int64
	PredecessorDigest [32]byte
	QuotaCreditBytes  int64
	QuotaCreditInodes int64

	SegmentRoot          [32]byte
	SegmentPayloadDigest [32]byte
	OperationCount       uint64
	EntryCount           uint64
	InstallRegionID      uint64
	InstallTerm          uint64
	InstallIndex         uint64
	InstallVersion       uint64
}

func (g VisibleAuthorityGrant) Valid() bool {
	return g.GrantID != "" &&
		g.EpochID != 0 &&
		g.HolderID != "" &&
		g.Scope.Valid() &&
		g.ExpiresUnixNano > 0
}

func (s VisibleAuthoritySeal) Valid() bool {
	var zero [32]byte
	return s.GrantID != "" &&
		s.EpochID != 0 &&
		s.HolderID != "" &&
		s.Scope.Valid() &&
		s.SegmentRoot != zero &&
		s.SegmentPayloadDigest != zero &&
		s.InstallRegionID != 0 &&
		s.InstallTerm != 0 &&
		s.InstallIndex != 0 &&
		s.InstallVersion != 0 &&
		s.SealedUnixNano > 0
}

func (g VisibleAuthorityGrant) ActiveAt(nowUnixNano int64) bool {
	return g.ExpiresUnixNano > 0 && nowUnixNano < g.ExpiresUnixNano
}

func (g VisibleAuthorityGrant) Covers(scope VisibleAuthorityScope, nowUnixNano int64) bool {
	if !g.Valid() || !g.ActiveAt(nowUnixNano) {
		return false
	}
	if scope.MountKeyID == 0 || g.Scope.MountKeyID != scope.MountKeyID {
		return false
	}
	if !visibleSubsetUint16(g.Scope.Buckets, scope.Buckets, false) {
		return false
	}
	if !visibleSubsetUint64(g.Scope.Parents, scope.Parents, true) {
		return false
	}
	return visibleSubsetUint64(g.Scope.Inodes, scope.Inodes, true)
}

// Overlaps reports whether two active grants might both admit one key. Visible
// authority v1 treats bucket overlap inside a mount as conflicting; finer parent/inode
// disambiguation can be added after root has explicit non-overlap proofs.
func (g VisibleAuthorityGrant) Overlaps(other VisibleAuthorityGrant) bool {
	if !g.Valid() || !other.Valid() || g.Scope.MountKeyID != other.Scope.MountKeyID {
		return false
	}
	return visibleOverlapsUint16(g.Scope.Buckets, other.Scope.Buckets)
}

func CloneVisibleAuthorityGrant(grant VisibleAuthorityGrant) VisibleAuthorityGrant {
	grant.Scope = CloneVisibleAuthorityScope(grant.Scope)
	return grant
}

func CloneVisibleAuthoritySeal(seal VisibleAuthoritySeal) VisibleAuthoritySeal {
	seal.Scope = CloneVisibleAuthorityScope(seal.Scope)
	return seal
}

func CloneVisibleAuthorityScope(scope VisibleAuthorityScope) VisibleAuthorityScope {
	return VisibleAuthorityScope{
		MountID:    scope.MountID,
		MountKeyID: scope.MountKeyID,
		Buckets:    append([]uint16(nil), scope.Buckets...),
		Parents:    append([]uint64(nil), scope.Parents...),
		Inodes:     append([]uint64(nil), scope.Inodes...),
	}
}

func visibleSubsetUint16(grant, requested []uint16, emptyRequestCovered bool) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return emptyRequestCovered
	}
	for _, value := range requested {
		if !visibleContainsUint16(grant, value) {
			return false
		}
	}
	return true
}

func visibleSubsetUint64(grant, requested []uint64, emptyRequestCovered bool) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return emptyRequestCovered
	}
	for _, value := range requested {
		if !visibleContainsUint64(grant, value) {
			return false
		}
	}
	return true
}

func visibleOverlapsUint16(left, right []uint16) bool {
	if len(left) == 0 || len(right) == 0 {
		return true
	}
	for _, value := range right {
		if visibleContainsUint16(left, value) {
			return true
		}
	}
	return false
}

func visibleContainsUint16(values []uint16, target uint16) bool {
	return slices.Contains(values, target)
}

func visibleContainsUint64(values []uint64, target uint64) bool {
	return slices.Contains(values, target)
}
