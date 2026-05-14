// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package protocol

import "slices"

// PerasAuthorityScope is the root-facing scope for one fsmeta Peras
// authority. Empty slice dimensions are wildcards owned by the grant.
type PerasAuthorityScope struct {
	MountID    string
	MountKeyID uint64
	Buckets    []uint16
	Parents    []uint64
	Inodes     []uint64
}

func (s PerasAuthorityScope) Valid() bool {
	return s.MountKeyID != 0
}

// PerasAuthorityGrant is the rooted authority object for fsmeta Peras
// holders. It is separate from AuthorityGrant, which grants coordinator service
// duties such as TSO and region lookup.
type PerasAuthorityGrant struct {
	GrantID           string
	EpochID           uint64
	HolderID          string
	Scope             PerasAuthorityScope
	ExpiresUnixNano   int64
	PredecessorDigest [32]byte
	QuotaCreditBytes  int64
	QuotaCreditInodes int64
}

// PerasAuthoritySeal is rooted evidence that one Peras segment for an
// authority grant has been installed through raftstore. Segment bytes remain
// in raftstore's catalog; root records only the digest frontier.
type PerasAuthoritySeal struct {
	GrantID              string
	EpochID              uint64
	HolderID             string
	Scope                PerasAuthorityScope
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

type PerasAuthorityAct uint8

const (
	PerasAuthorityActUnknown PerasAuthorityAct = iota
	PerasAuthorityActAcquire
	PerasAuthorityActRetire
	PerasAuthorityActSeal
)

type PerasAuthorityCommand struct {
	Kind              PerasAuthorityAct
	HolderID          string
	GrantID           string
	Scope             PerasAuthorityScope
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

func (g PerasAuthorityGrant) Valid() bool {
	return g.GrantID != "" &&
		g.EpochID != 0 &&
		g.HolderID != "" &&
		g.Scope.Valid() &&
		g.ExpiresUnixNano > 0
}

func (s PerasAuthoritySeal) Valid() bool {
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

func (g PerasAuthorityGrant) ActiveAt(nowUnixNano int64) bool {
	return g.ExpiresUnixNano > 0 && nowUnixNano < g.ExpiresUnixNano
}

func (g PerasAuthorityGrant) Covers(scope PerasAuthorityScope, nowUnixNano int64) bool {
	if !g.Valid() || !g.ActiveAt(nowUnixNano) {
		return false
	}
	if scope.MountKeyID == 0 || g.Scope.MountKeyID != scope.MountKeyID {
		return false
	}
	if !perasSubsetUint16(g.Scope.Buckets, scope.Buckets, false) {
		return false
	}
	if !perasSubsetUint64(g.Scope.Parents, scope.Parents, true) {
		return false
	}
	return perasSubsetUint64(g.Scope.Inodes, scope.Inodes, true)
}

// Overlaps reports whether two active grants might both admit one key. Peras
// v1 treats bucket overlap inside a mount as conflicting; finer parent/inode
// disambiguation can be added after root has explicit non-overlap proofs.
func (g PerasAuthorityGrant) Overlaps(other PerasAuthorityGrant) bool {
	if !g.Valid() || !other.Valid() || g.Scope.MountKeyID != other.Scope.MountKeyID {
		return false
	}
	return perasOverlapsUint16(g.Scope.Buckets, other.Scope.Buckets)
}

func ClonePerasAuthorityGrant(grant PerasAuthorityGrant) PerasAuthorityGrant {
	grant.Scope = ClonePerasAuthorityScope(grant.Scope)
	return grant
}

func ClonePerasAuthoritySeal(seal PerasAuthoritySeal) PerasAuthoritySeal {
	seal.Scope = ClonePerasAuthorityScope(seal.Scope)
	return seal
}

func ClonePerasAuthorityScope(scope PerasAuthorityScope) PerasAuthorityScope {
	return PerasAuthorityScope{
		MountID:    scope.MountID,
		MountKeyID: scope.MountKeyID,
		Buckets:    append([]uint16(nil), scope.Buckets...),
		Parents:    append([]uint64(nil), scope.Parents...),
		Inodes:     append([]uint64(nil), scope.Inodes...),
	}
}

func perasSubsetUint16(grant, requested []uint16, emptyRequestCovered bool) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return emptyRequestCovered
	}
	for _, value := range requested {
		if !perasContainsUint16(grant, value) {
			return false
		}
	}
	return true
}

func perasSubsetUint64(grant, requested []uint64, emptyRequestCovered bool) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return emptyRequestCovered
	}
	for _, value := range requested {
		if !perasContainsUint64(grant, value) {
			return false
		}
	}
	return true
}

func perasOverlapsUint16(left, right []uint16) bool {
	if len(left) == 0 || len(right) == 0 {
		return true
	}
	for _, value := range right {
		if perasContainsUint16(left, value) {
			return true
		}
	}
	return false
}

func perasContainsUint16(values []uint16, target uint16) bool {
	return slices.Contains(values, target)
}

func perasContainsUint64(values []uint64, target uint64) bool {
	return slices.Contains(values, target)
}
