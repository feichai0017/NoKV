package protocol

import "slices"

// CapsuleAuthorityScope is the root-facing scope for one fsmeta Capsule
// authority. Empty slice dimensions are wildcards owned by the grant.
type CapsuleAuthorityScope struct {
	MountID    string
	MountKeyID uint64
	Buckets    []uint16
	Parents    []uint64
	Inodes     []uint64
}

func (s CapsuleAuthorityScope) Valid() bool {
	return s.MountKeyID != 0
}

// CapsuleAuthorityGrant is the rooted authority object for fsmeta Capsule
// holders. It is separate from AuthorityGrant, which grants coordinator service
// duties such as TSO and region lookup.
type CapsuleAuthorityGrant struct {
	GrantID           string
	EpochID           uint64
	HolderID          string
	Scope             CapsuleAuthorityScope
	ExpiresUnixNano   int64
	PredecessorDigest [32]byte
	QuotaCreditBytes  int64
	QuotaCreditInodes int64
}

type CapsuleAuthorityAct uint8

const (
	CapsuleAuthorityActUnknown CapsuleAuthorityAct = iota
	CapsuleAuthorityActAcquire
	CapsuleAuthorityActRetire
)

type CapsuleAuthorityCommand struct {
	Kind              CapsuleAuthorityAct
	HolderID          string
	GrantID           string
	Scope             CapsuleAuthorityScope
	ExpiresUnixNano   int64
	NowUnixNano       int64
	PredecessorDigest [32]byte
	QuotaCreditBytes  int64
	QuotaCreditInodes int64
}

func (g CapsuleAuthorityGrant) Valid() bool {
	return g.GrantID != "" &&
		g.EpochID != 0 &&
		g.HolderID != "" &&
		g.Scope.Valid() &&
		g.ExpiresUnixNano > 0
}

func (g CapsuleAuthorityGrant) ActiveAt(nowUnixNano int64) bool {
	return g.ExpiresUnixNano > 0 && nowUnixNano < g.ExpiresUnixNano
}

func (g CapsuleAuthorityGrant) Covers(scope CapsuleAuthorityScope, nowUnixNano int64) bool {
	if !g.Valid() || !g.ActiveAt(nowUnixNano) {
		return false
	}
	if scope.MountKeyID == 0 || g.Scope.MountKeyID != scope.MountKeyID {
		return false
	}
	if !capsuleSubsetUint16(g.Scope.Buckets, scope.Buckets, false) {
		return false
	}
	if !capsuleSubsetUint64(g.Scope.Parents, scope.Parents, true) {
		return false
	}
	return capsuleSubsetUint64(g.Scope.Inodes, scope.Inodes, true)
}

// Overlaps reports whether two active grants might both admit one key. Capsule
// v1 treats bucket overlap inside a mount as conflicting; finer parent/inode
// disambiguation can be added after root has explicit non-overlap proofs.
func (g CapsuleAuthorityGrant) Overlaps(other CapsuleAuthorityGrant) bool {
	if !g.Valid() || !other.Valid() || g.Scope.MountKeyID != other.Scope.MountKeyID {
		return false
	}
	return capsuleOverlapsUint16(g.Scope.Buckets, other.Scope.Buckets)
}

func CloneCapsuleAuthorityGrant(grant CapsuleAuthorityGrant) CapsuleAuthorityGrant {
	grant.Scope = CloneCapsuleAuthorityScope(grant.Scope)
	return grant
}

func CloneCapsuleAuthorityScope(scope CapsuleAuthorityScope) CapsuleAuthorityScope {
	return CapsuleAuthorityScope{
		MountID:    scope.MountID,
		MountKeyID: scope.MountKeyID,
		Buckets:    append([]uint16(nil), scope.Buckets...),
		Parents:    append([]uint64(nil), scope.Parents...),
		Inodes:     append([]uint64(nil), scope.Inodes...),
	}
}

func capsuleSubsetUint16(grant, requested []uint16, emptyRequestCovered bool) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return emptyRequestCovered
	}
	for _, value := range requested {
		if !capsuleContainsUint16(grant, value) {
			return false
		}
	}
	return true
}

func capsuleSubsetUint64(grant, requested []uint64, emptyRequestCovered bool) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return emptyRequestCovered
	}
	for _, value := range requested {
		if !capsuleContainsUint64(grant, value) {
			return false
		}
	}
	return true
}

func capsuleOverlapsUint16(left, right []uint16) bool {
	if len(left) == 0 || len(right) == 0 {
		return true
	}
	for _, value := range right {
		if capsuleContainsUint16(left, value) {
			return true
		}
	}
	return false
}

func capsuleContainsUint16(values []uint16, target uint16) bool {
	return slices.Contains(values, target)
}

func capsuleContainsUint64(values []uint64, target uint64) bool {
	return slices.Contains(values, target)
}
