// Package capsule contains the execution-side pieces of the Capsule protocol.
package capsule

import (
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

var (
	ErrInvalidGrant       = errors.New("fsmeta capsule: invalid authority grant")
	ErrAmbiguousAuthority = errors.New("fsmeta capsule: ambiguous active authority")
	ErrConflictingGrant   = errors.New("fsmeta capsule: conflicting authority grant")
)

// AuthorityGrant is the execution-side alias for the root-issued fsmeta
// Capsule authority grant. The rooted protocol owns grant semantics; this
// package only adapts fsmeta compiler scopes into that protocol type.
type AuthorityGrant = rootproto.CapsuleAuthorityGrant

type ActiveAuthorities struct {
	mu     sync.RWMutex
	grants map[string]AuthorityGrant
}

func NewActiveAuthorities() *ActiveAuthorities {
	return &ActiveAuthorities{grants: make(map[string]AuthorityGrant)}
}

// Replace installs one root-observed active grant snapshot. Invalid entries are
// rejected instead of being partially installed, so callers never mix two root
// views in one table.
func (a *ActiveAuthorities) Replace(grants []AuthorityGrant) error {
	next := make(map[string]AuthorityGrant, len(grants))
	ordered := make([]AuthorityGrant, 0, len(grants))
	for _, grant := range grants {
		if !grant.Valid() {
			return ErrInvalidGrant
		}
		if _, ok := next[grant.GrantID]; ok {
			return ErrInvalidGrant
		}
		if slices.ContainsFunc(ordered, grant.Overlaps) {
			return ErrConflictingGrant
		}
		next[grant.GrantID] = cloneGrant(grant)
		ordered = append(ordered, grant)
	}
	a.mu.Lock()
	a.grants = next
	a.mu.Unlock()
	return nil
}

func (a *ActiveAuthorities) Snapshot() []AuthorityGrant {
	if a == nil {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]AuthorityGrant, 0, len(a.grants))
	for _, grant := range a.grants {
		out = append(out, cloneGrant(grant))
	}
	slices.SortFunc(out, func(left, right AuthorityGrant) int {
		if left.GrantID < right.GrantID {
			return -1
		}
		if left.GrantID > right.GrantID {
			return 1
		}
		return 0
	})
	return out
}

func (a *ActiveAuthorities) Find(scope compile.AuthorityScope, now time.Time) (AuthorityGrant, bool, error) {
	if a == nil {
		return AuthorityGrant{}, false, nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	var found AuthorityGrant
	rootScope := AuthorityScopeFromDelta(scope)
	nowUnixNano := now.UnixNano()
	for _, grant := range a.grants {
		if !grant.Covers(rootScope, nowUnixNano) {
			continue
		}
		if found.Valid() {
			return AuthorityGrant{}, false, ErrAmbiguousAuthority
		}
		found = grant
	}
	if !found.Valid() {
		return AuthorityGrant{}, false, nil
	}
	return cloneGrant(found), true, nil
}

func (a *ActiveAuthorities) HolderFor(scope compile.AuthorityScope, now time.Time) (string, bool, error) {
	grant, ok, err := a.Find(scope, now)
	if err != nil || !ok {
		return "", ok, err
	}
	return grant.HolderID, true, nil
}

func (a *ActiveAuthorities) HeldBy(holderID string, scope compile.AuthorityScope, now time.Time) (bool, error) {
	if holderID == "" {
		return false, nil
	}
	grant, ok, err := a.Find(scope, now)
	if err != nil || !ok {
		return false, err
	}
	return grant.HolderID == holderID, nil
}

func cloneGrant(grant AuthorityGrant) AuthorityGrant {
	return rootproto.CloneCapsuleAuthorityGrant(grant)
}

func AuthorityScopeFromDelta(scope compile.AuthorityScope) rootproto.CapsuleAuthorityScope {
	return rootproto.CapsuleAuthorityScope{
		MountID:    string(scope.Mount),
		MountKeyID: uint64(scope.MountKeyID),
		Buckets:    capsuleBucketsFromDelta(scope.Buckets),
		Parents:    capsuleInodesFromDelta(scope.Parents),
		Inodes:     capsuleInodesFromDelta(scope.Inodes),
	}
}

func capsuleBucketsFromDelta(buckets []fsmeta.AffinityBucket) []uint16 {
	out := make([]uint16, len(buckets))
	for i, bucket := range buckets {
		out[i] = uint16(bucket)
	}
	return out
}

func capsuleInodesFromDelta(inodes []fsmeta.InodeID) []uint64 {
	out := make([]uint64, len(inodes))
	for i, inode := range inodes {
		out[i] = uint64(inode)
	}
	return out
}
