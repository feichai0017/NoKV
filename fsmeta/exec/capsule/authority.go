// Package capsule contains the execution-side pieces of the Capsule protocol.
package capsule

import (
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

var (
	ErrInvalidGrant       = errors.New("fsmeta capsule: invalid authority grant")
	ErrAmbiguousAuthority = errors.New("fsmeta capsule: ambiguous active authority")
	ErrConflictingGrant   = errors.New("fsmeta capsule: conflicting authority grant")
)

// AuthorityGrant is the execution-side view of a root-issued fsmeta authority
// grant. It is intentionally separate from meta/root's coordinator-duty grant:
// Capsule grants cover fsmeta key scopes, not coordinator service duties.
type AuthorityGrant struct {
	GrantID           string
	EpochID           uint64
	HolderID          string
	Scope             compile.AuthorityScope
	ExpiresUnixNano   int64
	PredecessorDigest [32]byte
	QuotaCreditBytes  int64
	QuotaCreditInodes int64
}

func (g AuthorityGrant) ActiveAt(now time.Time) bool {
	if g.ExpiresUnixNano <= 0 {
		return false
	}
	return now.UnixNano() < g.ExpiresUnixNano
}

func (g AuthorityGrant) Valid() bool {
	return g.GrantID != "" &&
		g.EpochID != 0 &&
		g.HolderID != "" &&
		g.Scope.MountKeyID != 0 &&
		g.ExpiresUnixNano > 0
}

func (g AuthorityGrant) Covers(scope compile.AuthorityScope, now time.Time) bool {
	if !g.Valid() || !g.ActiveAt(now) {
		return false
	}
	if scope.MountKeyID == 0 || g.Scope.MountKeyID != scope.MountKeyID {
		return false
	}
	if !subsetBuckets(g.Scope.Buckets, scope.Buckets) {
		return false
	}
	if !subsetInodes(g.Scope.Parents, scope.Parents) {
		return false
	}
	return subsetInodes(g.Scope.Inodes, scope.Inodes)
}

// Overlaps reports whether two grants might cover the same fsmeta storage
// keys. The check is deliberately conservative: overlapping buckets in one
// mount are treated as conflicting even when parent/inode sets look disjoint.
func (g AuthorityGrant) Overlaps(other AuthorityGrant) bool {
	if !g.Valid() || !other.Valid() || g.Scope.MountKeyID != other.Scope.MountKeyID {
		return false
	}
	return bucketsOverlap(g.Scope.Buckets, other.Scope.Buckets)
}

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
		for _, existing := range ordered {
			if grant.Overlaps(existing) {
				return ErrConflictingGrant
			}
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
	for _, grant := range a.grants {
		if !grant.Covers(scope, now) {
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
	grant.Scope = cloneScope(grant.Scope)
	return grant
}

func cloneScope(scope compile.AuthorityScope) compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      scope.Mount,
		MountKeyID: scope.MountKeyID,
		Buckets:    append([]fsmeta.AffinityBucket(nil), scope.Buckets...),
		Parents:    append([]fsmeta.InodeID(nil), scope.Parents...),
		Inodes:     append([]fsmeta.InodeID(nil), scope.Inodes...),
	}
}

func subsetBuckets(grant, requested []fsmeta.AffinityBucket) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return false
	}
	allowed := make(map[fsmeta.AffinityBucket]struct{}, len(grant))
	for _, bucket := range grant {
		allowed[bucket] = struct{}{}
	}
	for _, bucket := range requested {
		if _, ok := allowed[bucket]; !ok {
			return false
		}
	}
	return true
}

func subsetInodes(grant, requested []fsmeta.InodeID) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return true
	}
	allowed := make(map[fsmeta.InodeID]struct{}, len(grant))
	for _, inode := range grant {
		allowed[inode] = struct{}{}
	}
	for _, inode := range requested {
		if _, ok := allowed[inode]; !ok {
			return false
		}
	}
	return true
}

func bucketsOverlap(left, right []fsmeta.AffinityBucket) bool {
	if len(left) == 0 || len(right) == 0 {
		return true
	}
	seen := make(map[fsmeta.AffinityBucket]struct{}, len(left))
	for _, bucket := range left {
		seen[bucket] = struct{}{}
	}
	for _, bucket := range right {
		if _, ok := seen[bucket]; ok {
			return true
		}
	}
	return false
}
