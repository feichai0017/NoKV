// Package perasauth adapts root-issued Peras authority grants to fsmeta runtime scopes.
package perasauth

import (
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

var (
	ErrInvalidGrant       = errors.New("fsmeta peras: invalid authority grant")
	ErrAmbiguousAuthority = errors.New("fsmeta peras: ambiguous active authority")
	ErrConflictingGrant   = errors.New("fsmeta peras: conflicting authority grant")
)

// AuthorityGrant is the execution-side alias for the root-issued fsmeta
// Peras authority grant. The rooted protocol owns grant semantics; this
// package only adapts fsmeta compiler scopes into that protocol type.
type AuthorityGrant = rootproto.PerasAuthorityGrant

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

func (a *ActiveAuthorities) ApplyRootEvent(event rootevent.Event) error {
	if a == nil || event.PerasGrant == nil {
		return nil
	}
	switch event.Kind {
	case rootevent.KindPerasAuthorityGranted:
		next := a.Snapshot()
		for i, grant := range next {
			if grant.GrantID == event.PerasGrant.GrantID {
				next[i] = cloneGrant(*event.PerasGrant)
				return a.Replace(next)
			}
		}
		next = append(next, cloneGrant(*event.PerasGrant))
		return a.Replace(next)
	case rootevent.KindPerasAuthorityRetired:
		next := a.Snapshot()
		for i := 0; i < len(next); i++ {
			if next[i].GrantID == event.PerasGrant.GrantID {
				next = append(next[:i], next[i+1:]...)
				i--
			}
		}
		return a.Replace(next)
	default:
		return nil
	}
}

func (a *ActiveAuthorities) Find(scope compile.AuthorityScope, now time.Time) (AuthorityGrant, bool, error) {
	if a == nil {
		return AuthorityGrant{}, false, nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	var found AuthorityGrant
	for _, grant := range a.grants {
		if !GrantCoversDelta(grant, scope, now) {
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

func GrantCoversDelta(grant AuthorityGrant, scope compile.AuthorityScope, now time.Time) bool {
	if !grant.Valid() || !grant.ActiveAt(now.UnixNano()) {
		return false
	}
	if scope.MountKeyID == 0 || grant.Scope.MountKeyID != uint64(scope.MountKeyID) {
		return false
	}
	if !grantCoversBuckets(grant.Scope.Buckets, scope.Buckets, false) {
		return false
	}
	if !grantCoversInodes(grant.Scope.Parents, scope.Parents, true) {
		return false
	}
	return grantCoversInodes(grant.Scope.Inodes, scope.Inodes, true)
}

func grantCoversBuckets(grant []uint16, requested []fsmeta.AffinityBucket, emptyRequestCovered bool) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return emptyRequestCovered
	}
	for _, value := range requested {
		if !slices.Contains(grant, uint16(value)) {
			return false
		}
	}
	return true
}

func grantCoversInodes(grant []uint64, requested []fsmeta.InodeID, emptyRequestCovered bool) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return emptyRequestCovered
	}
	for _, value := range requested {
		if !slices.Contains(grant, uint64(value)) {
			return false
		}
	}
	return true
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
	return rootproto.ClonePerasAuthorityGrant(grant)
}

func AuthorityScopeFromDelta(scope compile.AuthorityScope) rootproto.PerasAuthorityScope {
	return rootproto.PerasAuthorityScope{
		MountID:    string(scope.Mount),
		MountKeyID: uint64(scope.MountKeyID),
		Buckets:    perasBucketsFromDelta(scope.Buckets),
		Parents:    perasInodesFromDelta(scope.Parents),
		Inodes:     perasInodesFromDelta(scope.Inodes),
	}
}

func perasBucketsFromDelta(buckets []fsmeta.AffinityBucket) []uint16 {
	out := make([]uint16, len(buckets))
	for i, bucket := range buckets {
		out[i] = uint16(bucket)
	}
	return out
}

func perasInodesFromDelta(inodes []fsmeta.InodeID) []uint64 {
	out := make([]uint64, len(inodes))
	for i, inode := range inodes {
		out[i] = uint64(inode)
	}
	return out
}
