package raftstore

import (
	"context"
	"slices"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

type perasAuthorityUse struct {
	id    uint64
	scope compile.AuthorityScope
}

func (c *RemotePerasCommitter) DrainAuthority(ctx context.Context, retirer fsperas.AuthorityRetirer, scopes ...compile.AuthorityScope) error {
	if c == nil || retirer == nil {
		return errPerasCommitterInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	drainScopes := normalizeAuthorityDrainScopes(scopes)
	endDrain := c.beginAuthorityDrain(drainScopes)
	defer endDrain()
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.commitMu.Lock()
	var batches []perasFlushBatch
	if len(drainScopes) == 1 && authorityScopeEmpty(drainScopes[0]) {
		var err error
		batches, err = c.freezeFlushBatchesLocked(nil, true, 0)
		if err != nil {
			c.commitMu.Unlock()
			return err
		}
	} else {
		for _, scope := range drainScopes {
			scope := scope
			part, err := c.freezeFlushBatchesLocked(&scope, true, 0)
			if err != nil {
				c.commitMu.Unlock()
				return err
			}
			batches = append(batches, part...)
		}
	}
	c.commitMu.Unlock()
	if err := c.installFlushBatches(ctx, batches); err != nil {
		return err
	}
	return c.retireDrainedAuthority(ctx, retirer, scopes...)
}

func (c *RemotePerasCommitter) retireDrainedAuthority(ctx context.Context, retirer fsperas.AuthorityRetirer, scopes ...compile.AuthorityScope) error {
	if err := retirer.RetirePerasAuthority(ctx, scopes...); err != nil {
		return c.recordErrorf("retire peras authority: %w", err)
	}
	return nil
}

func (c *RemotePerasCommitter) enterAuthority(scope compile.AuthorityScope) func() {
	if c == nil || c.drainCond == nil {
		return func() {}
	}
	c.drainMu.Lock()
	for c.authorityDrainBlocksLocked(scope) {
		c.drainCond.Wait()
	}
	c.drainNextID++
	id := c.drainNextID
	c.drainUses = append(c.drainUses, perasAuthorityUse{
		id:    id,
		scope: cloneRuntimeAuthorityScope(scope),
	})
	c.drainMu.Unlock()
	return func() {
		c.leaveAuthority(id)
	}
}

func (c *RemotePerasCommitter) leaveAuthority(id uint64) {
	if c == nil || c.drainCond == nil || id == 0 {
		return
	}
	c.drainMu.Lock()
	for i, use := range c.drainUses {
		if use.id != id {
			continue
		}
		copy(c.drainUses[i:], c.drainUses[i+1:])
		c.drainUses[len(c.drainUses)-1] = perasAuthorityUse{}
		c.drainUses = c.drainUses[:len(c.drainUses)-1]
		break
	}
	c.drainCond.Broadcast()
	c.drainMu.Unlock()
}

func (c *RemotePerasCommitter) beginAuthorityDrain(scopes []compile.AuthorityScope) func() {
	if c == nil || c.drainCond == nil {
		return func() {}
	}
	drainScopes := cloneRuntimeAuthorityScopes(scopes)
	c.drainMu.Lock()
	c.drainScopes = append(c.drainScopes, drainScopes...)
	for c.authorityDrainHasActiveUseLocked(drainScopes) {
		c.drainCond.Wait()
	}
	c.drainMu.Unlock()
	return func() {
		c.endAuthorityDrain(drainScopes)
	}
}

func (c *RemotePerasCommitter) endAuthorityDrain(scopes []compile.AuthorityScope) {
	if c == nil || c.drainCond == nil {
		return
	}
	c.drainMu.Lock()
	for _, scope := range scopes {
		c.removeAuthorityDrainScopeLocked(scope)
	}
	c.drainCond.Broadcast()
	c.drainMu.Unlock()
}

func (c *RemotePerasCommitter) authorityDrainBlocksLocked(scope compile.AuthorityScope) bool {
	for _, drain := range c.drainScopes {
		if authorityDrainScopesOverlap(scope, drain) {
			return true
		}
	}
	return false
}

func (c *RemotePerasCommitter) authorityDrainHasActiveUseLocked(scopes []compile.AuthorityScope) bool {
	for _, use := range c.drainUses {
		for _, scope := range scopes {
			if authorityDrainScopesOverlap(use.scope, scope) {
				return true
			}
		}
	}
	return false
}

func (c *RemotePerasCommitter) removeAuthorityDrainScopeLocked(scope compile.AuthorityScope) {
	for i, current := range c.drainScopes {
		if !authorityScopesEqual(current, scope) {
			continue
		}
		copy(c.drainScopes[i:], c.drainScopes[i+1:])
		c.drainScopes[len(c.drainScopes)-1] = compile.AuthorityScope{}
		c.drainScopes = c.drainScopes[:len(c.drainScopes)-1]
		return
	}
}

func normalizeAuthorityDrainScopes(scopes []compile.AuthorityScope) []compile.AuthorityScope {
	if len(scopes) == 0 {
		return []compile.AuthorityScope{{}}
	}
	out := make([]compile.AuthorityScope, 0, len(scopes))
	for _, scope := range scopes {
		if authorityScopeEmpty(scope) {
			return []compile.AuthorityScope{{}}
		}
		out = append(out, cloneRuntimeAuthorityScope(scope))
	}
	return out
}

func authorityDrainScopesOverlap(left, right compile.AuthorityScope) bool {
	if authorityScopeEmpty(left) || authorityScopeEmpty(right) {
		return true
	}
	return fsperas.AuthorityScopesOverlap(left, right)
}

func authorityScopesEqual(left, right compile.AuthorityScope) bool {
	if left.Mount != right.Mount || left.MountKeyID != right.MountKeyID {
		return false
	}
	return slices.Equal(left.Buckets, right.Buckets) &&
		slices.Equal(left.Parents, right.Parents) &&
		slices.Equal(left.Inodes, right.Inodes)
}

func cloneRuntimeAuthorityScope(scope compile.AuthorityScope) compile.AuthorityScope {
	out := scope
	out.Buckets = append([]fsmeta.AffinityBucket(nil), scope.Buckets...)
	out.Parents = append([]fsmeta.InodeID(nil), scope.Parents...)
	out.Inodes = append([]fsmeta.InodeID(nil), scope.Inodes...)
	return out
}

func cloneRuntimeAuthorityScopes(scopes []compile.AuthorityScope) []compile.AuthorityScope {
	out := make([]compile.AuthorityScope, len(scopes))
	for i, scope := range scopes {
		out[i] = cloneRuntimeAuthorityScope(scope)
	}
	return out
}
