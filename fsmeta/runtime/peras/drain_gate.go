package peras

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

type perasAuthorityUse struct {
	id    uint64
	scope compile.AuthorityScope
}

func (c *Runtime) DrainAuthority(ctx context.Context, retirer fsperas.AuthorityRetirer, scopes ...compile.AuthorityScope) error {
	if c == nil || retirer == nil {
		return ErrRuntimeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	drainScopes := NormalizeScopes(scopes)
	endDrain := c.beginAuthorityDrain(drainScopes)
	defer endDrain()
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.commitMu.Lock()
	var batches []perasFlushBatch
	if len(drainScopes) == 1 && ScopeEmpty(drainScopes[0]) {
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
	if err := (flushPipeline{runtime: c, level: fsperas.SegmentPersistencePublished}).run(ctx, batches); err != nil {
		return err
	}
	return c.retireDrainedAuthority(ctx, retirer, scopes...)
}

func (c *Runtime) retireDrainedAuthority(ctx context.Context, retirer fsperas.AuthorityRetirer, scopes ...compile.AuthorityScope) error {
	if err := retirer.RetirePerasAuthority(ctx, scopes...); err != nil {
		return c.recordErrorf("retire peras authority: %w", err)
	}
	return nil
}

func (c *Runtime) enterAuthority(scope compile.AuthorityScope) func() {
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
		scope: CloneScope(scope),
	})
	c.drainMu.Unlock()
	return func() {
		c.leaveAuthority(id)
	}
}

func (c *Runtime) leaveAuthority(id uint64) {
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

func (c *Runtime) beginAuthorityDrain(scopes []compile.AuthorityScope) func() {
	if c == nil || c.drainCond == nil {
		return func() {}
	}
	drainScopes := CloneScopes(scopes)
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

func (c *Runtime) endAuthorityDrain(scopes []compile.AuthorityScope) {
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

func (c *Runtime) authorityDrainBlocksLocked(scope compile.AuthorityScope) bool {
	for _, drain := range c.drainScopes {
		if ScopesOverlap(scope, drain) {
			return true
		}
	}
	return false
}

func (c *Runtime) authorityDrainHasActiveUseLocked(scopes []compile.AuthorityScope) bool {
	for _, use := range c.drainUses {
		for _, scope := range scopes {
			if ScopesOverlap(use.scope, scope) {
				return true
			}
		}
	}
	return false
}

func (c *Runtime) removeAuthorityDrainScopeLocked(scope compile.AuthorityScope) {
	for i, current := range c.drainScopes {
		if !ScopesEqual(current, scope) {
			continue
		}
		copy(c.drainScopes[i:], c.drainScopes[i+1:])
		c.drainScopes[len(c.drainScopes)-1] = compile.AuthorityScope{}
		c.drainScopes = c.drainScopes[:len(c.drainScopes)-1]
		return
	}
}
