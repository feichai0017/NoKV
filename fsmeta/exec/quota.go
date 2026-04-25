package exec

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

const defaultQuotaTTL = time.Second

type quotaLookup interface {
	GetQuotaFence(context.Context, *coordpb.GetQuotaFenceRequest) (*coordpb.GetQuotaFenceResponse, error)
}

// QuotaResolver checks rooted quota fences and tracks local runtime usage.
// Runtime usage is deliberately a view, not rooted truth; rooted truth only
// stores limits and fence eras.
type QuotaResolver interface {
	CheckQuota(context.Context, fsmeta.MountID, fsmeta.InodeID, uint64, uint64) error
	AccountQuota(fsmeta.MountID, fsmeta.InodeID, int64, int64)
}

type quotaCache struct {
	coord quotaLookup
	ttl   time.Duration
	now   func() time.Time

	mu     sync.Mutex
	fences map[quotaSubject]quotaEntry
	usages map[quotaSubject]quotaUsage
	misses map[quotaSubject]time.Time
}

type quotaSubject struct {
	mount fsmeta.MountID
	root  fsmeta.InodeID
}

type quotaEntry struct {
	fence     quotaFence
	expiresAt time.Time
}

type quotaFence struct {
	limitBytes  uint64
	limitInodes uint64
	era         uint64
}

type quotaUsage struct {
	bytes  uint64
	inodes uint64
}

func (c *quotaCache) CheckQuota(ctx context.Context, mount fsmeta.MountID, root fsmeta.InodeID, addBytes, addInodes uint64) error {
	if c == nil || c.coord == nil {
		return nil
	}
	if mount == "" {
		return fsmeta.ErrInvalidMountID
	}
	if err := c.checkSubject(ctx, quotaSubject{mount: mount}, addBytes, addInodes); err != nil {
		return err
	}
	if root != 0 {
		return c.checkSubject(ctx, quotaSubject{mount: mount, root: root}, addBytes, addInodes)
	}
	return nil
}

func (c *quotaCache) AccountQuota(mount fsmeta.MountID, root fsmeta.InodeID, deltaBytes, deltaInodes int64) {
	if c == nil || mount == "" {
		return
	}
	c.accountSubject(quotaSubject{mount: mount}, deltaBytes, deltaInodes)
	if root != 0 {
		c.accountSubject(quotaSubject{mount: mount, root: root}, deltaBytes, deltaInodes)
	}
}

func (c *quotaCache) checkSubject(ctx context.Context, subject quotaSubject, addBytes, addInodes uint64) error {
	fence, ok, err := c.resolve(ctx, subject)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	c.mu.Lock()
	usage := c.usages[subject]
	c.mu.Unlock()
	if fence.limitBytes > 0 && wouldExceed(usage.bytes, addBytes, fence.limitBytes) {
		return fmt.Errorf("%w: mount=%s root=%d bytes=%d add=%d limit=%d era=%d",
			fsmeta.ErrQuotaExceeded, subject.mount, subject.root, usage.bytes, addBytes, fence.limitBytes, fence.era)
	}
	if fence.limitInodes > 0 && wouldExceed(usage.inodes, addInodes, fence.limitInodes) {
		return fmt.Errorf("%w: mount=%s root=%d inodes=%d add=%d limit=%d era=%d",
			fsmeta.ErrQuotaExceeded, subject.mount, subject.root, usage.inodes, addInodes, fence.limitInodes, fence.era)
	}
	return nil
}

func (c *quotaCache) resolve(ctx context.Context, subject quotaSubject) (quotaFence, bool, error) {
	now := c.clock()
	if fence, ok, found := c.lookup(subject, now); found {
		return fence, ok, nil
	}
	resp, err := c.coord.GetQuotaFence(ctx, &coordpb.GetQuotaFenceRequest{
		Subject: &coordpb.QuotaSubject{
			MountId:     string(subject.mount),
			SubtreeRoot: uint64(subject.root),
		},
	})
	if err != nil {
		return quotaFence{}, false, err
	}
	if resp == nil || resp.GetNotFound() || resp.GetFence() == nil {
		c.putMiss(subject, now)
		return quotaFence{}, false, nil
	}
	info := resp.GetFence()
	fence := quotaFence{limitBytes: info.GetLimitBytes(), limitInodes: info.GetLimitInodes(), era: info.GetEra()}
	c.putFence(subject, now, fence)
	return fence, true, nil
}

func (c *quotaCache) lookup(subject quotaSubject, now time.Time) (quotaFence, bool, bool) {
	if c.ttl <= 0 {
		return quotaFence{}, false, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.fences[subject]; ok && now.Before(entry.expiresAt) {
		return entry.fence, true, true
	}
	if expiresAt, ok := c.misses[subject]; ok && now.Before(expiresAt) {
		return quotaFence{}, false, true
	}
	return quotaFence{}, false, false
}

func (c *quotaCache) putFence(subject quotaSubject, now time.Time, fence quotaFence) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fences == nil {
		c.fences = make(map[quotaSubject]quotaEntry)
	}
	if c.misses != nil {
		delete(c.misses, subject)
	}
	c.fences[subject] = quotaEntry{fence: fence, expiresAt: now.Add(c.ttl)}
}

func (c *quotaCache) putMiss(subject quotaSubject, now time.Time) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.misses == nil {
		c.misses = make(map[quotaSubject]time.Time)
	}
	if c.fences != nil {
		delete(c.fences, subject)
	}
	c.misses[subject] = now.Add(c.ttl)
}

func (c *quotaCache) accountSubject(subject quotaSubject, deltaBytes, deltaInodes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.usages == nil {
		c.usages = make(map[quotaSubject]quotaUsage)
	}
	usage := c.usages[subject]
	usage.bytes = applyDelta(usage.bytes, deltaBytes)
	usage.inodes = applyDelta(usage.inodes, deltaInodes)
	c.usages[subject] = usage
}

func (c *quotaCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func wouldExceed(current, add, limit uint64) bool {
	if current > limit {
		return true
	}
	return add > limit-current
}

func applyDelta(current uint64, delta int64) uint64 {
	if delta < 0 {
		sub := uint64(-delta)
		if sub >= current {
			return 0
		}
		return current - sub
	}
	add := uint64(delta)
	if add > math.MaxUint64-current {
		return math.MaxUint64
	}
	return current + add
}

func inodeSizeDelta(size uint64) int64 {
	if size > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(size)
}
