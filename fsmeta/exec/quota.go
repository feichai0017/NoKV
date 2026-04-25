package exec

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

const defaultQuotaTTL = time.Second

type quotaLookup interface {
	GetQuotaFence(context.Context, *coordpb.GetQuotaFenceRequest) (*coordpb.GetQuotaFenceResponse, error)
}

// QuotaChange describes one logical quota delta. Scope 0 means the change only
// affects the mount-wide subject; non-zero scopes also affect that direct
// accounting scope.
type QuotaChange struct {
	Mount  fsmeta.MountID
	Scope  fsmeta.InodeID
	Bytes  int64
	Inodes int64
}

// QuotaResolver resolves rooted quota fences and plans usage-counter mutations
// that must be committed in the same transaction as the metadata mutation.
type QuotaResolver interface {
	ReserveQuota(context.Context, TxnRunner, []QuotaChange, uint64) ([]*kvrpcpb.Mutation, error)
}

type quotaCache struct {
	coord quotaLookup
	ttl   time.Duration
	now   func() time.Time

	mu     sync.Mutex
	fences map[quotaSubject]quotaEntry
	misses map[quotaSubject]time.Time

	checksTotal         atomic.Uint64
	rejectsTotal        atomic.Uint64
	cacheHitsTotal      atomic.Uint64
	cacheMissesTotal    atomic.Uint64
	fenceUpdatesTotal   atomic.Uint64
	usageMutationsTotal atomic.Uint64
}

type quotaSubject struct {
	mount fsmeta.MountID
	scope fsmeta.InodeID
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

type quotaDelta struct {
	bytes  int64
	inodes int64
}

// ReserveQuota returns usage-counter mutations for the provided quota deltas.
// The returned mutations are intentionally plain Put/Delete mutations; the
// Percolator conflict check on the shared usage keys serializes concurrent
// writers across fsmeta gateway processes.
func (c *quotaCache) ReserveQuota(ctx context.Context, runner TxnRunner, changes []QuotaChange, startVersion uint64) ([]*kvrpcpb.Mutation, error) {
	if c == nil || c.coord == nil || runner == nil || len(changes) == 0 {
		return nil, nil
	}
	deltas, err := c.aggregate(changes)
	if err != nil {
		return nil, err
	}
	mutations := make([]*kvrpcpb.Mutation, 0, len(deltas))
	for subject, delta := range deltas {
		mut, err := c.reserveSubject(ctx, runner, subject, delta, startVersion)
		if err != nil {
			return nil, err
		}
		if mut != nil {
			mutations = append(mutations, mut)
		}
	}
	return mutations, nil
}

func (c *quotaCache) aggregate(changes []QuotaChange) (map[quotaSubject]quotaDelta, error) {
	out := make(map[quotaSubject]quotaDelta)
	for _, change := range changes {
		if change.Mount == "" {
			return nil, fsmeta.ErrInvalidMountID
		}
		addDelta(out, quotaSubject{mount: change.Mount}, change.Bytes, change.Inodes)
		if change.Scope != 0 {
			addDelta(out, quotaSubject{mount: change.Mount, scope: change.Scope}, change.Bytes, change.Inodes)
		}
	}
	for subject, delta := range out {
		if delta.bytes == 0 && delta.inodes == 0 {
			delete(out, subject)
		}
	}
	return out, nil
}

func addDelta(out map[quotaSubject]quotaDelta, subject quotaSubject, bytesDelta, inodesDelta int64) {
	delta := out[subject]
	delta.bytes = saturatingAddInt64(delta.bytes, bytesDelta)
	delta.inodes = saturatingAddInt64(delta.inodes, inodesDelta)
	out[subject] = delta
}

func (c *quotaCache) reserveSubject(ctx context.Context, runner TxnRunner, subject quotaSubject, delta quotaDelta, startVersion uint64) (*kvrpcpb.Mutation, error) {
	c.checksTotal.Add(1)
	fence, ok, err := c.resolve(ctx, subject)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	key, err := fsmeta.EncodeUsageKey(subject.mount, subject.scope)
	if err != nil {
		return nil, err
	}
	current, err := readUsage(ctx, runner, key, startVersion)
	if err != nil {
		return nil, err
	}
	next := applyUsageDelta(current, delta)
	if delta.bytes > 0 && fence.limitBytes > 0 && next.Bytes > fence.limitBytes {
		c.rejectsTotal.Add(1)
		return nil, fmt.Errorf("%w: mount=%s scope=%d bytes=%d add=%d limit=%d era=%d",
			fsmeta.ErrQuotaExceeded, subject.mount, subject.scope, current.Bytes, delta.bytes, fence.limitBytes, fence.era)
	}
	if delta.inodes > 0 && fence.limitInodes > 0 && next.Inodes > fence.limitInodes {
		c.rejectsTotal.Add(1)
		return nil, fmt.Errorf("%w: mount=%s scope=%d inodes=%d add=%d limit=%d era=%d",
			fsmeta.ErrQuotaExceeded, subject.mount, subject.scope, current.Inodes, delta.inodes, fence.limitInodes, fence.era)
	}
	c.usageMutationsTotal.Add(1)
	if next.Bytes == 0 && next.Inodes == 0 {
		return &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: key}, nil
	}
	value, err := fsmeta.EncodeUsageValue(next)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: key, Value: value}, nil
}

func readUsage(ctx context.Context, runner TxnRunner, key []byte, version uint64) (fsmeta.UsageRecord, error) {
	value, ok, err := runner.Get(ctx, key, version)
	if err != nil {
		return fsmeta.UsageRecord{}, err
	}
	if !ok {
		return fsmeta.UsageRecord{}, nil
	}
	return fsmeta.DecodeUsageValue(value)
}

func applyUsageDelta(current fsmeta.UsageRecord, delta quotaDelta) fsmeta.UsageRecord {
	return fsmeta.UsageRecord{
		Bytes:  applyDelta(current.Bytes, delta.bytes),
		Inodes: applyDelta(current.Inodes, delta.inodes),
	}
}

func (c *quotaCache) resolve(ctx context.Context, subject quotaSubject) (quotaFence, bool, error) {
	now := c.clock()
	if fence, ok, found := c.lookup(subject, now); found {
		c.cacheHitsTotal.Add(1)
		return fence, ok, nil
	}
	c.cacheMissesTotal.Add(1)
	resp, err := c.coord.GetQuotaFence(ctx, &coordpb.GetQuotaFenceRequest{
		Subject: &coordpb.QuotaSubject{
			MountId:     string(subject.mount),
			SubtreeRoot: uint64(subject.scope),
		},
	})
	if err != nil {
		return quotaFence{}, false, err
	}
	if resp == nil || resp.GetNotFound() || resp.GetFence() == nil {
		c.putMiss(subject, now)
		return quotaFence{}, false, nil
	}
	fence := quotaFenceFromProto(resp.GetFence())
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

func (c *quotaCache) markFenceUpdated(info *coordpb.QuotaFenceInfo) {
	if c == nil || info == nil || info.GetSubject() == nil || info.GetSubject().GetMountId() == "" {
		return
	}
	subject := quotaSubject{
		mount: fsmeta.MountID(info.GetSubject().GetMountId()),
		scope: fsmeta.InodeID(info.GetSubject().GetSubtreeRoot()),
	}
	c.putFence(subject, c.clock(), quotaFenceFromProto(info))
	c.fenceUpdatesTotal.Add(1)
}

func (c *quotaCache) purgeMount(mount fsmeta.MountID) {
	if c == nil || mount == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for subject := range c.fences {
		if subject.mount == mount {
			delete(c.fences, subject)
		}
	}
	for subject := range c.misses {
		if subject.mount == mount {
			delete(c.misses, subject)
		}
	}
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

func (c *quotaCache) Stats() map[string]any {
	if c == nil {
		return map[string]any{}
	}
	return map[string]any{
		"checks_total":          c.checksTotal.Load(),
		"rejects_total":         c.rejectsTotal.Load(),
		"cache_hits_total":      c.cacheHitsTotal.Load(),
		"cache_misses_total":    c.cacheMissesTotal.Load(),
		"fence_updates_total":   c.fenceUpdatesTotal.Load(),
		"usage_mutations_total": c.usageMutationsTotal.Load(),
	}
}

func (c *quotaCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func quotaFenceFromProto(info *coordpb.QuotaFenceInfo) quotaFence {
	if info == nil {
		return quotaFence{}
	}
	return quotaFence{limitBytes: info.GetLimitBytes(), limitInodes: info.GetLimitInodes(), era: info.GetEra()}
}

func saturatingAddInt64(current, delta int64) int64 {
	if delta > 0 && current > math.MaxInt64-delta {
		return math.MaxInt64
	}
	if delta < 0 && current < math.MinInt64-delta {
		return math.MinInt64
	}
	return current + delta
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
