package raftstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	perasauth "github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

const (
	defaultPerasSegmentWitnessRetries      = 3
	defaultPerasSegmentWitnessRetryBackoff = 20 * time.Millisecond
	defaultPerasSegmentBatchSize           = 256
	defaultPerasSegmentMaxReplayMutations  = 20
	defaultPerasSegmentFlushEvery          = 25 * time.Millisecond
	defaultPerasBackgroundFlushTimeout     = 2 * time.Second
	defaultPerasBackgroundErrorBackoff     = time.Second
)

type perasGrantProvider interface {
	HolderID() string
	Acquire(context.Context, compile.AuthorityScope) (perasauth.AuthorityGrant, bool, error)
}

type perasSegmentInstaller interface {
	InstallPerasSegment(context.Context, compile.AuthorityScope, fsperas.PerasSegment, []byte, [32]byte) error
}

type perasSegmentInstallClient interface {
	InstallPerasSegment(context.Context, []byte, *kvrpcpb.PerasInstallSegmentRequest) (*kvrpcpb.PerasInstallSegmentResponse, error)
}

type RemotePerasCommitterConfig struct {
	Authority                  perasGrantProvider
	Witnesses                  []fsperas.WitnessReplica
	Installer                  perasSegmentInstaller
	Quorum                     int
	SegmentWitnessRetries      int
	SegmentWitnessRetryBackoff time.Duration
	SegmentBatchSize           int
	SegmentMaxReplayMutations  int
	SegmentFlushEvery          time.Duration
	BackgroundFlushTimeout     time.Duration
	BackgroundErrorBackoff     time.Duration
	Now                        func() time.Time
}

// RemotePerasCommitter is the fsmeta runtime bridge from compiler deltas to
// remote durable witnesses. It keeps an in-process read overlay until segment
// apply lands.
type RemotePerasCommitter struct {
	authority  perasGrantProvider
	witnesses  []fsperas.WitnessReplica
	installer  perasSegmentInstaller
	quorum     int
	retries    int
	backoff    time.Duration
	batchSize  int
	maxReplay  int
	flushEvery time.Duration
	bgTimeout  time.Duration
	bgBackoff  time.Duration
	now        func() time.Time

	commitMu  sync.RWMutex
	flushMu   sync.Mutex
	bgRunning atomic.Bool
	bgNext    atomic.Int64
	holdersMu sync.Mutex
	holders   map[uint64]*fsperas.Holder
	latches   *fsperas.AdmissionLatches

	overlayMu        sync.RWMutex
	overlay          map[string]runtimePerasOverlayEntry
	sealed           map[string]runtimePerasOverlayEntry
	known            map[string]bool
	emptyDirs        map[string]struct{}
	segments         []fsperas.PerasSegment
	overlayKeys      []string
	sealedKeys       []string
	overlayKeysDirty bool
	sealedKeysDirty  bool

	stop chan struct{}

	commitTotal       atomic.Uint64
	flushTotal        atomic.Uint64
	segmentTotal      atomic.Uint64
	segmentOpsTotal   atomic.Uint64
	segmentEntryTotal atomic.Uint64
	errorTotal        atomic.Uint64
	retryTotal        atomic.Uint64
	bgSkipTotal       atomic.Uint64
	bgErrorTotal      atomic.Uint64

	statsMu          sync.RWMutex
	lastSegmentStats fsperas.SegmentStats
	lastSegmentRoot  [32]byte
	lastError        string
}

type runtimePerasOverlayEntry struct {
	opID   fsperas.OperationID
	key    []byte
	value  []byte
	delete bool
}

type runtimePerasFlushJob struct {
	holder  *fsperas.Holder
	scope   compile.AuthorityScope
	plan    fsperas.ReplayPlan
	segment fsperas.PerasSegment
	payload []byte
	digest  [32]byte
}

type runtimePerasFrozenPlan struct {
	holder *fsperas.Holder
	scope  compile.AuthorityScope
	plan   fsperas.ReplayPlan
}

func NewRemotePerasCommitter(cfg RemotePerasCommitterConfig) (*RemotePerasCommitter, error) {
	if cfg.Authority == nil || cfg.Authority.HolderID() == "" || len(cfg.Witnesses) == 0 {
		return nil, errPerasCommitterInvalid
	}
	witnesses := make([]fsperas.WitnessReplica, 0, len(cfg.Witnesses))
	seen := make(map[string]struct{}, len(cfg.Witnesses))
	for _, witness := range cfg.Witnesses {
		if witness == nil || witness.ID() == "" {
			return nil, errPerasCommitterInvalid
		}
		if _, ok := seen[witness.ID()]; ok {
			return nil, errPerasCommitterInvalid
		}
		seen[witness.ID()] = struct{}{}
		witnesses = append(witnesses, witness)
	}
	quorum := cfg.Quorum
	if quorum == 0 {
		quorum = len(witnesses)/2 + 1
	}
	if quorum <= 0 || quorum > len(witnesses) {
		return nil, errPerasCommitterInvalid
	}
	retries := cfg.SegmentWitnessRetries
	if retries == 0 {
		retries = defaultPerasSegmentWitnessRetries
	}
	if retries < 0 {
		return nil, errPerasCommitterInvalid
	}
	backoff := cfg.SegmentWitnessRetryBackoff
	if backoff == 0 {
		backoff = defaultPerasSegmentWitnessRetryBackoff
	}
	if backoff < 0 {
		return nil, errPerasCommitterInvalid
	}
	batchSize := cfg.SegmentBatchSize
	if batchSize == 0 {
		batchSize = defaultPerasSegmentBatchSize
	}
	if batchSize < 0 {
		return nil, errPerasCommitterInvalid
	}
	maxReplay := cfg.SegmentMaxReplayMutations
	if maxReplay == 0 {
		maxReplay = defaultPerasSegmentMaxReplayMutations
	}
	if maxReplay < 0 {
		return nil, errPerasCommitterInvalid
	}
	flushEvery := cfg.SegmentFlushEvery
	if flushEvery == 0 {
		flushEvery = defaultPerasSegmentFlushEvery
	}
	if flushEvery < 0 {
		return nil, errPerasCommitterInvalid
	}
	bgTimeout := cfg.BackgroundFlushTimeout
	if bgTimeout == 0 {
		bgTimeout = defaultPerasBackgroundFlushTimeout
	}
	if bgTimeout < 0 {
		return nil, errPerasCommitterInvalid
	}
	bgBackoff := cfg.BackgroundErrorBackoff
	if bgBackoff == 0 {
		bgBackoff = defaultPerasBackgroundErrorBackoff
	}
	if bgBackoff < 0 {
		return nil, errPerasCommitterInvalid
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	c := &RemotePerasCommitter{
		authority:  cfg.Authority,
		witnesses:  witnesses,
		installer:  cfg.Installer,
		quorum:     quorum,
		retries:    retries,
		backoff:    backoff,
		batchSize:  batchSize,
		maxReplay:  maxReplay,
		flushEvery: flushEvery,
		bgTimeout:  bgTimeout,
		bgBackoff:  bgBackoff,
		now:        now,
		holders:    make(map[uint64]*fsperas.Holder),
		latches:    fsperas.NewAdmissionLatches(),
		overlay:    make(map[string]runtimePerasOverlayEntry),
		sealed:     make(map[string]runtimePerasOverlayEntry),
		known:      make(map[string]bool),
		emptyDirs:  make(map[string]struct{}),
		stop:       make(chan struct{}),
	}
	if c.flushEvery > 0 {
		go c.flushLoop()
	}
	return c, nil
}

func (c *RemotePerasCommitter) CommitPeras(ctx context.Context, id fsperas.OperationID, delta compile.SemanticDelta, admission fsperas.AdmissionFunc) (fsperas.VisibleAck, error) {
	if c == nil || c.authority == nil {
		return fsperas.VisibleAck{}, errPerasCommitterInvalid
	}
	c.commitMu.RLock()
	defer c.commitMu.RUnlock()
	grant, owned, err := c.authority.Acquire(ctx, delta.Authority)
	if err != nil {
		c.recordError(err)
		return fsperas.VisibleAck{}, err
	}
	if !owned {
		c.recordError(errPerasAuthorityNotHeld)
		return fsperas.VisibleAck{}, errPerasAuthorityNotHeld
	}
	holder, err := c.holderForGrant(grant)
	if err != nil {
		c.recordError(err)
		return fsperas.VisibleAck{}, err
	}
	unlockAdmission := c.latches.Lock(delta)
	defer unlockAdmission()
	if err := fsperas.Admit(ctx, delta, admission); err != nil {
		if !errors.Is(err, fsperas.ErrAdmissionRejected) && !isPerasAdmissionTerminalError(err) {
			c.recordError(err)
		}
		return fsperas.VisibleAck{}, err
	}
	ack, err := holder.Submit(ctx, id, delta)
	if err != nil {
		c.recordError(err)
		return fsperas.VisibleAck{}, err
	}
	if err := c.addOverlay(id, delta); err != nil {
		holder.MarkAppliedIDs(id)
		c.recordError(err)
		return fsperas.VisibleAck{}, err
	}
	c.commitTotal.Add(1)
	if c.batchSize > 0 && holder.Pending() >= c.batchSize {
		c.triggerBackgroundFlush()
	}
	return ack, nil
}

func (c *RemotePerasCommitter) holderForGrant(grant perasauth.AuthorityGrant) (*fsperas.Holder, error) {
	if !grant.Valid() || grant.HolderID != c.authority.HolderID() {
		return nil, errPerasCommitterInvalid
	}
	c.holdersMu.Lock()
	defer c.holdersMu.Unlock()
	if holder := c.holders[grant.EpochID]; holder != nil {
		return holder, nil
	}
	holder, err := fsperas.NewHolder(fsperas.HolderConfig{
		EpochID:  grant.EpochID,
		HolderID: grant.HolderID,
	})
	if err != nil {
		return nil, err
	}
	c.holders[grant.EpochID] = holder
	return holder, nil
}

func (c *RemotePerasCommitter) Flush(ctx context.Context) error {
	return c.flush(ctx, nil)
}

func (c *RemotePerasCommitter) FlushAuthority(ctx context.Context, scope compile.AuthorityScope) error {
	if scope.Mount == "" || scope.MountKeyID == 0 {
		return c.Flush(ctx)
	}
	return c.flush(ctx, &scope)
}

func (c *RemotePerasCommitter) DrainAuthority(ctx context.Context, retirer fsperas.AuthorityRetirer, scopes ...compile.AuthorityScope) error {
	if c == nil || retirer == nil {
		return errPerasCommitterInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.commitMu.Lock()
	defer c.commitMu.Unlock()

	jobs, err := c.freezeFlushJobsLocked(nil)
	if err != nil {
		return err
	}
	if err := c.installFlushJobs(ctx, jobs); err != nil {
		return err
	}
	return c.retireDrainedAuthority(ctx, retirer, scopes...)
}

func (c *RemotePerasCommitter) flush(ctx context.Context, scope *compile.AuthorityScope) error {
	if c == nil {
		return errPerasCommitterInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	return c.flushLocked(ctx, scope)
}

func (c *RemotePerasCommitter) RecoverWitnessSegments(ctx context.Context, scope compile.AuthorityScope, epochID uint64) error {
	if c == nil || epochID == 0 || c.installer == nil {
		return errPerasCommitterInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	records, err := c.collectWitnessSegments(ctx, epochID)
	if err != nil {
		return c.recordErrorf("probe peras segment witnesses: %w", err)
	}
	for _, record := range records {
		if c.segmentInstalled(record.SegmentRoot) {
			continue
		}
		if err := fsperas.VerifySegmentWitnessRecord(record); err != nil {
			return c.recordErrorf("verify peras segment witness: %w", err)
		}
		segment, err := fsperas.VerifyPerasSegmentPayload(record.SegmentPayload, record.SegmentRoot, record.SegmentPayloadDigest)
		if err != nil {
			return c.recordErrorf("decode peras witness segment: %w", err)
		}
		stats := segment.Stats()
		if record.OperationCount != stats.OperationCount || record.EntryCount != stats.EntryCount {
			return c.recordError(fsperas.ErrInvalidWitnessRecord)
		}
		if err := c.installer.InstallPerasSegment(ctx, scope, segment, record.SegmentPayload, record.SegmentPayloadDigest); err != nil {
			return c.recordErrorf("recover peras segment install: %w", err)
		}
		c.installSegment(fsperas.ReplayPlan{}, segment)
	}
	return nil
}

func (c *RemotePerasCommitter) flushLocked(ctx context.Context, scope *compile.AuthorityScope) error {
	c.commitMu.Lock()
	jobs, err := c.freezeFlushJobsLocked(scope)
	c.commitMu.Unlock()
	if err != nil {
		return err
	}
	return c.installFlushJobs(ctx, jobs)
}

func (c *RemotePerasCommitter) installFlushJobs(ctx context.Context, jobs []runtimePerasFlushJob) error {
	if len(jobs) > 0 && c.installer == nil {
		return c.recordError(errPerasCommitterInvalid)
	}
	for _, job := range jobs {
		if err := c.appendSegmentWitnessesWithRetry(ctx, job.scope, job.holder, job.segment, job.payload, job.digest); err != nil {
			return c.recordErrorf("append peras segment witness: %w", err)
		}
		if err := c.installer.InstallPerasSegment(ctx, job.scope, job.segment, job.payload, job.digest); err != nil {
			return c.recordErrorf("install peras segment: %w", err)
		}
		if err := job.holder.MarkReplayPlanApplied(job.plan); err != nil {
			return c.recordErrorf("mark peras plan applied: %w", err)
		}
		c.installSegment(job.plan, job.segment)
		c.flushTotal.Add(1)
	}
	return nil
}

func (c *RemotePerasCommitter) retireDrainedAuthority(ctx context.Context, retirer fsperas.AuthorityRetirer, scopes ...compile.AuthorityScope) error {
	if err := retirer.RetirePerasAuthority(ctx, scopes...); err != nil {
		return c.recordErrorf("retire peras authority: %w", err)
	}
	return nil
}

func (c *RemotePerasCommitter) freezeFlushJobsLocked(target *compile.AuthorityScope) ([]runtimePerasFlushJob, error) {
	plans, err := c.freezeReplayPlansLocked(target)
	if err != nil {
		return nil, err
	}
	jobs := make([]runtimePerasFlushJob, 0, len(plans))
	for _, frozen := range plans {
		bucketPlans, err := fsperas.SplitReplayPlanByFSMetaBucket(frozen.plan)
		if err != nil {
			return nil, c.recordErrorf("split peras replay plan: %w", err)
		}
		parts := make([]fsperas.ReplayPlan, 0, len(bucketPlans))
		for _, bucketPlan := range bucketPlans {
			sized, err := fsperas.SplitReplayPlanByMutationBudget(bucketPlan, c.maxReplay)
			if err != nil {
				return nil, c.recordErrorf("split peras replay plan by install budget: %w", err)
			}
			parts = append(parts, sized...)
		}
		for _, plan := range parts {
			segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
			if err != nil {
				return nil, c.recordErrorf("build peras segment: %w", err)
			}
			payload, err := fsperas.EncodePerasSegment(segment)
			if err != nil {
				return nil, c.recordErrorf("encode peras segment: %w", err)
			}
			digest, err := fsperas.PerasSegmentPayloadDigest(payload)
			if err != nil {
				return nil, c.recordErrorf("digest peras segment: %w", err)
			}
			jobs = append(jobs, runtimePerasFlushJob{
				holder:  frozen.holder,
				scope:   frozen.scope,
				plan:    plan,
				segment: segment,
				payload: payload,
				digest:  digest,
			})
		}
	}
	return jobs, nil
}

func (c *RemotePerasCommitter) freezeReplayPlansLocked(target *compile.AuthorityScope) ([]runtimePerasFrozenPlan, error) {
	holders := c.holderSnapshot()
	plans := make([]runtimePerasFrozenPlan, 0, len(holders))
	for _, holder := range holders {
		plan, scope, ok, err := c.buildFlushPlan(holder, target)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		plans = append(plans, runtimePerasFrozenPlan{
			holder: holder,
			scope:  scope,
			plan:   plan,
		})
	}
	return plans, nil
}

func (c *RemotePerasCommitter) buildFlushPlan(holder *fsperas.Holder, target *compile.AuthorityScope) (fsperas.ReplayPlan, compile.AuthorityScope, bool, error) {
	if target != nil {
		plan, scope, ok, err := holder.BuildPendingReplayPlanForScope(0, *target)
		if err != nil {
			return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, c.recordErrorf("build peras replay plan: %w", err)
		}
		return plan, scope, ok, nil
	}
	pending := holder.PendingIDs()
	if len(pending) == 0 {
		return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, nil
	}
	plan, scope, err := holder.BuildPendingReplayPlan(0)
	if err != nil {
		return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, c.recordErrorf("build peras replay plan: %w", err)
	}
	if len(plan.Operations) != len(pending) {
		return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, c.recordError(fsperas.ErrInvalidPerasSegment)
	}
	return plan, scope, true, nil
}

func (c *RemotePerasCommitter) holderSnapshot() []*fsperas.Holder {
	c.holdersMu.Lock()
	defer c.holdersMu.Unlock()
	out := make([]*fsperas.Holder, 0, len(c.holders))
	for _, holder := range c.holders {
		out = append(out, holder)
	}
	return out
}

func (c *RemotePerasCommitter) appendSegmentWitnessesWithRetry(ctx context.Context, scope compile.AuthorityScope, holder *fsperas.Holder, segment fsperas.PerasSegment, payload []byte, digest [32]byte) error {
	var last error
	attempts := c.retries + 1
	for attempt := range attempts {
		err := c.appendSegmentWitnesses(ctx, scope, holder, segment, payload, digest)
		if err == nil {
			return nil
		}
		last = err
		if !errors.Is(err, fsperas.ErrSegmentWitnessQuorumUnavailable) || attempt == attempts-1 {
			break
		}
		c.retryTotal.Add(1)
		if !sleepContext(ctx, c.backoff) {
			return ctx.Err()
		}
	}
	return last
}

func (c *RemotePerasCommitter) appendSegmentWitnesses(ctx context.Context, scope compile.AuthorityScope, holder *fsperas.Holder, segment fsperas.PerasSegment, payload []byte, digest [32]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stats := segment.Stats()
	record := fsperas.SegmentWitnessRecord{
		EpochID:              holder.EpochID(),
		SegmentRoot:          segment.Root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   uint64(len(payload)),
		SegmentPayload:       runtimeCloneBytes(payload),
		OperationCount:       stats.OperationCount,
		EntryCount:           stats.EntryCount,
		TimestampUnixNano:    c.now().UnixNano(),
		HolderID:             holder.HolderID(),
	}
	broadcastCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		id  string
		err error
	}
	resultCh := make(chan result, len(c.witnesses))
	for _, witness := range c.witnesses {
		go func() {
			err := witness.AppendSegment(broadcastCtx, scope, record)
			resultCh <- result{id: witness.ID(), err: err}
		}()
	}
	acks := make([]string, 0, len(c.witnesses))
	failures := make([]error, 0, len(c.witnesses))
	for range c.witnesses {
		res := <-resultCh
		if res.err == nil {
			acks = append(acks, res.id)
			if len(acks) >= c.quorum {
				cancel()
				slices.Sort(acks)
				return nil
			}
			continue
		}
		failures = append(failures, fmt.Errorf("%s: %w", res.id, res.err))
	}
	if len(failures) == 0 {
		return fsperas.ErrSegmentWitnessQuorumUnavailable
	}
	return errors.Join(append([]error{fsperas.ErrSegmentWitnessQuorumUnavailable}, failures...)...)
}

func (c *RemotePerasCommitter) collectWitnessSegments(ctx context.Context, epochID uint64) ([]fsperas.SegmentWitnessRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		id       string
		snapshot fsperas.WitnessSnapshot
		err      error
	}
	resultCh := make(chan result, len(c.witnesses))
	for _, witness := range c.witnesses {
		go func() {
			snapshot, err := witness.Probe(ctx, epochID)
			resultCh <- result{id: witness.ID(), snapshot: snapshot, err: err}
		}()
	}
	type key struct {
		root   [32]byte
		digest [32]byte
	}
	records := make(map[key]fsperas.SegmentWitnessRecord)
	failures := make([]error, 0, len(c.witnesses))
	successes := 0
	for range c.witnesses {
		res := <-resultCh
		if res.err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", res.id, res.err))
			continue
		}
		successes++
		for _, record := range res.snapshot.Segments {
			if record.EpochID != epochID {
				continue
			}
			k := key{root: record.SegmentRoot, digest: record.SegmentPayloadDigest}
			current, ok := records[k]
			if !ok || len(record.SegmentPayload) > len(current.SegmentPayload) {
				records[k] = record
			}
		}
	}
	if successes == 0 {
		if len(failures) == 0 {
			return nil, fsperas.ErrSegmentWitnessQuorumUnavailable
		}
		return nil, errors.Join(append([]error{fsperas.ErrSegmentWitnessQuorumUnavailable}, failures...)...)
	}
	out := make([]fsperas.SegmentWitnessRecord, 0, len(records))
	for _, record := range records {
		out = append(out, record)
	}
	slices.SortFunc(out, func(a, b fsperas.SegmentWitnessRecord) int {
		if a.TimestampUnixNano < b.TimestampUnixNano {
			return -1
		}
		if a.TimestampUnixNano > b.TimestampUnixNano {
			return 1
		}
		return bytes.Compare(a.SegmentRoot[:], b.SegmentRoot[:])
	})
	return out, nil
}

func (c *RemotePerasCommitter) installSegment(plan fsperas.ReplayPlan, segment fsperas.PerasSegment) {
	stats := segment.Stats()
	c.overlayMu.Lock()
	c.segments = append(c.segments, segment)
	for _, kv := range segment.ScanView(nil, ^uint32(0)) {
		c.sealed[string(kv.Key)] = runtimePerasOverlayEntry{
			key:    kv.Key,
			value:  kv.Value,
			delete: kv.Delete,
		}
	}
	c.sealedKeysDirty = true
	for _, op := range plan.Operations {
		for _, mutation := range op.Mutations {
			key := string(mutation.Key)
			entry, ok := c.overlay[key]
			if ok && entry.opID == op.OpID {
				delete(c.overlay, key)
				c.overlayKeysDirty = true
			}
		}
	}
	c.overlayMu.Unlock()

	c.segmentTotal.Add(1)
	c.segmentOpsTotal.Add(stats.OperationCount)
	c.segmentEntryTotal.Add(stats.EntryCount)
	c.statsMu.Lock()
	c.lastSegmentStats = stats
	c.lastSegmentRoot = segment.Root
	c.statsMu.Unlock()
}

func (c *RemotePerasCommitter) segmentInstalled(root [32]byte) bool {
	c.overlayMu.RLock()
	defer c.overlayMu.RUnlock()
	for _, segment := range c.segments {
		if segment.Root == root {
			return true
		}
	}
	return false
}

func (c *RemotePerasCommitter) flushBackground() {
	if c == nil {
		return
	}
	defer c.bgRunning.Store(false)
	now := c.now().UnixNano()
	if until := c.bgNext.Load(); until > now {
		c.bgSkipTotal.Add(1)
		return
	}
	if !c.flushMu.TryLock() {
		c.bgSkipTotal.Add(1)
		return
	}
	defer c.flushMu.Unlock()
	ctx := context.Background()
	var cancel context.CancelFunc
	if c.bgTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.bgTimeout)
		defer cancel()
	}
	if err := c.flushLocked(ctx, nil); err != nil {
		c.bgErrorTotal.Add(1)
		if c.bgBackoff > 0 {
			c.bgNext.Store(c.now().Add(c.bgBackoff).UnixNano())
		}
		return
	}
	c.bgNext.Store(0)
}

func (c *RemotePerasCommitter) GetPerasOverlay(key []byte) ([]byte, bool, bool) {
	if c == nil {
		return nil, false, false
	}
	c.overlayMu.RLock()
	entry, ok := c.overlay[string(key)]
	if ok {
		c.overlayMu.RUnlock()
		return runtimeCloneBytes(entry.value), entry.delete, true
	}
	entry, ok = c.sealed[string(key)]
	if ok {
		c.overlayMu.RUnlock()
		return runtimeCloneBytes(entry.value), entry.delete, true
	}
	c.overlayMu.RUnlock()
	return nil, false, false
}

func (c *RemotePerasCommitter) KeyState(key []byte) (present bool, known bool) {
	if c == nil {
		return false, false
	}
	c.overlayMu.RLock()
	entry, ok := c.overlay[string(key)]
	if ok {
		c.overlayMu.RUnlock()
		return !entry.delete, true
	}
	entry, ok = c.sealed[string(key)]
	if ok {
		c.overlayMu.RUnlock()
		return !entry.delete, true
	}
	present, ok = c.known[string(key)]
	c.overlayMu.RUnlock()
	return present, ok
}

func (c *RemotePerasCommitter) DirectoryEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool {
	if c == nil {
		return false
	}
	c.overlayMu.RLock()
	_, ok := c.emptyDirs[fsperas.DirectoryFactKey(mount, inode)]
	c.overlayMu.RUnlock()
	return ok
}

func (c *RemotePerasCommitter) RememberKey(key []byte, present bool) {
	if c == nil || len(key) == 0 {
		return
	}
	c.overlayMu.Lock()
	if c.known == nil {
		c.known = make(map[string]bool)
	}
	c.known[string(key)] = present
	c.overlayMu.Unlock()
}

func (c *RemotePerasCommitter) RememberEmptyDirectory(mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if c == nil {
		return
	}
	c.overlayMu.Lock()
	if c.emptyDirs == nil {
		c.emptyDirs = make(map[string]struct{})
	}
	fsperas.RememberEmptyDirectoryFact(c.emptyDirs, mount, inode)
	c.overlayMu.Unlock()
}

func (c *RemotePerasCommitter) ScanPerasOverlay(start []byte, limit uint32) []fsperas.OverlayKV {
	if c == nil || limit == 0 {
		return nil
	}
	c.overlayMu.Lock()
	c.refreshPerasViewKeysLocked()
	merged := make(map[string]runtimePerasOverlayEntry, int(limit)*2)
	c.collectPerasViewSuffixLocked(merged, c.sealed, c.sealedKeys, start, limit)
	c.collectPerasViewSuffixLocked(merged, c.overlay, c.overlayKeys, start, limit)
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > int(limit) {
		keys = keys[:limit]
	}
	out := make([]fsperas.OverlayKV, 0, len(keys))
	for _, key := range keys {
		entry := merged[key]
		out = append(out, fsperas.OverlayKV{
			Key:    runtimeCloneBytes(entry.key),
			Value:  runtimeCloneBytes(entry.value),
			Delete: entry.delete,
		})
	}
	c.overlayMu.Unlock()
	return out
}

func (c *RemotePerasCommitter) refreshPerasViewKeysLocked() {
	if c.sealedKeysDirty {
		c.sealedKeys = sortedRuntimePerasViewKeys(c.sealed, c.sealedKeys)
		c.sealedKeysDirty = false
	}
	if c.overlayKeysDirty {
		c.overlayKeys = sortedRuntimePerasViewKeys(c.overlay, c.overlayKeys)
		c.overlayKeysDirty = false
	}
}

func sortedRuntimePerasViewKeys(view map[string]runtimePerasOverlayEntry, reuse []string) []string {
	reuse = reuse[:0]
	for key := range view {
		reuse = append(reuse, key)
	}
	sort.Strings(reuse)
	return reuse
}

func (c *RemotePerasCommitter) collectPerasViewSuffixLocked(dst map[string]runtimePerasOverlayEntry, view map[string]runtimePerasOverlayEntry, keys []string, start []byte, _ uint32) {
	if len(keys) == 0 {
		return
	}
	startKey := string(start)
	i := sort.SearchStrings(keys, startKey)
	for ; i < len(keys); i++ {
		key := keys[i]
		entry, ok := view[key]
		if !ok {
			continue
		}
		dst[key] = entry
	}
}

func (c *RemotePerasCommitter) Stats() map[string]any {
	if c == nil {
		return map[string]any{
			"commit_total":                  uint64(0),
			"flush_total":                   uint64(0),
			"segment_total":                 uint64(0),
			"segment_operations_total":      uint64(0),
			"segment_entries_total":         uint64(0),
			"last_segment_operations":       uint64(0),
			"last_segment_input_mutations":  uint64(0),
			"last_segment_entries":          uint64(0),
			"last_segment_coalesced":        uint64(0),
			"last_segment_compression_x100": uint64(0),
			"last_segment_root":             [32]byte{},
			"last_error":                    "",
			"error_total":                   uint64(0),
			"retry_total":                   uint64(0),
			"background_skip_total":         uint64(0),
			"background_error_total":        uint64(0),
			"overlay_keys":                  0,
			"segment_keys":                  0,
			"predicate_known_keys":          0,
			"predicate_empty_dirs":          0,
			"holders":                       0,
			"pending":                       0,
			"witness_count":                 0,
			"quorum":                        0,
		}
	}
	c.overlayMu.RLock()
	overlayKeys := len(c.overlay)
	segmentKeys := len(c.sealed)
	knownKeys := len(c.known)
	emptyDirs := len(c.emptyDirs)
	c.overlayMu.RUnlock()
	c.holdersMu.Lock()
	holders := len(c.holders)
	pending := 0
	for _, holder := range c.holders {
		pending += holder.Pending()
	}
	c.holdersMu.Unlock()
	c.statsMu.RLock()
	lastSegmentStats := c.lastSegmentStats
	lastSegmentRoot := c.lastSegmentRoot
	lastError := c.lastError
	c.statsMu.RUnlock()
	return map[string]any{
		"commit_total":                  c.commitTotal.Load(),
		"flush_total":                   c.flushTotal.Load(),
		"segment_total":                 c.segmentTotal.Load(),
		"segment_operations_total":      c.segmentOpsTotal.Load(),
		"segment_entries_total":         c.segmentEntryTotal.Load(),
		"last_segment_operations":       lastSegmentStats.OperationCount,
		"last_segment_input_mutations":  lastSegmentStats.InputMutationCount,
		"last_segment_entries":          lastSegmentStats.EntryCount,
		"last_segment_coalesced":        lastSegmentStats.CoalescedMutations,
		"last_segment_compression_x100": uint64(lastSegmentStats.CompressionRatio * 100),
		"last_segment_root":             lastSegmentRoot,
		"last_error":                    lastError,
		"error_total":                   c.errorTotal.Load(),
		"retry_total":                   c.retryTotal.Load(),
		"background_skip_total":         c.bgSkipTotal.Load(),
		"background_error_total":        c.bgErrorTotal.Load(),
		"overlay_keys":                  overlayKeys,
		"segment_keys":                  segmentKeys,
		"predicate_known_keys":          knownKeys,
		"predicate_empty_dirs":          emptyDirs,
		"holders":                       holders,
		"pending":                       pending,
		"witness_count":                 len(c.witnesses),
		"quorum":                        c.quorum,
	}
}

func (c *RemotePerasCommitter) Close() {
	if c == nil || c.stop == nil {
		return
	}
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
}

func (c *RemotePerasCommitter) flushLoop() {
	ticker := time.NewTicker(c.flushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.triggerBackgroundFlush()
		case <-c.stop:
			return
		}
	}
}

func (c *RemotePerasCommitter) triggerBackgroundFlush() {
	if c == nil {
		return
	}
	if !c.bgRunning.CompareAndSwap(false, true) {
		c.bgSkipTotal.Add(1)
		return
	}
	go c.flushBackground()
}

func (c *RemotePerasCommitter) addOverlay(id fsperas.OperationID, delta compile.SemanticDelta) error {
	c.overlayMu.Lock()
	defer c.overlayMu.Unlock()
	for _, effect := range delta.WriteEffects {
		if len(effect.Key) == 0 {
			return errPerasCommitterInvalid
		}
		entry := runtimePerasOverlayEntry{
			opID: id,
			key:  runtimeCloneBytes(effect.Key),
		}
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return errPerasCommitterInvalid
			}
			entry.value = runtimeCloneBytes(effect.Value)
		case compile.EffectDelete:
			entry.delete = true
		default:
			return errPerasCommitterInvalid
		}
		if _, ok := c.overlay[string(effect.Key)]; !ok {
			c.overlayKeysDirty = true
		}
		c.overlay[string(effect.Key)] = entry
	}
	if c.known == nil {
		c.known = make(map[string]bool)
	}
	if c.emptyDirs == nil {
		c.emptyDirs = make(map[string]struct{})
	}
	if err := fsperas.RememberDeltaFacts(c.known, c.emptyDirs, delta); err != nil {
		return err
	}
	return nil
}

type runnerPerasSegmentInstaller struct {
	runner *Runner
}

func newRunnerPerasSegmentInstaller(runner *Runner) *runnerPerasSegmentInstaller {
	return &runnerPerasSegmentInstaller{runner: runner}
}

func (i *runnerPerasSegmentInstaller) InstallPerasSegment(ctx context.Context, _ compile.AuthorityScope, segment fsperas.PerasSegment, payload []byte, digest [32]byte) error {
	if i == nil || i.runner == nil || i.runner.kv == nil {
		return errPerasCommitterInvalid
	}
	kv, ok := i.runner.kv.(perasSegmentInstallClient)
	if !ok {
		return errPerasCommitterInvalid
	}
	entries := segment.Entries()
	if len(entries) == 0 || len(entries[0].Key) == 0 {
		return fsperas.ErrInvalidPerasSegment
	}
	installVersion, err := i.runner.ReserveTimestamp(ctx, 1)
	if err != nil {
		return err
	}
	resp, err := kv.InstallPerasSegment(ctx, entries[0].Key, &kvrpcpb.PerasInstallSegmentRequest{
		RoutingKey:           runtimeCloneBytes(entries[0].Key),
		SegmentRoot:          append([]byte(nil), segment.Root[:]...),
		SegmentPayloadDigest: append([]byte(nil), digest[:]...),
		SegmentPayload:       append([]byte(nil), payload...),
		InstallVersion:       installVersion,
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return errPerasCommitterInvalid
	}
	if keyErr := resp.GetError(); keyErr != nil {
		return runnerKeyError("peras install segment", keyErr)
	}
	return validatePerasSegmentInstallResponse(segment, resp)
}

func validatePerasSegmentInstallResponse(segment fsperas.PerasSegment, resp *kvrpcpb.PerasInstallSegmentResponse) error {
	if resp == nil {
		return errPerasCommitterInvalid
	}
	if !bytes.Equal(resp.GetSegmentRoot(), segment.Root[:]) {
		return errPerasCommitterInvalid
	}
	stats := segment.Stats()
	if resp.GetOperationCount() != stats.OperationCount ||
		resp.GetEntryCount() != stats.EntryCount ||
		(stats.EntryCount > 0 && resp.GetAppliedEntries() == 0) {
		return errPerasCommitterInvalid
	}
	return nil
}

func (c *RemotePerasCommitter) recordErrorf(format string, args ...any) error {
	return c.recordError(fmt.Errorf(format, args...))
}

func isPerasAdmissionTerminalError(err error) bool {
	return errors.Is(err, fsmeta.ErrExists) ||
		errors.Is(err, fsmeta.ErrNotFound) ||
		errors.Is(err, fsmeta.ErrInvalidRequest) ||
		errors.Is(err, fsmeta.ErrInvalidValue)
}

func (c *RemotePerasCommitter) recordError(err error) error {
	if c == nil || err == nil {
		return err
	}
	c.errorTotal.Add(1)
	c.statsMu.Lock()
	c.lastError = err.Error()
	c.statsMu.Unlock()
	return err
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func runtimeCloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
