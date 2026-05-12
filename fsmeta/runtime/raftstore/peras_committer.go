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

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	perasauth "github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

const (
	defaultPerasSegmentWitnessRetries      = 3
	defaultPerasSegmentWitnessRetryBackoff = 20 * time.Millisecond
	defaultPerasSegmentBatchSize           = 512
	defaultPerasSegmentMaxReplayMutations  = 4096
	// A materialized drain expands each replay mutation into MVCC records.
	// Keep the request below the local write-batch entry cap while preserving
	// large catalog-only segments on the common path.
	defaultPerasMaterializeMaxReplayMutations = 20
	defaultPerasSegmentFlushEvery             = 20 * time.Millisecond
	defaultPerasBackgroundFlushTimeout        = 30 * time.Second
	defaultPerasBackgroundErrorBackoff        = time.Second
	defaultPerasSegmentInstallRetries         = 24
	defaultPerasSegmentInstallRetryBackoff    = 10 * time.Millisecond
	defaultPerasSegmentInstallMaxBackoff      = 500 * time.Millisecond
	defaultPerasInstallTimestampRetries       = 6
	defaultPerasInstallTimestampBackoff       = 2 * time.Millisecond
)

type perasGrantProvider interface {
	HolderID() string
	Acquire(context.Context, compile.AuthorityScope) (perasauth.AuthorityGrant, bool, error)
}

type perasSegmentInstaller interface {
	InstallPerasSegment(context.Context, compile.AuthorityScope, fsperas.PerasSegment, []byte, [32]byte, bool) error
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
	closed    atomic.Bool
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
	scope       compile.AuthorityScope
	plan        fsperas.ReplayPlan
	segment     fsperas.PerasSegment
	payload     []byte
	digest      [32]byte
	materialize bool
}

type runtimePerasFlushBatch struct {
	holder *fsperas.Holder
	plan   fsperas.ReplayPlan
	jobs   []runtimePerasFlushJob
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
	if c.closed.Load() {
		return fsperas.VisibleAck{}, errPerasCommitterClosed
	}
	c.commitMu.RLock()
	defer c.commitMu.RUnlock()
	if c.closed.Load() {
		return fsperas.VisibleAck{}, errPerasCommitterClosed
	}
	grant, owned, err := c.authority.Acquire(ctx, delta.Authority)
	if err != nil {
		c.recordError(err)
		return fsperas.VisibleAck{}, err
	}
	if !owned {
		c.recordError(errPerasAuthorityNotHeld)
		return fsperas.VisibleAck{}, errPerasAuthorityNotHeld
	}
	holder, err := c.holderForGrant(ctx, grant, delta.Authority)
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

func (c *RemotePerasCommitter) holderForGrant(ctx context.Context, grant perasauth.AuthorityGrant, scope compile.AuthorityScope) (*fsperas.Holder, error) {
	if !grant.Valid() || grant.HolderID != c.authority.HolderID() {
		return nil, errPerasCommitterInvalid
	}
	c.holdersMu.Lock()
	if holder := c.holders[grant.EpochID]; holder != nil {
		c.holdersMu.Unlock()
		return holder, nil
	}
	c.holdersMu.Unlock()

	if grantHasPredecessor(grant) && c.installer != nil {
		recoveryScope := perasAuthorityScopeFromGrant(grant)
		if authorityScopeEmpty(recoveryScope) {
			recoveryScope = scope
		}
		if err := c.RecoverWitnessSegments(ctx, recoveryScope, grant.EpochID-1); err != nil {
			return nil, err
		}
	}

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{
		EpochID:  grant.EpochID,
		HolderID: grant.HolderID,
	})
	if err != nil {
		return nil, err
	}
	c.holdersMu.Lock()
	defer c.holdersMu.Unlock()
	if current := c.holders[grant.EpochID]; current != nil {
		return current, nil
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

	batches, err := c.freezeFlushBatchesLocked(nil, true, 0)
	if err != nil {
		return err
	}
	if err := c.installFlushBatches(ctx, batches); err != nil {
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
		if !perasSegmentWithinScope(segment, scope) {
			continue
		}
		stats := segment.Stats()
		if record.OperationCount != stats.OperationCount || record.EntryCount != stats.EntryCount {
			return c.recordError(fsperas.ErrInvalidWitnessRecord)
		}
		if err := c.installer.InstallPerasSegment(ctx, scope, segment, record.SegmentPayload, record.SegmentPayloadDigest, false); err != nil {
			return c.recordErrorf("recover peras segment install: %w", err)
		}
		c.installSegment(fsperas.ReplayPlan{}, segment)
	}
	return nil
}

func (c *RemotePerasCommitter) flushLocked(ctx context.Context, scope *compile.AuthorityScope) error {
	c.commitMu.Lock()
	batches, err := c.freezeFlushBatchesLocked(scope, false, 0)
	c.commitMu.Unlock()
	if err != nil {
		return err
	}
	return c.installFlushBatches(ctx, batches)
}

func (c *RemotePerasCommitter) installFlushBatches(ctx context.Context, batches []runtimePerasFlushBatch) error {
	if len(batches) > 0 && c.installer == nil {
		return c.recordError(errPerasCommitterInvalid)
	}
	for _, batch := range batches {
		if err := c.installFlushBatchJobs(ctx, batch); err != nil {
			return err
		}
		if err := batch.holder.MarkReplayPlanApplied(batch.plan); err != nil {
			return c.recordErrorf("mark peras plan applied: %w", err)
		}
		for _, job := range batch.jobs {
			c.installSegment(job.plan, job.segment)
		}
	}
	return nil
}

func (c *RemotePerasCommitter) installFlushBatchJobs(ctx context.Context, batch runtimePerasFlushBatch) error {
	for _, job := range batch.jobs {
		if err := c.installOneFlushJob(ctx, batch.holder, job); err != nil {
			return err
		}
	}
	return nil
}

func (c *RemotePerasCommitter) installOneFlushJob(ctx context.Context, holder *fsperas.Holder, job runtimePerasFlushJob) error {
	if err := c.appendSegmentWitnessesWithRetry(ctx, job.scope, holder, job.segment, job.payload, job.digest); err != nil {
		return c.recordErrorf("append peras segment witness: %w", err)
	}
	if err := c.installSegmentWithRetry(ctx, job); err != nil {
		return c.recordErrorf("install peras segment: %w", err)
	}
	c.flushTotal.Add(1)
	return nil
}

func (c *RemotePerasCommitter) installSegmentWithRetry(ctx context.Context, job runtimePerasFlushJob) error {
	var last error
	for attempt := 0; attempt <= defaultPerasSegmentInstallRetries; attempt++ {
		err := c.installer.InstallPerasSegment(ctx, job.scope, job.segment, job.payload, job.digest, job.materialize)
		if err == nil {
			return nil
		}
		last = err
		if !nokverrors.Retryable(err) || attempt == defaultPerasSegmentInstallRetries {
			break
		}
		c.retryTotal.Add(1)
		delay := defaultPerasSegmentInstallRetryBackoff << attempt
		if delay > defaultPerasSegmentInstallMaxBackoff {
			delay = defaultPerasSegmentInstallMaxBackoff
		}
		if !sleepContext(ctx, delay) {
			return ctx.Err()
		}
	}
	return last
}

func (c *RemotePerasCommitter) retireDrainedAuthority(ctx context.Context, retirer fsperas.AuthorityRetirer, scopes ...compile.AuthorityScope) error {
	if err := retirer.RetirePerasAuthority(ctx, scopes...); err != nil {
		return c.recordErrorf("retire peras authority: %w", err)
	}
	return nil
}

func (c *RemotePerasCommitter) freezeFlushBatchesLocked(target *compile.AuthorityScope, materialize bool, maxOpsPerHolder int) ([]runtimePerasFlushBatch, error) {
	plans, err := c.freezeReplayPlansLocked(target, maxOpsPerHolder)
	if err != nil {
		return nil, err
	}
	batches := make([]runtimePerasFlushBatch, 0, len(plans))
	for _, frozen := range plans {
		bucketPlans, err := fsperas.SplitReplayPlanByFSMetaBucket(frozen.plan)
		if err != nil {
			return nil, c.recordErrorf("split peras replay plan: %w", err)
		}
		batch := runtimePerasFlushBatch{
			holder: frozen.holder,
			plan:   frozen.plan,
			jobs:   make([]runtimePerasFlushJob, 0, len(bucketPlans)),
		}
		for _, bucketPlan := range bucketPlans {
			sized, err := fsperas.SplitReplayPlanByMutationBudget(bucketPlan, c.replayMutationBudget(materialize))
			if err != nil {
				return nil, c.recordErrorf("split peras replay plan by install budget: %w", err)
			}
			for _, plan := range sized {
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
				batch.jobs = append(batch.jobs, runtimePerasFlushJob{
					scope:       frozen.scope,
					plan:        plan,
					segment:     segment,
					payload:     payload,
					digest:      digest,
					materialize: materialize,
				})
			}
		}
		if len(batch.jobs) > 0 {
			batches = append(batches, batch)
		}
	}
	return batches, nil
}

func (c *RemotePerasCommitter) replayMutationBudget(materialize bool) int {
	if !materialize {
		return c.maxReplay
	}
	if c.maxReplay > 0 && c.maxReplay < defaultPerasMaterializeMaxReplayMutations {
		return c.maxReplay
	}
	return defaultPerasMaterializeMaxReplayMutations
}

func (c *RemotePerasCommitter) freezeReplayPlansLocked(target *compile.AuthorityScope, maxOpsPerHolder int) ([]runtimePerasFrozenPlan, error) {
	holders := c.holderSnapshot()
	plans := make([]runtimePerasFrozenPlan, 0, len(holders))
	for _, holder := range holders {
		plan, scope, ok, err := c.buildFlushPlan(holder, target, maxOpsPerHolder)
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

func (c *RemotePerasCommitter) buildFlushPlan(holder *fsperas.Holder, target *compile.AuthorityScope, maxOps int) (fsperas.ReplayPlan, compile.AuthorityScope, bool, error) {
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
	plan, scope, err := holder.BuildPendingReplayPlanLimit(0, maxOps)
	if err != nil {
		return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, c.recordErrorf("build peras replay plan: %w", err)
	}
	if maxOps <= 0 && len(plan.Operations) != len(pending) {
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

func perasAuthorityScopeFromGrant(grant perasauth.AuthorityGrant) compile.AuthorityScope {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID(grant.Scope.MountID),
		MountKeyID: fsmeta.MountKeyID(grant.Scope.MountKeyID),
		Parents:    rootInodesToFSMeta(grant.Scope.Parents),
		Inodes:     rootInodesToFSMeta(grant.Scope.Inodes),
	}
	if len(grant.Scope.Buckets) > 0 {
		scope.Buckets = make([]fsmeta.AffinityBucket, len(grant.Scope.Buckets))
		for i, bucket := range grant.Scope.Buckets {
			scope.Buckets[i] = fsmeta.AffinityBucket(bucket)
		}
	}
	return scope
}

func grantHasPredecessor(grant perasauth.AuthorityGrant) bool {
	var zero [32]byte
	return grant.EpochID > 1 && grant.PredecessorDigest != zero
}

func rootInodesToFSMeta(in []uint64) []fsmeta.InodeID {
	if len(in) == 0 {
		return nil
	}
	out := make([]fsmeta.InodeID, len(in))
	for i, inode := range in {
		out[i] = fsmeta.InodeID(inode)
	}
	return out
}

func perasSegmentWithinScope(segment fsperas.PerasSegment, scope compile.AuthorityScope) bool {
	if authorityScopeEmpty(scope) {
		return true
	}
	checked := false
	for _, entry := range segment.EntriesView() {
		parts, ok := fsmeta.InspectKey(entry.Key)
		if !ok {
			if checked {
				return false
			}
			continue
		}
		checked = true
		if !perasScopeCoversKeyParts(scope, parts) {
			return false
		}
	}
	return true
}

func authorityScopeEmpty(scope compile.AuthorityScope) bool {
	return scope.Mount == "" || scope.MountKeyID == 0
}

func perasScopeCoversKeyParts(scope compile.AuthorityScope, parts fsmeta.KeyParts) bool {
	if scope.MountKeyID == 0 || parts.MountKeyID != scope.MountKeyID {
		return false
	}
	if len(scope.Buckets) > 0 && !slices.Contains(scope.Buckets, parts.Bucket) {
		return false
	}
	switch parts.Kind {
	case fsmeta.KeyKindDentry:
		return len(scope.Parents) == 0 || slices.Contains(scope.Parents, parts.Parent)
	case fsmeta.KeyKindInode, fsmeta.KeyKindChunk, fsmeta.KeyKindSession:
		return len(scope.Inodes) == 0 || slices.Contains(scope.Inodes, parts.Inode)
	case fsmeta.KeyKindUsage:
		return len(scope.Parents) == 0 || slices.Contains(scope.Parents, parts.UsageScope)
	default:
		return true
	}
}

func (c *RemotePerasCommitter) flushBackground() {
	if c == nil {
		return
	}
	reschedule := false
	defer func() {
		c.bgRunning.Store(false)
		if reschedule {
			c.triggerBackgroundFlush()
		}
	}()
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
	c.commitMu.Lock()
	batches, err := c.freezeFlushBatchesLocked(nil, false, c.batchSize)
	c.commitMu.Unlock()
	if err == nil {
		err = c.installFlushBatches(ctx, batches)
	}
	if err != nil {
		c.bgErrorTotal.Add(1)
		if c.bgBackoff > 0 {
			c.bgNext.Store(c.now().Add(c.bgBackoff).UnixNano())
		}
		return
	}
	c.bgNext.Store(0)
	reschedule = c.pendingOperations() > 0
}

func (c *RemotePerasCommitter) pendingOperations() int {
	if c == nil {
		return 0
	}
	total := 0
	for _, holder := range c.holderSnapshot() {
		total += holder.Pending()
	}
	return total
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
	c.closed.Store(true)
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
}

func (c *RemotePerasCommitter) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.Close()
	if ctx == nil {
		ctx = context.Background()
	}
	return c.Flush(ctx)
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
	router *fsmetawatch.Router
}

func newRunnerPerasSegmentInstaller(runner *Runner, router *fsmetawatch.Router) *runnerPerasSegmentInstaller {
	return &runnerPerasSegmentInstaller{runner: runner, router: router}
}

func (i *runnerPerasSegmentInstaller) InstallPerasSegment(ctx context.Context, _ compile.AuthorityScope, segment fsperas.PerasSegment, payload []byte, digest [32]byte, materialize bool) error {
	if i == nil || i.runner == nil || i.runner.kv == nil {
		return errPerasCommitterInvalid
	}
	kv, ok := i.runner.kv.(perasSegmentInstallClient)
	if !ok {
		return errPerasCommitterInvalid
	}
	routingKey, err := segment.FirstKey()
	if err != nil {
		return err
	}
	installVersion, err := i.reserveInstallVersion(ctx)
	if err != nil {
		return err
	}
	resp, err := kv.InstallPerasSegment(ctx, routingKey, &kvrpcpb.PerasInstallSegmentRequest{
		RoutingKey:           runtimeCloneBytes(routingKey),
		SegmentRoot:          append([]byte(nil), segment.Root[:]...),
		SegmentPayloadDigest: append([]byte(nil), digest[:]...),
		SegmentPayload:       append([]byte(nil), payload...),
		InstallVersion:       installVersion,
		MaterializeMvcc:      materialize,
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
	if err := validatePerasSegmentInstallResponse(segment, resp); err != nil {
		return err
	}
	if !materialize {
		i.publishInstalledSegment(segment, resp)
	}
	return nil
}

func (i *runnerPerasSegmentInstaller) reserveInstallVersion(ctx context.Context) (uint64, error) {
	var last error
	for attempt := 0; attempt <= defaultPerasInstallTimestampRetries; attempt++ {
		version, err := i.runner.ReserveTimestamp(ctx, 1)
		if err == nil {
			return version, nil
		}
		if !nokverrors.Retryable(err) {
			return 0, err
		}
		last = err
		if attempt == defaultPerasInstallTimestampRetries {
			break
		}
		timer := time.NewTimer(defaultPerasInstallTimestampBackoff << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
	return 0, last
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

func (i *runnerPerasSegmentInstaller) publishInstalledSegment(segment fsperas.PerasSegment, resp *kvrpcpb.PerasInstallSegmentResponse) {
	if i == nil || i.router == nil || resp == nil || resp.GetRegionId() == 0 || resp.GetIndex() == 0 {
		return
	}
	commitVersion := resp.GetCommitVersion()
	if commitVersion == 0 {
		return
	}
	cursor := fsmeta.WatchCursor{
		RegionID: resp.GetRegionId(),
		Term:     resp.GetTerm(),
		Index:    resp.GetIndex(),
	}
	for _, entry := range segment.Dentries {
		if len(entry.Key) == 0 {
			continue
		}
		i.router.Publish(fsmeta.WatchEvent{
			Cursor:        cursor,
			CommitVersion: commitVersion,
			Source:        fsmeta.WatchEventSourceCommit,
			Key:           entry.Key,
		})
	}
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
