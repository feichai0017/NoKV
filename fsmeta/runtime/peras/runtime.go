package peras

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/feichai0017/NoKV/utils"
)

const (
	defaultPerasSegmentWitnessRetries      = 3
	defaultPerasSegmentWitnessRetryBackoff = 20 * time.Millisecond
	defaultPerasSegmentBatchSize           = 512
	defaultPerasSegmentMaxReplayOperations = 512
	defaultPerasSegmentMaxReplayMutations  = 4096
	defaultPerasSegmentMaxPayloadBytes     = 512 << 10
	defaultPerasSegmentCatalogScanLimit    = 128
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
	defaultPerasSegmentInstallStaleBackoff    = time.Millisecond
	defaultPerasSegmentInstallStaleMaxBackoff = 10 * time.Millisecond
	defaultPerasVisibleWatchQueue             = 65536
)

type Config struct {
	Authority                  GrantProvider
	Witnesses                  []fsperas.WitnessReplica
	Installer                  SegmentInstaller
	CatalogScanner             SegmentCatalogScanner
	WatchPublisher             perasWatchPublisher
	Quorum                     int
	SegmentWitnessRetries      int
	SegmentWitnessRetryBackoff time.Duration
	SegmentBatchSize           int
	SegmentMaxReplayOperations int
	SegmentMaxReplayMutations  int
	SegmentMaxPayloadBytes     uint64
	SegmentFlushEvery          time.Duration
	BackgroundFlushTimeout     time.Duration
	BackgroundErrorBackoff     time.Duration
	SegmentInstallParallelism  int
	Now                        func() time.Time
}

// Runtime is the fsmeta runtime bridge from compiler deltas to
// remote durable witnesses. It keeps an in-process read overlay until segment
// apply lands.
type Runtime struct {
	authority  GrantProvider
	seals      SealProvider
	witnesses  []fsperas.WitnessReplica
	installer  SegmentInstaller
	catalog    SegmentCatalogScanner
	watch      perasWatchPublisher
	quorum     int
	retries    int
	backoff    time.Duration
	batchSize  int
	maxOps     int
	maxReplay  int
	maxPayload uint64
	installN   int
	flushEvery time.Duration
	bgTimeout  time.Duration
	bgBackoff  time.Duration
	now        func() time.Time

	commitMu   sync.RWMutex
	flushMu    sync.Mutex
	bgLaunchMu sync.Mutex
	bgRunning  atomic.Bool
	bgNext     atomic.Int64
	visibleSeq atomic.Uint64
	closed     atomic.Bool
	closer     *utils.Closer
	flushTask  *utils.PeriodicTask
	watchQueue *utils.MPSCQueue[fsmeta.WatchEvent]
	installQ   *perasInstallLane
	sealQ      *perasSealLane

	// Lock order for multi-lock paths:
	// commitMu -> flushMu -> drainMu -> epochTable.mu -> readState.mu -> runtimeMetrics.statsMu.
	// Lane workers communicate through queues and must not take commitMu.
	drainMu     sync.Mutex
	drainCond   *sync.Cond
	drainNextID uint64
	drainUses   []perasAuthorityUse
	drainScopes []compile.AuthorityScope

	epochs  *epochTable
	latches *fsperas.AdmissionLatches

	read *readState

	metrics runtimeMetrics
}

type perasCompletion struct {
	epochID    uint64
	completion fsperas.SegmentCompletion
}

type perasFlushJob struct {
	scope       compile.AuthorityScope
	plan        fsperas.ReplayPlan
	segment     fsperas.PerasSegment
	payload     []byte
	digest      [32]byte
	materialize bool
	cursor      InstallCursor
}

type perasFlushBatch struct {
	holder *fsperas.Holder
	plan   fsperas.ReplayPlan
	jobs   []perasFlushJob
}

type perasFrozenPlan struct {
	holder *fsperas.Holder
	scope  compile.AuthorityScope
	plan   fsperas.ReplayPlan
}

func NewRuntime(cfg Config) (*Runtime, error) {
	if cfg.Authority == nil || cfg.Authority.HolderID() == "" || len(cfg.Witnesses) == 0 {
		return nil, ErrRuntimeInvalid
	}
	witnesses := make([]fsperas.WitnessReplica, 0, len(cfg.Witnesses))
	seen := make(map[string]struct{}, len(cfg.Witnesses))
	for _, witness := range cfg.Witnesses {
		if witness == nil || witness.ID() == "" {
			return nil, ErrRuntimeInvalid
		}
		if _, ok := seen[witness.ID()]; ok {
			return nil, ErrRuntimeInvalid
		}
		seen[witness.ID()] = struct{}{}
		witnesses = append(witnesses, witness)
	}
	quorum := cfg.Quorum
	if quorum == 0 {
		quorum = len(witnesses)/2 + 1
	}
	if quorum <= 0 || quorum > len(witnesses) {
		return nil, ErrRuntimeInvalid
	}
	retries := cfg.SegmentWitnessRetries
	if retries == 0 {
		retries = defaultPerasSegmentWitnessRetries
	}
	if retries < 0 {
		return nil, ErrRuntimeInvalid
	}
	backoff := cfg.SegmentWitnessRetryBackoff
	if backoff == 0 {
		backoff = defaultPerasSegmentWitnessRetryBackoff
	}
	if backoff < 0 {
		return nil, ErrRuntimeInvalid
	}
	batchSize := cfg.SegmentBatchSize
	if batchSize == 0 {
		batchSize = defaultPerasSegmentBatchSize
	}
	if batchSize < 0 {
		return nil, ErrRuntimeInvalid
	}
	maxOps := cfg.SegmentMaxReplayOperations
	if maxOps == 0 {
		maxOps = defaultPerasSegmentMaxReplayOperations
	}
	if maxOps < 0 {
		return nil, ErrRuntimeInvalid
	}
	maxReplay := cfg.SegmentMaxReplayMutations
	if maxReplay == 0 {
		maxReplay = defaultPerasSegmentMaxReplayMutations
	}
	if maxReplay < 0 {
		return nil, ErrRuntimeInvalid
	}
	maxPayload := cfg.SegmentMaxPayloadBytes
	if maxPayload == 0 {
		maxPayload = defaultPerasSegmentMaxPayloadBytes
	}
	installN := cfg.SegmentInstallParallelism
	if installN == 0 {
		installN = defaultPerasSegmentInstallParallelism()
	}
	if installN < 0 {
		return nil, ErrRuntimeInvalid
	}
	flushEvery := cfg.SegmentFlushEvery
	if flushEvery == 0 {
		flushEvery = defaultPerasSegmentFlushEvery
	}
	if flushEvery < 0 {
		return nil, ErrRuntimeInvalid
	}
	bgTimeout := cfg.BackgroundFlushTimeout
	if bgTimeout == 0 {
		bgTimeout = defaultPerasBackgroundFlushTimeout
	}
	if bgTimeout < 0 {
		return nil, ErrRuntimeInvalid
	}
	bgBackoff := cfg.BackgroundErrorBackoff
	if bgBackoff == 0 {
		bgBackoff = defaultPerasBackgroundErrorBackoff
	}
	if bgBackoff < 0 {
		return nil, ErrRuntimeInvalid
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	seals, _ := cfg.Authority.(SealProvider)
	c := &Runtime{
		authority:  cfg.Authority,
		seals:      seals,
		witnesses:  witnesses,
		installer:  cfg.Installer,
		catalog:    cfg.CatalogScanner,
		watch:      cfg.WatchPublisher,
		quorum:     quorum,
		retries:    retries,
		backoff:    backoff,
		batchSize:  batchSize,
		maxOps:     maxOps,
		maxReplay:  maxReplay,
		maxPayload: maxPayload,
		installN:   installN,
		flushEvery: flushEvery,
		bgTimeout:  bgTimeout,
		bgBackoff:  bgBackoff,
		now:        now,
		closer:     utils.NewCloser(),
		epochs:     newEpochTable(),
		latches:    fsperas.NewAdmissionLatches(),
		read:       newReadState(),
	}
	c.drainCond = sync.NewCond(&c.drainMu)
	if c.installer != nil {
		c.installQ = newPerasInstallLane(c, c.installN)
	}
	if _, ok := c.authority.(SealPublisher); ok {
		c.sealQ = newPerasSealLane(c, c.installN)
	}
	if c.watch != nil {
		c.watchQueue = utils.NewMPSCQueue[fsmeta.WatchEvent](defaultPerasVisibleWatchQueue)
		c.closer.Add(1)
		go c.visibleWatchLoop()
	}
	if c.flushEvery > 0 {
		var skipFirst atomic.Bool
		skipFirst.Store(true)
		c.flushTask = utils.NewPeriodicTask(utils.PeriodicTaskConfig{
			Name:     "peras-segment-flush",
			Interval: c.flushEvery,
			Run: func(context.Context) error {
				if skipFirst.Swap(false) {
					return nil
				}
				c.triggerBackgroundFlush()
				return nil
			},
		})
		c.flushTask.Start()
	}
	return c, nil
}

func (c *Runtime) SubmitVisible(ctx context.Context, id fsperas.OperationID, op compile.MaterializedOp, admission fsperas.AdmissionFunc) (fsperas.VisibleAck, error) {
	if c == nil || c.authority == nil {
		return fsperas.VisibleAck{}, ErrRuntimeInvalid
	}
	if c.closed.Load() {
		return fsperas.VisibleAck{}, ErrRuntimeClosed
	}
	if err := op.ValidateForAdmissionIntent(); err != nil {
		return fsperas.VisibleAck{}, fsperas.ErrIneligibleOperation
	}
	if !op.Placement.CanSegment {
		return fsperas.VisibleAck{}, fsperas.ErrIneligibleOperation
	}
	delta := op.Delta
	leaveAuthority := c.enterAuthority(delta.Authority)
	defer leaveAuthority()
	c.commitMu.RLock()
	defer c.commitMu.RUnlock()
	if c.closed.Load() {
		return fsperas.VisibleAck{}, ErrRuntimeClosed
	}
	if completion, ok := c.completionForOperation(id); ok {
		return fsperas.VisibleAck{EpochID: completion.epochID, OpID: id, HolderID: c.authority.HolderID()}, nil
	}
	grant, owned, err := c.authority.Acquire(ctx, delta.Authority)
	if err != nil {
		c.recordError(err)
		return fsperas.VisibleAck{}, err
	}
	if !owned {
		c.recordError(ErrNotHeld)
		return fsperas.VisibleAck{}, ErrNotHeld
	}
	holder, err := c.holderForGrant(ctx, grant, delta.Authority)
	if err != nil {
		c.recordError(err)
		return fsperas.VisibleAck{}, err
	}
	if ack, ok, err := holder.PendingAck(id, op); ok || err != nil {
		if err != nil {
			c.recordError(err)
		}
		return ack, err
	}
	unlockAdmission := c.latches.Lock(op)
	defer unlockAdmission()
	admitted, err := fsperas.AdmitAndSeal(ctx, op, admission)
	if err != nil {
		if !errors.Is(err, fsperas.ErrAdmissionRejected) && !isAdmissionTerminalError(err) {
			c.recordError(err)
		}
		return fsperas.VisibleAck{}, err
	}
	op = admitted
	ack, err := holder.Submit(ctx, id, op)
	if err != nil {
		c.recordError(err)
		return fsperas.VisibleAck{}, err
	}
	if err := c.addOverlay(id, op); err != nil {
		holder.MarkAppliedIDs(id)
		c.recordError(err)
		return fsperas.VisibleAck{}, err
	}
	c.publishVisibleWatch(op, ack)
	c.metrics.commitTotal.Add(1)
	if c.batchSize > 0 && holder.Pending() >= c.batchSize {
		c.triggerBackgroundFlush()
	}
	return ack, nil
}

func (c *Runtime) holderForGrant(ctx context.Context, grant AuthorityGrant, scope compile.AuthorityScope) (*fsperas.Holder, error) {
	if !grant.Valid() || grant.HolderID != c.authority.HolderID() {
		return nil, ErrRuntimeInvalid
	}
	if holder, ok := c.epochs.holder(grant); ok {
		return holder, nil
	}

	recoveryScope := scope
	if GrantHasPredecessor(grant) {
		if grantScope := ScopeFromGrant(grant); !ScopeEmpty(grantScope) {
			recoveryScope = grantScope
		}
	}
	if err := c.LoadRootSealedSegments(ctx, recoveryScope); err != nil {
		return nil, err
	}
	if GrantHasPredecessor(grant) && c.installer != nil {
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
	return c.epochs.installHolder(grant, holder), nil
}

func (c *Runtime) FlushDurable(ctx context.Context) error {
	return c.FlushTo(ctx, fsperas.SegmentPersistenceDurable)
}

func (c *Runtime) FlushPublished(ctx context.Context) error {
	return c.FlushTo(ctx, fsperas.SegmentPersistencePublished)
}

func (c *Runtime) FlushTo(ctx context.Context, level fsperas.SegmentPersistenceLevel) error {
	return c.flush(ctx, nil, level)
}

func (c *Runtime) FlushAuthority(ctx context.Context, scope compile.AuthorityScope) error {
	return c.FlushAuthorityTo(ctx, scope, fsperas.SegmentPersistenceDurable)
}

func (c *Runtime) FlushAuthorityPublished(ctx context.Context, scope compile.AuthorityScope) error {
	return c.FlushAuthorityTo(ctx, scope, fsperas.SegmentPersistencePublished)
}

func (c *Runtime) FlushAuthorityTo(ctx context.Context, scope compile.AuthorityScope, level fsperas.SegmentPersistenceLevel) error {
	if scope.Mount == "" || scope.MountKeyID == 0 {
		return c.FlushTo(ctx, level)
	}
	return c.flush(ctx, &scope, level)
}

func (c *Runtime) flush(ctx context.Context, scope *compile.AuthorityScope, level fsperas.SegmentPersistenceLevel) error {
	if c == nil {
		return ErrRuntimeInvalid
	}
	level = fsperas.NormalizeSegmentPersistence(level)
	if !level.Valid() {
		return ErrRuntimeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	return c.flushLocked(ctx, scope, level)
}

func defaultPerasSegmentInstallParallelism() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		return 1
	}
	return n
}

func (c *Runtime) flushBackground() {
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
		c.metrics.bgSkipTotal.Add(1)
		return
	}
	if !c.flushMu.TryLock() {
		c.metrics.bgSkipTotal.Add(1)
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
	plans, err := c.freezeReplayPlansLocked(nil, c.batchSize)
	c.commitMu.Unlock()
	var batches []perasFlushBatch
	if err == nil {
		batches, err = c.buildFlushBatches(plans, false)
	}
	if err == nil {
		err = (flushPipeline{runtime: c, level: fsperas.SegmentPersistencePublished}).run(ctx, batches)
	}
	if err != nil {
		c.metrics.bgErrorTotal.Add(1)
		if c.bgBackoff > 0 {
			c.bgNext.Store(c.now().Add(c.bgBackoff).UnixNano())
		}
		return
	}
	c.bgNext.Store(0)
	reschedule = c.pendingOperations() > 0
}

func (c *Runtime) pendingOperations() int {
	if c == nil {
		return 0
	}
	total := 0
	for _, holder := range c.holderSnapshot() {
		total += holder.Pending()
	}
	return total
}

func (c *Runtime) Close() {
	if c == nil {
		return
	}
	c.closed.Store(true)
	if c.flushTask != nil {
		c.flushTask.Close()
	}
	if c.watchQueue != nil {
		c.watchQueue.Close()
	}
	if c.installQ != nil {
		c.installQ.close()
	}
	if c.sealQ != nil {
		c.sealQ.close()
	}
	c.bgLaunchMu.Lock()
	c.bgLaunchMu.Unlock()
	if c.closer != nil {
		c.closer.Close()
	}
}

func (c *Runtime) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := c.FlushPublished(ctx)
	c.Close()
	return err
}

func (c *Runtime) triggerBackgroundFlush() {
	if c == nil {
		return
	}
	c.bgLaunchMu.Lock()
	defer c.bgLaunchMu.Unlock()
	if c.closed.Load() {
		return
	}
	if !c.bgRunning.CompareAndSwap(false, true) {
		c.metrics.bgSkipTotal.Add(1)
		return
	}
	if c.closer != nil {
		c.closer.Add(1)
	}
	go func() {
		defer func() {
			if c.closer != nil {
				c.closer.Done()
			}
		}()
		c.flushBackground()
	}()
}
