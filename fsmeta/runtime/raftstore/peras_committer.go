package raftstore

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/feichai0017/NoKV/fsmeta/runtime/perasauthority"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/utils"
)

const (
	defaultPerasSegmentWitnessRetries      = 3
	defaultPerasSegmentWitnessRetryBackoff = 20 * time.Millisecond
	defaultPerasSegmentBatchSize           = 512
	defaultPerasSegmentMaxReplayMutations  = 4096
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

type perasGrantProvider interface {
	HolderID() string
	Acquire(context.Context, compile.AuthorityScope) (perasauthority.AuthorityGrant, bool, error)
}

type perasSealPublisher interface {
	PublishSegmentSeal(context.Context, perasauthority.AuthorityGrant, fsperas.PerasSegment, [32]byte, perasauthority.InstallCursor) error
}

type perasSealProvider interface {
	ListPerasAuthoritySeals(context.Context, compile.AuthorityScope) ([]rootproto.PerasAuthoritySeal, error)
}

type perasSegmentInstaller interface {
	InstallPerasSegment(context.Context, compile.AuthorityScope, fsperas.PerasSegment, []byte, [32]byte, bool) (perasauthority.InstallCursor, error)
}

type perasSegmentCatalogScanner interface {
	Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]fsmetaexec.KV, error)
}

type RemotePerasCommitterConfig struct {
	Authority                  perasGrantProvider
	Witnesses                  []fsperas.WitnessReplica
	Installer                  perasSegmentInstaller
	CatalogScanner             perasSegmentCatalogScanner
	WatchPublisher             perasWatchPublisher
	Quorum                     int
	SegmentWitnessRetries      int
	SegmentWitnessRetryBackoff time.Duration
	SegmentBatchSize           int
	SegmentMaxReplayMutations  int
	SegmentFlushEvery          time.Duration
	BackgroundFlushTimeout     time.Duration
	BackgroundErrorBackoff     time.Duration
	SegmentInstallParallelism  int
	Now                        func() time.Time
}

// RemotePerasCommitter is the fsmeta runtime bridge from compiler deltas to
// remote durable witnesses. It keeps an in-process read overlay until segment
// apply lands.
type RemotePerasCommitter struct {
	authority  perasGrantProvider
	seals      perasSealProvider
	witnesses  []fsperas.WitnessReplica
	installer  perasSegmentInstaller
	catalog    perasSegmentCatalogScanner
	watch      perasWatchPublisher
	quorum     int
	retries    int
	backoff    time.Duration
	batchSize  int
	maxReplay  int
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
	// commitMu -> flushMu -> drainMu -> holdersMu -> overlayMu -> statsMu.
	// Lane workers communicate through queues and must not take commitMu.
	drainMu     sync.Mutex
	drainCond   *sync.Cond
	drainNextID uint64
	drainUses   []perasAuthorityUse
	drainScopes []compile.AuthorityScope

	holdersMu sync.Mutex
	holders   map[uint64]*fsperas.Holder
	grants    map[uint64]perasauthority.AuthorityGrant
	latches   *fsperas.AdmissionLatches

	overlayMu sync.RWMutex
	overlay   *fsperas.OverlayView
	sealed    *fsperas.OverlayView
	segments  []fsperas.PerasSegment
	completed map[fsperas.OperationID]perasCompletion

	commitTotal          atomic.Uint64
	flushTotal           atomic.Uint64
	segmentTotal         atomic.Uint64
	segmentOpsTotal      atomic.Uint64
	segmentEntryTotal    atomic.Uint64
	sealTotal            atomic.Uint64
	flushLatencyTotal    atomic.Uint64
	flushLatencyLast     atomic.Uint64
	flushLatencyMax      atomic.Uint64
	witnessLatencyTotal  atomic.Uint64
	witnessLatencyLast   atomic.Uint64
	witnessLatencyMax    atomic.Uint64
	installLatencyTotal  atomic.Uint64
	installLatencyLast   atomic.Uint64
	installLatencyMax    atomic.Uint64
	installPayloadTotal  atomic.Uint64
	installPayloadLast   atomic.Uint64
	installPayloadMax    atomic.Uint64
	installRoutesTotal   atomic.Uint64
	installRoutesLast    atomic.Uint64
	installRoutesMax     atomic.Uint64
	sealLatencyTotal     atomic.Uint64
	sealLatencyLast      atomic.Uint64
	sealLatencyMax       atomic.Uint64
	flushBatchTotal      atomic.Uint64
	flushJobTotal        atomic.Uint64
	flushJobLast         atomic.Uint64
	flushJobMax          atomic.Uint64
	errorTotal           atomic.Uint64
	retryTotal           atomic.Uint64
	retryUnavailable     atomic.Uint64
	retryRouting         atomic.Uint64
	retryStaleEpoch      atomic.Uint64
	retryOther           atomic.Uint64
	bgSkipTotal          atomic.Uint64
	bgErrorTotal         atomic.Uint64
	catalogLoadTotal     atomic.Uint64
	rootSealTotal        atomic.Uint64
	rootSealMissingTotal atomic.Uint64
	recoveryInstallTotal atomic.Uint64
	recoverySkipTotal    atomic.Uint64

	statsMu          sync.RWMutex
	lastSegmentStats fsperas.SegmentStats
	lastSegmentRoot  [32]byte
	lastError        string
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
	cursor      perasauthority.InstallCursor
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
	installN := cfg.SegmentInstallParallelism
	if installN == 0 {
		installN = defaultPerasSegmentInstallParallelism()
	}
	if installN < 0 {
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
	seals, _ := cfg.Authority.(perasSealProvider)
	c := &RemotePerasCommitter{
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
		maxReplay:  maxReplay,
		installN:   installN,
		flushEvery: flushEvery,
		bgTimeout:  bgTimeout,
		bgBackoff:  bgBackoff,
		now:        now,
		closer:     utils.NewCloser(),
		holders:    make(map[uint64]*fsperas.Holder),
		grants:     make(map[uint64]perasauthority.AuthorityGrant),
		latches:    fsperas.NewAdmissionLatches(),
		overlay:    fsperas.NewOverlayView(),
		sealed:     fsperas.NewOverlayView(),
		completed:  make(map[fsperas.OperationID]perasCompletion),
	}
	c.drainCond = sync.NewCond(&c.drainMu)
	if c.installer != nil {
		c.installQ = newPerasInstallLane(c, c.installN)
	}
	if _, ok := c.authority.(perasSealPublisher); ok {
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

func (c *RemotePerasCommitter) SubmitVisible(ctx context.Context, id fsperas.OperationID, op compile.MaterializedOp, admission fsperas.AdmissionFunc) (fsperas.VisibleAck, error) {
	if c == nil || c.authority == nil {
		return fsperas.VisibleAck{}, errPerasCommitterInvalid
	}
	if c.closed.Load() {
		return fsperas.VisibleAck{}, errPerasCommitterClosed
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
		return fsperas.VisibleAck{}, errPerasCommitterClosed
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
		c.recordError(perasauthority.ErrNotHeld)
		return fsperas.VisibleAck{}, perasauthority.ErrNotHeld
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
		if !errors.Is(err, fsperas.ErrAdmissionRejected) && !isPerasAdmissionTerminalError(err) {
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
	c.commitTotal.Add(1)
	if c.batchSize > 0 && holder.Pending() >= c.batchSize {
		c.triggerBackgroundFlush()
	}
	return ack, nil
}

func (c *RemotePerasCommitter) holderForGrant(ctx context.Context, grant perasauthority.AuthorityGrant, scope compile.AuthorityScope) (*fsperas.Holder, error) {
	if !grant.Valid() || grant.HolderID != c.authority.HolderID() {
		return nil, errPerasCommitterInvalid
	}
	c.holdersMu.Lock()
	if holder := c.holders[grant.EpochID]; holder != nil {
		c.grants[grant.EpochID] = grant
		c.holdersMu.Unlock()
		return holder, nil
	}
	c.holdersMu.Unlock()

	recoveryScope := scope
	if grantHasPredecessor(grant) {
		if grantScope := perasauthority.ScopeFromGrant(grant); !perasauthority.ScopeEmpty(grantScope) {
			recoveryScope = grantScope
		}
	}
	if err := c.LoadRootSealedSegments(ctx, recoveryScope); err != nil {
		return nil, err
	}
	if grantHasPredecessor(grant) && c.installer != nil {
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
		c.grants[grant.EpochID] = grant
		return current, nil
	}
	c.holders[grant.EpochID] = holder
	c.grants[grant.EpochID] = grant
	return holder, nil
}

func (c *RemotePerasCommitter) FlushDurable(ctx context.Context) error {
	return c.flush(ctx, nil)
}

func (c *RemotePerasCommitter) FlushAuthority(ctx context.Context, scope compile.AuthorityScope) error {
	if scope.Mount == "" || scope.MountKeyID == 0 {
		return c.FlushDurable(ctx)
	}
	return c.flush(ctx, &scope)
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

func defaultPerasSegmentInstallParallelism() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		return 1
	}
	return n
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
	plans, err := c.freezeReplayPlansLocked(nil, c.batchSize)
	c.commitMu.Unlock()
	var batches []perasFlushBatch
	if err == nil {
		batches, err = c.buildFlushBatches(plans, false)
	}
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

func (c *RemotePerasCommitter) Close() {
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

func (c *RemotePerasCommitter) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := c.FlushDurable(ctx)
	c.Close()
	return err
}

func (c *RemotePerasCommitter) triggerBackgroundFlush() {
	if c == nil {
		return
	}
	c.bgLaunchMu.Lock()
	defer c.bgLaunchMu.Unlock()
	if c.closed.Load() {
		return
	}
	if !c.bgRunning.CompareAndSwap(false, true) {
		c.bgSkipTotal.Add(1)
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
