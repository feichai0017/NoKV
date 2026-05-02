package mvcc

import (
	"context"
	"errors"
	"sync"
	"time"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	txnstore "github.com/feichai0017/NoKV/percolator/storage"
	dbruntime "github.com/feichai0017/NoKV/runtime"
)

const MaintenanceTaskName = "mvcc-maintenance"

// MaintenanceWorkerConfig wires replicated MVCC maintenance for one
// raftstore node. The worker never writes through the local Store surface:
// every destructive mutation must pass through the supplied raft proposers.
type MaintenanceWorkerConfig struct {
	MVCCStore            txnstore.Store
	MaintenanceProposer  MaintenanceProposer
	LockResolverProposer LockResolverProposer

	Interval time.Duration
	Timeout  time.Duration

	SafePoint func() uint64
	CurrentTs func() uint64
	Retention func() rootstate.SnapshotRetentionIndex
	Mount     MountResolver

	Apply        ApplyOptions
	ResolveLocks ResolveLocksOptions

	RunOrphanDefaults bool
	OrphanDefaults    OrphanDefaultOptions
}

// MaintenanceSnapshot reports the last replicated maintenance pass.
type MaintenanceSnapshot struct {
	Enabled              bool
	Runs                 uint64
	LastUnix             int64
	LastDurationMs       float64
	LastError            string
	LastResolveError     string
	LastApplyError       string
	LastOrphanError      string
	LastSafePointSkipped bool

	LastResolveLocks   ResolveLocksStats
	LastApply          ApplyStats
	LastOrphanDefaults OrphanDefaultStats
}

// MaintenanceWorker runs replicated MVCC maintenance on a fixed interval.
type MaintenanceWorker struct {
	cfg MaintenanceWorkerConfig

	periodic *dbruntime.PeriodicTask

	mu       sync.RWMutex
	snapshot MaintenanceSnapshot
}

// NewMaintenanceWorker returns a disabled marker when the config is incomplete.
func NewMaintenanceWorker(cfg MaintenanceWorkerConfig) (*MaintenanceWorker, bool) {
	if cfg.MVCCStore == nil || cfg.Interval <= 0 {
		return nil, false
	}
	hasGCApply := cfg.SafePoint != nil && cfg.MaintenanceProposer != nil
	hasLockResolution := cfg.CurrentTs != nil && cfg.LockResolverProposer != nil
	hasOrphanCleanup := cfg.RunOrphanDefaults && cfg.MaintenanceProposer != nil
	if !hasGCApply && !hasLockResolution && !hasOrphanCleanup {
		return nil, false
	}
	worker := &MaintenanceWorker{
		cfg: cfg,
		snapshot: MaintenanceSnapshot{
			Enabled: true,
		},
	}
	worker.periodic = dbruntime.NewPeriodicTask(dbruntime.PeriodicTaskConfig{
		Name:     MaintenanceTaskName,
		Interval: cfg.Interval,
		Run:      worker.RunOnce,
	})
	return worker, true
}

// Start begins the background maintenance loop.
func (w *MaintenanceWorker) Start() {
	if w == nil {
		return
	}
	w.periodic.Start()
}

// Close stops the background maintenance loop.
func (w *MaintenanceWorker) Close() {
	if w == nil {
		return
	}
	w.periodic.Close()
}

// RunOnce executes one replicated maintenance pass. It is exposed so tests and
// future store-level schedulers can run a bounded pass without starting a
// ticker.
func (w *MaintenanceWorker) RunOnce(ctx context.Context) error {
	if w == nil {
		return nil
	}
	start := time.Now()
	var (
		resolveStats     ResolveLocksStats
		applyStats       ApplyStats
		orphanStats      OrphanDefaultStats
		resolveErr       error
		applyErr         error
		orphanErr        error
		safePointSkipped bool
	)
	runCtx := ctx
	cancel := func() {}
	if w.cfg.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, w.cfg.Timeout)
	}
	defer cancel()

	currentTs := uint64(0)
	if w.cfg.CurrentTs != nil {
		currentTs = w.cfg.CurrentTs()
	}
	if currentTs != 0 && w.cfg.LockResolverProposer != nil {
		resolveOpt := w.cfg.ResolveLocks
		resolveOpt.CurrentTs = currentTs
		resolveStats, resolveErr = ResolveExpiredLocksReplicated(runCtx, w.cfg.MVCCStore, w.cfg.LockResolverProposer, resolveOpt)
	}
	if w.cfg.SafePoint != nil && w.cfg.MaintenanceProposer != nil {
		requestedSafePoint := w.cfg.SafePoint()
		if requestedSafePoint != 0 {
			txnFloor, floorErr := PlanTxnFloor(runCtx, w.cfg.MVCCStore)
			if floorErr != nil {
				applyErr = floorErr
			} else {
				var retention rootstate.SnapshotRetentionIndex
				if w.cfg.Retention != nil {
					retention = w.cfg.Retention()
				}
				applyStats, applyErr = ApplyReplicated(runCtx, w.cfg.MVCCStore, w.cfg.MaintenanceProposer, SafePointPolicy{
					RequestedSafePoint: requestedSafePoint,
					TxnFloor:           txnFloor.OldestStartTs,
					SnapshotRetention:  retention,
					Mount:              w.cfg.Mount,
				}, w.cfg.Apply)
			}
		} else {
			safePointSkipped = true
		}
	}
	if w.cfg.RunOrphanDefaults && w.cfg.MaintenanceProposer != nil {
		orphanStats, orphanErr = ApplyOrphanDefaultsReplicated(runCtx, w.cfg.MVCCStore, w.cfg.MaintenanceProposer, w.cfg.OrphanDefaults)
	}

	err := errors.Join(resolveErr, applyErr, orphanErr)
	w.record(start, resolveStats, applyStats, orphanStats, maintenanceStageErrors{
		resolve:          resolveErr,
		apply:            applyErr,
		orphan:           orphanErr,
		joined:           err,
		safePointSkipped: safePointSkipped,
	})
	return err
}

func (w *MaintenanceWorker) Snapshot() MaintenanceSnapshot {
	if w == nil {
		return MaintenanceSnapshot{}
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.snapshot
}

type maintenanceStageErrors struct {
	resolve          error
	apply            error
	orphan           error
	joined           error
	safePointSkipped bool
}

func (w *MaintenanceWorker) record(start time.Time, resolve ResolveLocksStats, apply ApplyStats, orphan OrphanDefaultStats, stage maintenanceStageErrors) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.snapshot.Enabled = true
	w.snapshot.Runs++
	w.snapshot.LastUnix = time.Now().Unix()
	w.snapshot.LastDurationMs = float64(time.Since(start).Microseconds()) / 1000
	if stage.joined != nil {
		w.snapshot.LastError = stage.joined.Error()
	} else {
		w.snapshot.LastError = ""
	}
	w.snapshot.LastResolveError = errorString(stage.resolve)
	w.snapshot.LastApplyError = errorString(stage.apply)
	w.snapshot.LastOrphanError = errorString(stage.orphan)
	w.snapshot.LastSafePointSkipped = stage.safePointSkipped
	w.snapshot.LastResolveLocks = resolve
	w.snapshot.LastApply = apply
	w.snapshot.LastOrphanDefaults = orphan
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
