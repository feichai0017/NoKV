package mvcc

import (
	"context"
	"sync"
	"time"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	txnstore "github.com/feichai0017/NoKV/percolator/storage"
	"github.com/feichai0017/NoKV/utils"
)

const GCPlanTaskName = "mvcc-gc.plan"

// GCPlanSnapshot exposes the last read-only background MVCC GC plan.
type GCPlanSnapshot struct {
	utils.PeriodicTaskSnapshot
	SkippedRuns  uint64
	LastTxnFloor TxnFloor
	LastPlan     PlanStats
}

// GCPlanConfig wires the read-only MVCC GC planner into DB background
// services. MVCCStore is the only required data-plane surface; callers may pass
// a wider concrete type, but the planner only uses percolator/storage.Store.
type GCPlanConfig struct {
	MVCCStore txnstore.Store
	Interval  time.Duration
	SafePoint func() uint64
	Retention func() rootstate.SnapshotRetentionIndex
	Mount     MountResolver
}

// GCPlanner owns the last observed read-only MVCC GC plan for one DB.
type GCPlanner struct {
	store     txnstore.Store
	safePoint func() uint64
	retention func() rootstate.SnapshotRetentionIndex
	mount     MountResolver

	mu       sync.RWMutex
	skipped  uint64
	txnFloor TxnFloor
	plan     PlanStats
}

func NewGCPlanTask(cfg GCPlanConfig) (utils.PeriodicTaskConfig, *GCPlanner, bool) {
	if cfg.MVCCStore == nil || cfg.Interval <= 0 || cfg.SafePoint == nil {
		return utils.PeriodicTaskConfig{}, nil, false
	}
	planner := &GCPlanner{
		store:     cfg.MVCCStore,
		safePoint: cfg.SafePoint,
		retention: cfg.Retention,
		mount:     cfg.Mount,
	}
	return utils.PeriodicTaskConfig{
		Name:     GCPlanTaskName,
		Interval: cfg.Interval,
		Run:      planner.run,
	}, planner, true
}

func (s *GCPlanner) run(ctx context.Context) error {
	requestedSafePoint := s.safePoint()
	if requestedSafePoint == 0 {
		s.recordSkipped()
		return nil
	}
	txnFloor, err := PlanTxnFloor(ctx, s.store)
	if err != nil {
		s.record(txnFloor, PlanStats{})
		return err
	}
	var retention rootstate.SnapshotRetentionIndex
	if s.retention != nil {
		retention = s.retention()
	}
	plan, err := Plan(ctx, s.store, SafePointPolicy{
		RequestedSafePoint: requestedSafePoint,
		SnapshotRetention:  retention,
		TxnFloor:           txnFloor.OldestStartTs,
		Mount:              s.mount,
	})
	s.record(txnFloor, plan)
	return err
}

func (s *GCPlanner) Snapshot(task utils.PeriodicTaskSnapshot) GCPlanSnapshot {
	if s == nil {
		return GCPlanSnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return GCPlanSnapshot{
		PeriodicTaskSnapshot: task,
		SkippedRuns:          s.skipped,
		LastTxnFloor:         s.txnFloor,
		LastPlan:             s.plan,
	}
}

func (s *GCPlanner) record(txnFloor TxnFloor, plan PlanStats) {
	s.mu.Lock()
	s.txnFloor = txnFloor
	s.plan = plan
	s.mu.Unlock()
}

func (s *GCPlanner) recordSkipped() {
	s.mu.Lock()
	s.skipped++
	s.mu.Unlock()
}
