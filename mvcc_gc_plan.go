package NoKV

import (
	"context"
	"sync"

	"github.com/feichai0017/NoKV/fsmeta"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/mvccgc"
	dbruntime "github.com/feichai0017/NoKV/runtime"
)

const mvccGCPlanTaskName = "mvcc-gc-plan"

// MVCCGCPlanSnapshot exposes the last read-only background MVCC GC plan.
type MVCCGCPlanSnapshot struct {
	dbruntime.PeriodicTaskSnapshot
	LastTxnFloor mvccgc.TxnFloor
	LastPlan     mvccgc.PlanStats
}

type mvccGCPlanState struct {
	db        *DB
	safePoint func() uint64
	retention func() rootstate.SnapshotRetentionIndex

	mu       sync.RWMutex
	txnFloor mvccgc.TxnFloor
	plan     mvccgc.PlanStats
}

func (db *DB) newMVCCGCPlanTask() (dbruntime.PeriodicTaskConfig, *mvccGCPlanState, bool) {
	if db == nil || db.opt == nil || db.opt.MVCCGCPlanInterval <= 0 || db.opt.MVCCGCSafePoint == nil {
		return dbruntime.PeriodicTaskConfig{}, nil, false
	}
	state := &mvccGCPlanState{
		db:        db,
		safePoint: db.opt.MVCCGCSafePoint,
		retention: db.opt.MVCCGCSnapshotRetention,
	}
	return dbruntime.PeriodicTaskConfig{
		Name:     mvccGCPlanTaskName,
		Interval: db.opt.MVCCGCPlanInterval,
		Run:      state.run,
	}, state, true
}

func (s *mvccGCPlanState) run(ctx context.Context) error {
	requestedSafePoint := s.safePoint()
	if requestedSafePoint == 0 {
		s.record(mvccgc.TxnFloor{}, mvccgc.PlanStats{})
		return nil
	}
	txnFloor, err := mvccgc.PlanTxnFloor(ctx, s.db)
	if err != nil {
		s.record(txnFloor, mvccgc.PlanStats{})
		return err
	}
	var retention rootstate.SnapshotRetentionIndex
	if s.retention != nil {
		retention = s.retention()
	}
	plan, err := mvccgc.Plan(ctx, s.db, mvccgc.SafePointPolicy{
		RequestedSafePoint: requestedSafePoint,
		SnapshotRetention:  retention,
		TxnFloor:           txnFloor.OldestStartTs,
		Mount:              fsmetaMountResolver,
	})
	s.record(txnFloor, plan)
	return err
}

func fsmetaMountResolver(userKey []byte) (string, bool) {
	mount, ok := fsmeta.MountIDOfKey(userKey)
	return string(mount), ok
}

func (s *mvccGCPlanState) snapshot(task dbruntime.PeriodicTaskSnapshot) MVCCGCPlanSnapshot {
	if s == nil {
		return MVCCGCPlanSnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return MVCCGCPlanSnapshot{
		PeriodicTaskSnapshot: task,
		LastTxnFloor:         s.txnFloor,
		LastPlan:             s.plan,
	}
}

func (s *mvccGCPlanState) record(txnFloor mvccgc.TxnFloor, plan mvccgc.PlanStats) {
	s.mu.Lock()
	s.txnFloor = txnFloor
	s.plan = plan
	s.mu.Unlock()
}
