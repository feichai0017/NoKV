package stats

import (
	localstats "github.com/feichai0017/NoKV/local/stats"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
)

// MVCCGC returns the runtime stats view of the raftstore MVCC GC state.
func MVCCGC(plan storemvcc.GCPlanSnapshot, maintenance storemvcc.MaintenanceSnapshot) localstats.MVCCGCStatsSnapshot {
	snap := localstats.MVCCGCStatsSnapshot{
		Enabled:               plan.Enabled,
		Runs:                  plan.Runs,
		SkippedRuns:           plan.SkippedRuns,
		LastUnix:              plan.LastUnix,
		LastDurationMs:        plan.LastDurationMs,
		LastError:             plan.LastError,
		ActiveLocks:           plan.LastTxnFloor.ActiveLocks,
		OldestStartTs:         plan.LastTxnFloor.OldestStartTs,
		MaxStartTs:            plan.LastTxnFloor.MaxStartTs,
		ScannedKeys:           plan.LastPlan.ScannedKeys,
		DroppableKeys:         plan.LastPlan.DroppableKeys,
		WriteVersions:         plan.LastPlan.WriteVersions,
		RetainedWrites:        plan.LastPlan.RetainedWrites,
		DroppableWrites:       plan.LastPlan.DroppableWrites,
		AnchorWrites:          plan.LastPlan.AnchorWrites,
		RetainedDefaultRefs:   plan.LastPlan.RetainedDefaultRefs,
		DeletedWriteMarkers:   plan.LastPlan.DeletedWriteMarkers,
		SafePointClampedKeys:  plan.LastPlan.SafePointClampedKeys,
		MaxVersionsPerKey:     plan.LastPlan.MaxVersionsPerKey,
		MinEffectiveSafePoint: plan.LastPlan.MinEffectiveSafePoint,
		MaxEffectiveSafePoint: plan.LastPlan.MaxEffectiveSafePoint,
	}
	addMaintenance(&snap, maintenance)
	return snap
}

func addMaintenance(s *localstats.MVCCGCStatsSnapshot, maintenance storemvcc.MaintenanceSnapshot) {
	if s == nil {
		return
	}
	s.MaintenanceEnabled = maintenance.Enabled
	s.MaintenanceRuns = maintenance.Runs
	s.MaintenanceLastUnix = maintenance.LastUnix
	s.MaintenanceLastDurationMs = maintenance.LastDurationMs
	s.MaintenanceLastError = maintenance.LastError
	s.MaintenanceResolveError = maintenance.LastResolveError
	s.MaintenanceApplyError = maintenance.LastApplyError
	s.MaintenanceOrphanError = maintenance.LastOrphanError
	s.MaintenanceSafePointSkipped = maintenance.LastSafePointSkipped
	s.ScannedLocks = maintenance.LastResolveLocks.ScannedLocks
	s.ExpiredLocks = maintenance.LastResolveLocks.ExpiredLocks
	s.ResolvedLocks = maintenance.LastResolveLocks.ResolvedLocks
	s.CommittedLocks = maintenance.LastResolveLocks.CommittedLocks
	s.RolledBackLocks = maintenance.LastResolveLocks.RolledBackLocks
	s.DeletedLockMarkers = maintenance.LastResolveLocks.DeletedLockMarkers
	s.AppliedWriteDeletes = maintenance.LastApply.AppliedWriteDeletes
	s.AppliedDefaultDeletes = maintenance.LastApply.AppliedDefaultDeletes
	s.OrphanDefaults = maintenance.LastOrphanDefaults.OrphanDefaults
	s.AppliedOrphanDefaults = maintenance.LastOrphanDefaults.AppliedDefaultDeletes
}

// ControlLogPointers adapts store-local raft checkpoints to the root DB control-log stats view.
func ControlLogPointers(source func() map[uint64]localmeta.RaftLogPointer) func() map[uint64]localstats.ControlLogPointer {
	if source == nil {
		return nil
	}
	return func() map[uint64]localstats.ControlLogPointer {
		ptrs := source()
		if ptrs == nil {
			return nil
		}
		out := make(map[uint64]localstats.ControlLogPointer, len(ptrs))
		for groupID, ptr := range ptrs {
			out[groupID] = localstats.ControlLogPointer{
				Segment:      ptr.Segment,
				SegmentIndex: ptr.SegmentIndex,
			}
		}
		return out
	}
}
