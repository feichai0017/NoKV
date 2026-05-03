package stats

import (
	"testing"

	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	dbruntime "github.com/feichai0017/NoKV/runtime"
)

func TestMVCCGCAdaptsPlanAndMaintenanceSnapshots(t *testing.T) {
	plan := storemvcc.GCPlanSnapshot{
		PeriodicTaskSnapshot: dbruntime.PeriodicTaskSnapshot{
			Enabled:        true,
			Runs:           3,
			LastUnix:       1710000100,
			LastDurationMs: 12.5,
			LastError:      "plan warning",
		},
		SkippedRuns: 4,
		LastTxnFloor: storemvcc.TxnFloor{
			ActiveLocks:   5,
			OldestStartTs: 10,
			MaxStartTs:    30,
		},
		LastPlan: storemvcc.PlanStats{
			ScannedKeys:           11,
			DroppableKeys:         12,
			WriteVersions:         13,
			RetainedWrites:        14,
			DroppableWrites:       15,
			AnchorWrites:          16,
			RetainedDefaultRefs:   17,
			DeletedWriteMarkers:   18,
			SafePointClampedKeys:  19,
			MaxVersionsPerKey:     20,
			MinEffectiveSafePoint: 21,
			MaxEffectiveSafePoint: 22,
		},
	}
	maintenance := storemvcc.MaintenanceSnapshot{
		Enabled:              true,
		Runs:                 31,
		LastUnix:             1710000200,
		LastDurationMs:       42.25,
		LastError:            "maintenance warning",
		LastResolveError:     "resolve warning",
		LastApplyError:       "apply warning",
		LastOrphanError:      "orphan warning",
		LastSafePointSkipped: true,
		LastResolveLocks: storemvcc.ResolveLocksStats{
			ScannedLocks:       41,
			ExpiredLocks:       42,
			ResolvedLocks:      43,
			CommittedLocks:     44,
			RolledBackLocks:    45,
			DeletedLockMarkers: 46,
		},
		LastApply: storemvcc.ApplyStats{
			AppliedWriteDeletes:   51,
			AppliedDefaultDeletes: 52,
		},
		LastOrphanDefaults: storemvcc.OrphanDefaultStats{
			OrphanDefaults:        61,
			AppliedDefaultDeletes: 62,
		},
	}

	snap := MVCCGC(plan, maintenance)
	if !snap.Enabled || snap.Runs != 3 || snap.SkippedRuns != 4 || snap.LastUnix != 1710000100 || snap.LastDurationMs != 12.5 || snap.LastError != "plan warning" {
		t.Fatalf("plan task fields not adapted: %#v", snap)
	}
	if snap.ActiveLocks != 5 || snap.OldestStartTs != 10 || snap.MaxStartTs != 30 {
		t.Fatalf("txn floor not adapted: %#v", snap)
	}
	if snap.ScannedKeys != 11 ||
		snap.DroppableKeys != 12 ||
		snap.WriteVersions != 13 ||
		snap.RetainedWrites != 14 ||
		snap.DroppableWrites != 15 ||
		snap.AnchorWrites != 16 ||
		snap.RetainedDefaultRefs != 17 ||
		snap.DeletedWriteMarkers != 18 ||
		snap.SafePointClampedKeys != 19 ||
		snap.MaxVersionsPerKey != 20 ||
		snap.MinEffectiveSafePoint != 21 ||
		snap.MaxEffectiveSafePoint != 22 {
		t.Fatalf("plan stats not adapted: %#v", snap)
	}
	if !snap.MaintenanceEnabled ||
		snap.MaintenanceRuns != 31 ||
		snap.MaintenanceLastUnix != 1710000200 ||
		snap.MaintenanceLastDurationMs != 42.25 ||
		snap.MaintenanceLastError != "maintenance warning" ||
		snap.MaintenanceResolveError != "resolve warning" ||
		snap.MaintenanceApplyError != "apply warning" ||
		snap.MaintenanceOrphanError != "orphan warning" ||
		!snap.MaintenanceSafePointSkipped {
		t.Fatalf("maintenance task fields not adapted: %#v", snap)
	}
	if snap.ScannedLocks != 41 ||
		snap.ExpiredLocks != 42 ||
		snap.ResolvedLocks != 43 ||
		snap.CommittedLocks != 44 ||
		snap.RolledBackLocks != 45 ||
		snap.DeletedLockMarkers != 46 ||
		snap.AppliedWriteDeletes != 51 ||
		snap.AppliedDefaultDeletes != 52 ||
		snap.OrphanDefaults != 61 ||
		snap.AppliedOrphanDefaults != 62 {
		t.Fatalf("maintenance counters not adapted: %#v", snap)
	}
}

func TestRaftLogPointersAdaptsDetachedMap(t *testing.T) {
	source := map[uint64]localmeta.RaftLogPointer{
		1: {Segment: 10, SegmentIndex: 100, AppliedIndex: 1},
		2: {Segment: 20, SegmentIndex: 200, AppliedIndex: 2},
	}
	fn := RaftLogPointers(func() map[uint64]localmeta.RaftLogPointer {
		return source
	})
	if fn == nil {
		t.Fatal("expected adapter function")
	}

	first := fn()
	if len(first) != 2 ||
		first[1].Segment != 10 ||
		first[1].SegmentIndex != 100 ||
		first[2].Segment != 20 ||
		first[2].SegmentIndex != 200 {
		t.Fatalf("unexpected raft log pointer snapshot: %#v", first)
	}

	source[1] = localmeta.RaftLogPointer{Segment: 99, SegmentIndex: 999}
	if first[1].Segment != 10 || first[1].SegmentIndex != 100 {
		t.Fatalf("adapter returned aliased map entry: %#v", first[1])
	}
	second := fn()
	if second[1].Segment != 99 || second[1].SegmentIndex != 999 {
		t.Fatalf("adapter did not reflect fresh source snapshot: %#v", second[1])
	}
}

func TestRaftLogPointersNilSources(t *testing.T) {
	if RaftLogPointers(nil) != nil {
		t.Fatal("nil source should return nil adapter")
	}
	fn := RaftLogPointers(func() map[uint64]localmeta.RaftLogPointer {
		return nil
	})
	if fn == nil {
		t.Fatal("expected adapter function for non-nil source")
	}
	if out := fn(); out != nil {
		t.Fatalf("nil source map should stay nil, got %#v", out)
	}
}
