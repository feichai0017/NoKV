package store

import (
	"testing"

	"github.com/feichai0017/NoKV/manifest"
)

func TestRegionMetricsCounts(t *testing.T) {
	rm := NewRegionMetrics()
	hooks := rm.Hooks()

	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 1, State: manifest.RegionStateRunning})
	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 2, State: manifest.RegionStateRunning})
	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 3, State: manifest.RegionStateRemoving})

	snap := rm.Snapshot()
	if snap.Total != 3 || snap.Running != 2 || snap.Removing != 1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 2, State: manifest.RegionStateTombstone})
	snap = rm.Snapshot()
	if snap.Total != 3 || snap.Running != 1 || snap.Tombstone != 1 {
		t.Fatalf("unexpected snapshot after transition: %+v", snap)
	}

	hooks.OnRegionRemove(3)
	snap = rm.Snapshot()
	if snap.Total != 2 || snap.Removing != 0 {
		t.Fatalf("unexpected snapshot after remove: %+v", snap)
	}
}

func TestRegionMetricsSnapshotCopy(t *testing.T) {
	rm := NewRegionMetrics()
	hooks := rm.Hooks()
	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 10, State: manifest.RegionStateRunning})

	snap := rm.Snapshot()
	if snap.Total != 1 {
		t.Fatalf("expected total=1, got %+v", snap)
	}

	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 10, State: manifest.RegionStateRemoving})
	snap2 := rm.Snapshot()
	if snap2.Removing != 1 {
		t.Fatalf("expected removing=1, got %+v", snap2)
	}
}
