package server

import (
	"context"
	"testing"
	"time"

	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/tso"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

func TestServiceDiagnosticsSnapshotIncludesSuccessionMetrics(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.successionMetrics.recordTenureEpochTransition(1, 2)
	svc.successionMetrics.recordTransitStageTransition(rootproto.TransitStageUnspecified, rootproto.TransitStageConfirmed)
	svc.successionMetrics.recordGateRejection(gateMandateAdmission)
	svc.successionMetrics.recordGuaranteeViolation(guaranteeInheritance)

	metrics := svc.DiagnosticsSnapshot()["succession_metrics"].(map[string]any)
	require.Equal(t, uint64(1), metrics["tenure_epoch_transitions_total"])
	require.Equal(t, map[string]any{
		"confirmed":  uint64(1),
		"closed":     uint64(0),
		"reattached": uint64(0),
	}, metrics["transit_stage_transitions_total"])
	require.Equal(t, map[string]any{
		"legacy_formation":  uint64(0),
		"transit_mutation":  uint64(0),
		"mandate_admission": uint64(1),
	}, metrics["gate_rejections_total"])
	require.Equal(t, map[string]any{
		"primacy":     uint64(0),
		"inheritance": uint64(1),
		"silence":     uint64(0),
		"closure":     uint64(0),
	}, metrics["guarantee_violations_total"])
}

func TestServiceValidatePreActionLeaseRecordsPostSealMetric(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	err := svc.validateGateTenure(
		gateMandateAdmission,
		rootproto.MandateAllocID,
		rootstate.Tenure{
			HolderID:        "c1",
			ExpiresUnixNano: time.Unix(0, 200).UnixNano(),
			Epoch:           3,
			Mandate:         rootproto.MandateDefault,
		},
		rootstate.Legacy{
			HolderID:  "c1",
			Epoch:     3,
			Mandate:   rootproto.MandateDefault,
			Frontiers: succession.Frontiers(rootstate.State{IDFence: 5, TSOFence: 9}, 0),
		},
	)
	require.Error(t, err)

	metrics := svc.DiagnosticsSnapshot()["succession_metrics"].(map[string]any)
	require.Equal(t, map[string]any{
		"legacy_formation":  uint64(0),
		"transit_mutation":  uint64(0),
		"mandate_admission": uint64(1),
	}, metrics["gate_rejections_total"])
	require.Equal(t, map[string]any{
		"primacy":     uint64(0),
		"inheritance": uint64(0),
		"silence":     uint64(1),
		"closure":     uint64(0),
	}, metrics["guarantee_violations_total"])
}

func TestServiceClosureMetricsTrackLifecycleStages(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Allocator: rootview.AllocatorState{
				IDCurrent: 12,
				TSCurrent: 34,
			},
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 20_000).UnixNano(),
				Epoch:           3,
				Mandate:         rootproto.MandateDefault,
			},
			Legacy: rootstate.Legacy{
				HolderID:  "c1",
				Epoch:     2,
				Mandate:   rootproto.MandateDefault,
				Frontiers: succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
				SealedAt:  rootstate.Cursor{Term: 1, Index: 9},
			},
			Descriptors: rootCloneDescriptorsForTest(map[uint64]descriptor.Descriptor{
				1: {RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			}),
		},
	}
	store.snapshot.Tenure.LineageDigest = rootstate.DigestOfLegacy(store.snapshot.Legacy)
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	require.NoError(t, svc.ConfirmTransit())
	require.NoError(t, svc.CloseTransit())
	require.NoError(t, svc.ReattachTransit())

	metrics := svc.DiagnosticsSnapshot()["succession_metrics"].(map[string]any)
	require.Equal(t, map[string]any{
		"confirmed":  uint64(1),
		"closed":     uint64(1),
		"reattached": uint64(1),
	}, metrics["transit_stage_transitions_total"])
}

func TestServiceEnsureTenureRecordsCoverageViolationMetric(t *testing.T) {
	store := &fakeStorage{
		leader:      true,
		campaignErr: rootstate.ErrInheritance,
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)

	metrics := svc.DiagnosticsSnapshot()["succession_metrics"].(map[string]any)
	require.Equal(t, map[string]any{
		"primacy":     uint64(0),
		"inheritance": uint64(1),
		"silence":     uint64(0),
		"closure":     uint64(0),
	}, metrics["guarantee_violations_total"])
}
