package server

import (
	"context"
	"testing"
	"time"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
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

func TestServiceDiagnosticsSnapshotIncludesCCCMetrics(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.cccMetrics.recordLeaseGenerationTransition(1, 2)
	svc.cccMetrics.recordClosureStageTransition(rootproto.CoordinatorClosureStageUnspecified, rootproto.CoordinatorClosureStageConfirmed)
	svc.cccMetrics.recordPreActionGateRejection(preActionDutyAdmission)
	svc.cccMetrics.recordALIViolation(aliSuccessorCoverage)

	metrics := svc.DiagnosticsSnapshot()["ccc_metrics"].(map[string]any)
	require.Equal(t, uint64(1), metrics["lease_generation_transitions_total"])
	require.Equal(t, map[string]any{
		"confirmed":  uint64(1),
		"closed":     uint64(0),
		"reattached": uint64(0),
	}, metrics["closure_stage_transitions_total"])
	require.Equal(t, map[string]any{
		"seal_current_generation": uint64(0),
		"lifecycle_mutation":      uint64(0),
		"duty_admission":          uint64(1),
	}, metrics["pre_action_gate_rejections_total"])
	require.Equal(t, map[string]any{
		"authority_uniqueness":      uint64(0),
		"successor_coverage":        uint64(1),
		"post_seal_inadmissibility": uint64(0),
		"closure_completeness":      uint64(0),
	}, metrics["ali_violations_total"])
}

func TestServiceValidatePreActionLeaseRecordsPostSealMetric(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	err := svc.validatePreActionLease(
		preActionDutyAdmission,
		rootproto.CoordinatorDutyAllocID,
		rootstate.CoordinatorLease{
			HolderID:        "c1",
			ExpiresUnixNano: time.Unix(0, 200).UnixNano(),
			CertGeneration:  3,
			DutyMask:        rootproto.CoordinatorDutyMaskDefault,
		},
		rootstate.CoordinatorSeal{
			HolderID:       "c1",
			CertGeneration: 3,
			DutyMask:       rootproto.CoordinatorDutyMaskDefault,
			Frontiers:      controlplane.Frontiers(rootstate.State{IDFence: 5, TSOFence: 9}, 0),
		},
	)
	require.Error(t, err)

	metrics := svc.DiagnosticsSnapshot()["ccc_metrics"].(map[string]any)
	require.Equal(t, map[string]any{
		"seal_current_generation": uint64(0),
		"lifecycle_mutation":      uint64(0),
		"duty_admission":          uint64(1),
	}, metrics["pre_action_gate_rejections_total"])
	require.Equal(t, map[string]any{
		"authority_uniqueness":      uint64(0),
		"successor_coverage":        uint64(0),
		"post_seal_inadmissibility": uint64(1),
		"closure_completeness":      uint64(0),
	}, metrics["ali_violations_total"])
}

func TestServiceClosureMetricsTrackLifecycleStages(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Allocator: rootview.AllocatorState{
				IDCurrent: 12,
				TSCurrent: 34,
			},
			CoordinatorLease: rootstate.CoordinatorLease{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 20_000).UnixNano(),
				CertGeneration:  3,
				DutyMask:        rootproto.CoordinatorDutyMaskDefault,
			},
			CoordinatorSeal: rootstate.CoordinatorSeal{
				HolderID:       "c1",
				CertGeneration: 2,
				DutyMask:       rootproto.CoordinatorDutyMaskDefault,
				Frontiers:      controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
				SealedAtCursor: rootstate.Cursor{Term: 1, Index: 9},
			},
			Descriptors: rootCloneDescriptorsForTest(map[uint64]descriptor.Descriptor{
				1: {RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			}),
		},
	}
	store.snapshot.CoordinatorLease.PredecessorDigest = rootstate.CoordinatorSealDigest(store.snapshot.CoordinatorSeal)
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	require.NoError(t, svc.ConfirmCoordinatorClosure())
	require.NoError(t, svc.CloseCoordinatorClosure())
	require.NoError(t, svc.ReattachCoordinatorClosure())

	metrics := svc.DiagnosticsSnapshot()["ccc_metrics"].(map[string]any)
	require.Equal(t, map[string]any{
		"confirmed":  uint64(1),
		"closed":     uint64(1),
		"reattached": uint64(1),
	}, metrics["closure_stage_transitions_total"])
}

func TestServiceEnsureCoordinatorLeaseRecordsCoverageViolationMetric(t *testing.T) {
	store := &fakeStorage{
		leader:      true,
		campaignErr: rootstate.ErrCoordinatorLeaseCoverage,
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)

	metrics := svc.DiagnosticsSnapshot()["ccc_metrics"].(map[string]any)
	require.Equal(t, map[string]any{
		"authority_uniqueness":      uint64(0),
		"successor_coverage":        uint64(1),
		"post_seal_inadmissibility": uint64(0),
		"closure_completeness":      uint64(0),
	}, metrics["ali_violations_total"])
}
