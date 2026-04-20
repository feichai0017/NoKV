package audit_test

import (
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestBuildReport(t *testing.T) {
	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 2,
		DutyMask:       rootstate.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(12, 34, 7),
		SealedAtCursor: rootstate.Cursor{Term: 1, Index: 9},
	}
	sealDigest := rootstate.CoordinatorSealDigest(seal)
	snapshot := coordstorage.Snapshot{
		CatchUpState: coordstorage.CatchUpStateFresh,
		Allocator: coordstorage.AllocatorState{
			IDCurrent: 12,
			TSCurrent: 34,
		},
		CoordinatorLease: rootstate.CoordinatorLease{
			HolderID:          "c1",
			ExpiresUnixNano:   2_000,
			CertGeneration:    3,
			DutyMask:          rootstate.CoordinatorDutyMaskDefault,
			PredecessorDigest: sealDigest,
		},
		CoordinatorSeal: seal,
		CoordinatorClosure: rootstate.CoordinatorClosure{
			HolderID:            "c1",
			SealGeneration:      2,
			SuccessorGeneration: 3,
			SealDigest:          sealDigest,
			Stage:               rootstate.CoordinatorClosureStageReattached,
		},
		Descriptors: map[uint64]descriptor.Descriptor{
			1: {RegionID: 1, RootEpoch: 7},
		},
	}

	report := coordaudit.BuildReport(snapshot, "c1", 1_000)
	require.Equal(t, uint64(7), report.RootDescriptorRevision)
	require.Equal(t, "fresh", report.CatchUpState)
	require.Equal(t, "c1", report.CurrentHolderID)
	require.Equal(t, uint64(3), report.CurrentGeneration)
	require.True(t, report.ClosureWitness.ClosureSatisfied())
	require.Equal(t, rootstate.CoordinatorClosureStageReattached, report.Closure.Stage)
	require.False(t, report.Anomalies.ClosureIncomplete)
	require.False(t, report.Anomalies.MissingClose)
	require.False(t, report.Anomalies.ReattachIncomplete)
}

func TestBuildReportSurfacesClosureGaps(t *testing.T) {
	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 2,
		DutyMask:       rootstate.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(12, 34, 9),
		SealedAtCursor: rootstate.Cursor{Term: 1, Index: 9},
	}
	sealDigest := rootstate.CoordinatorSealDigest(seal)
	snapshot := coordstorage.Snapshot{
		CatchUpState: coordstorage.CatchUpStateFresh,
		Allocator: coordstorage.AllocatorState{
			IDCurrent: 12,
			TSCurrent: 34,
		},
		CoordinatorLease: rootstate.CoordinatorLease{
			HolderID:          "c2",
			ExpiresUnixNano:   2_000,
			CertGeneration:    3,
			DutyMask:          rootstate.CoordinatorDutyMaskDefault,
			PredecessorDigest: sealDigest,
		},
		CoordinatorSeal: seal,
		Descriptors: map[uint64]descriptor.Descriptor{
			1: {RegionID: 1, RootEpoch: 9},
		},
	}

	report := coordaudit.BuildReport(snapshot, "c2", 1_000)
	require.Equal(t, rootstate.CoordinatorClosureStagePendingConfirm, report.Closure.Stage)
	require.False(t, report.Anomalies.SuccessorLineageMismatch)
	require.False(t, report.Anomalies.UncoveredMonotoneFrontier)
	require.False(t, report.Anomalies.UncoveredDescriptorRevision)
	require.False(t, report.Anomalies.ClosureIncomplete)
	require.False(t, report.Anomalies.SealedGenerationStillLive)
	require.True(t, report.Anomalies.MissingConfirm)
	require.False(t, report.Anomalies.MissingClose)
}

func TestBuildLeaseStartCoverageReport(t *testing.T) {
	report := coordaudit.BuildLeaseStartCoverageReport(
		controlplane.LeaseView{HolderID: "A", LeaseStart: 100},
		controlplane.LeaseView{HolderID: "C", LeaseStart: 103},
		controlplane.NewReadSummary(
			controlplane.ServedRead{Key: "k", Timestamp: 105},
		),
	)

	require.True(t, report.Anomalies.LeaseStartCoverageViolation)
	require.False(t, report.Coverage.Covered())
	require.Len(t, report.Coverage.Violations(), 1)

	report = coordaudit.BuildLeaseStartCoverageReport(
		controlplane.LeaseView{HolderID: "A", LeaseStart: 100},
		controlplane.LeaseView{HolderID: "C", LeaseStart: 106},
		controlplane.NewReadSummary(
			controlplane.ServedRead{Key: "k", Timestamp: 105},
		),
	)

	require.False(t, report.Anomalies.LeaseStartCoverageViolation)
	require.True(t, report.Coverage.Covered())
}
