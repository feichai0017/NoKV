package audit_test

import (
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestBuildReport(t *testing.T) {
	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 2,
		DutyMask:       rootproto.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
		SealedAtCursor: rootstate.Cursor{Term: 1, Index: 9},
	}
	sealDigest := rootstate.CoordinatorSealDigest(seal)
	snapshot := rootview.Snapshot{
		CatchUpState: rootview.CatchUpStateFresh,
		Allocator: rootview.AllocatorState{
			IDCurrent: 12,
			TSCurrent: 34,
		},
		CoordinatorLease: rootstate.CoordinatorLease{
			HolderID:          "c1",
			ExpiresUnixNano:   2_000,
			CertGeneration:    3,
			DutyMask:          rootproto.CoordinatorDutyMaskDefault,
			PredecessorDigest: sealDigest,
		},
		CoordinatorSeal: seal,
		CoordinatorClosure: rootstate.CoordinatorClosure{
			HolderID:            "c1",
			SealGeneration:      2,
			SuccessorGeneration: 3,
			SealDigest:          sealDigest,
			Stage:               rootproto.CoordinatorClosureStageReattached,
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
	require.Equal(t, rootproto.CoordinatorClosureStageReattached, report.Closure.Stage)
	require.Equal(t, coordaudit.ClosureDefectNone, report.Anomalies.ClosureDefect)
}

func TestBuildReportSurfacesClosureGaps(t *testing.T) {
	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 2,
		DutyMask:       rootproto.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 9),
		SealedAtCursor: rootstate.Cursor{Term: 1, Index: 9},
	}
	sealDigest := rootstate.CoordinatorSealDigest(seal)
	snapshot := rootview.Snapshot{
		CatchUpState: rootview.CatchUpStateFresh,
		Allocator: rootview.AllocatorState{
			IDCurrent: 12,
			TSCurrent: 34,
		},
		CoordinatorLease: rootstate.CoordinatorLease{
			HolderID:          "c2",
			ExpiresUnixNano:   2_000,
			CertGeneration:    3,
			DutyMask:          rootproto.CoordinatorDutyMaskDefault,
			PredecessorDigest: sealDigest,
		},
		CoordinatorSeal: seal,
		Descriptors: map[uint64]descriptor.Descriptor{
			1: {RegionID: 1, RootEpoch: 9},
		},
	}

	report := coordaudit.BuildReport(snapshot, "c2", 1_000)
	require.Equal(t, rootproto.CoordinatorClosureStageUnspecified, report.Closure.Stage)
	require.False(t, report.Anomalies.SuccessorLineageMismatch)
	require.False(t, report.Anomalies.UncoveredMonotoneFrontier)
	require.False(t, report.Anomalies.UncoveredDescriptorRevision)
	require.False(t, report.Anomalies.SealedGenerationStillLive)
	require.Equal(t, coordaudit.ClosureDefectMissingConfirm, report.Anomalies.ClosureDefect)
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
