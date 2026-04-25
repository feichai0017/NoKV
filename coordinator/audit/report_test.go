package audit_test

import (
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	eunomia "github.com/feichai0017/NoKV/coordinator/protocol/eunomia"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestBuildReport(t *testing.T) {
	seal := rootstate.Legacy{
		HolderID:  "c1",
		Era:       2,
		Mandate:   rootproto.MandateDefault,
		Frontiers: eunomia.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
		SealedAt:  rootstate.Cursor{Term: 1, Index: 9},
	}
	legacyDigest := rootstate.DigestOfLegacy(seal)
	snapshot := rootview.Snapshot{
		CatchUpState: rootview.CatchUpStateFresh,
		Allocator: rootview.AllocatorState{
			IDCurrent: 12,
			TSCurrent: 34,
		},
		Tenure: rootstate.Tenure{
			HolderID:        "c1",
			ExpiresUnixNano: 2_000,
			Era:             3,
			Mandate:         rootproto.MandateDefault,
			LineageDigest:   legacyDigest,
		},
		Legacy: seal,
		Handover: rootstate.Handover{
			HolderID:     "c1",
			LegacyEra:    2,
			SuccessorEra: 3,
			LegacyDigest: legacyDigest,
			Stage:        rootproto.HandoverStageReattached,
		},
		Descriptors: map[uint64]descriptor.Descriptor{
			1: {RegionID: 1, RootEpoch: 7},
		},
	}

	report := coordaudit.BuildReport(snapshot, "c1", 1_000)
	require.Equal(t, uint64(7), report.RootDescriptorRevision)
	require.Equal(t, "fresh", report.CatchUpState)
	require.Equal(t, "c1", report.CurrentHolderID)
	require.Equal(t, uint64(3), report.CurrentEra)
	require.True(t, report.HandoverWitness.FinalitySatisfied())
	require.Equal(t, rootproto.HandoverStageReattached, report.Handover.Stage)
	require.Equal(t, coordaudit.FinalityDefectNone, report.Anomalies.FinalityDefect)
}

func TestBuildReportSurfacesClosureGaps(t *testing.T) {
	seal := rootstate.Legacy{
		HolderID:  "c1",
		Era:       2,
		Mandate:   rootproto.MandateDefault,
		Frontiers: eunomia.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 9),
		SealedAt:  rootstate.Cursor{Term: 1, Index: 9},
	}
	legacyDigest := rootstate.DigestOfLegacy(seal)
	snapshot := rootview.Snapshot{
		CatchUpState: rootview.CatchUpStateFresh,
		Allocator: rootview.AllocatorState{
			IDCurrent: 12,
			TSCurrent: 34,
		},
		Tenure: rootstate.Tenure{
			HolderID:        "c2",
			ExpiresUnixNano: 2_000,
			Era:             3,
			Mandate:         rootproto.MandateDefault,
			LineageDigest:   legacyDigest,
		},
		Legacy: seal,
		Descriptors: map[uint64]descriptor.Descriptor{
			1: {RegionID: 1, RootEpoch: 9},
		},
	}

	report := coordaudit.BuildReport(snapshot, "c2", 1_000)
	require.Equal(t, rootproto.HandoverStageUnspecified, report.Handover.Stage)
	require.False(t, report.Anomalies.SuccessorLineageMismatch)
	require.False(t, report.Anomalies.UncoveredMonotoneFrontier)
	require.False(t, report.Anomalies.UncoveredDescriptorRevision)
	require.False(t, report.Anomalies.SealedEraStillLive)
	require.Equal(t, coordaudit.FinalityDefectMissingConfirm, report.Anomalies.FinalityDefect)
}

func TestBuildLeaseStartCoverageReport(t *testing.T) {
	report := coordaudit.BuildLeaseStartCoverageReport(
		eunomia.LeaseView{HolderID: "A", LeaseStart: 100},
		eunomia.LeaseView{HolderID: "C", LeaseStart: 103},
		eunomia.NewReadSummary(
			eunomia.ServedRead{Key: "k", Timestamp: 105},
		),
	)

	require.True(t, report.Anomalies.LeaseStartCoverageViolation)
	require.False(t, report.Coverage.Covered())
	require.Len(t, report.Coverage.Violations(), 1)

	report = coordaudit.BuildLeaseStartCoverageReport(
		eunomia.LeaseView{HolderID: "A", LeaseStart: 100},
		eunomia.LeaseView{HolderID: "C", LeaseStart: 106},
		eunomia.NewReadSummary(
			eunomia.ServedRead{Key: "k", Timestamp: 105},
		),
	)

	require.False(t, report.Anomalies.LeaseStartCoverageViolation)
	require.True(t, report.Coverage.Covered())
}
