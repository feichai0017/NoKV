package audit_test

import (
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/meta/topology"
	"github.com/stretchr/testify/require"
)

func TestBuildReportSurfacesSealedExactCompleted(t *testing.T) {
	snapshot := rootview.Snapshot{
		CatchUpState: rootview.CatchUpStateFresh,
		ActiveGrant: rootproto.AuthorityGrant{
			GrantID:  "g2",
			HolderID: "c2",
			Era:      2,
			Duties: []rootproto.DutyGrant{
				rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20),
				rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 30),
			},
		},
		RetiredGrants: []rootproto.GrantRetirement{
			{
				GrantID:            "g1",
				HolderID:           "c1",
				Era:                1,
				Mode:               rootproto.GrantRetirementSealedExact,
				Bounds:             []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
				InheritedByGrantID: "g2",
			},
		},
		GrantInheritances: []rootproto.GrantInheritance{{PredecessorGrantID: "g1", SuccessorGrantID: "g2"}},
		Descriptors:       map[uint64]topology.Descriptor{1: {RegionID: 1, RootEpoch: 7}},
	}

	report := coordaudit.BuildReport(snapshot, "c2", 1_000)
	require.Equal(t, uint64(7), report.RootDescriptorRevision)
	require.Equal(t, "fresh", report.CatchUpState)
	require.Equal(t, "c2", report.CurrentHolderID)
	require.Equal(t, uint64(2), report.CurrentEra)
	require.Equal(t, uint64(1), report.RetiredEraFloor)
	require.Equal(t, coordaudit.AuthorityCompletionSealedExactCompleted, report.AuthorityCompletion)
	require.Equal(t, coordaudit.FinalityDefectNone, report.Anomalies.FinalityDefect)
}

func TestBuildReportSurfacesExpiredBoundInherited(t *testing.T) {
	report := coordaudit.BuildReport(rootview.Snapshot{
		ActiveGrant: rootproto.AuthorityGrant{GrantID: "g2", HolderID: "c2", Era: 2},
		RetiredGrants: []rootproto.GrantRetirement{
			{
				GrantID:            "g1",
				HolderID:           "c1",
				Era:                1,
				Mode:               rootproto.GrantRetirementExpiredBound,
				Bounds:             []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
				InheritedByGrantID: "g2",
			},
		},
	}, "c2", 1_000)

	require.Equal(t, coordaudit.AuthorityCompletionExpiredBoundInherited, report.AuthorityCompletion)
	require.Equal(t, coordaudit.FinalityDefectNone, report.Anomalies.FinalityDefect)
}

func TestBuildReportSurfacesRetiredNotInherited(t *testing.T) {
	report := coordaudit.BuildReport(rootview.Snapshot{
		RetiredGrants: []rootproto.GrantRetirement{
			{GrantID: "g1", HolderID: "c1", Era: 1, Mode: rootproto.GrantRetirementExpiredBound},
		},
	}, "c2", 1_000)

	require.Equal(t, coordaudit.AuthorityCompletionRetiredNotInherited, report.AuthorityCompletion)
	require.True(t, report.Anomalies.RetiredGrantNotInherited)
	require.Equal(t, coordaudit.FinalityDefectRetiredNotInherited, report.Anomalies.FinalityDefect)
	require.Zero(t, report.RetiredEraFloor)
}

func TestBuildReportSurfacesInvalidSuccessorBound(t *testing.T) {
	report := coordaudit.BuildReport(rootview.Snapshot{
		ActiveGrant: rootproto.AuthorityGrant{
			GrantID:  "g2",
			HolderID: "c2",
			Era:      2,
			Duties:   []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 5)},
		},
		RetiredGrants: []rootproto.GrantRetirement{
			{
				GrantID:  "g1",
				HolderID: "c1",
				Era:      1,
				Mode:     rootproto.GrantRetirementExpiredBound,
				Bounds:   []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
			},
		},
	}, "c2", 1_000)

	require.True(t, report.Anomalies.InvalidSuccessorBound)
	require.Equal(t, coordaudit.FinalityDefectInvalidSuccessorBound, report.Anomalies.FinalityDefect)
}

func TestBuildReportPreservesCompactedRetiredEraFloor(t *testing.T) {
	report := coordaudit.BuildReport(rootview.Snapshot{
		RetiredEraFloor: 3,
		ActiveGrant: rootproto.AuthorityGrant{
			GrantID: "g4",
			Era:     4,
			Duties:  []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 40)},
		},
	}, "c4", 1_000)

	require.Equal(t, uint64(3), report.RetiredEraFloor)
	require.Equal(t, coordaudit.FinalityDefectNone, report.Anomalies.FinalityDefect)
}

func TestBuildReportSurfacesOrphanInheritance(t *testing.T) {
	report := coordaudit.BuildReport(rootview.Snapshot{
		GrantInheritances: []rootproto.GrantInheritance{{PredecessorGrantID: "missing", SuccessorGrantID: "g2"}},
	}, "c2", 1_000)

	require.True(t, report.Anomalies.OrphanInheritance)
	require.Equal(t, coordaudit.FinalityDefectOrphanInheritance, report.Anomalies.FinalityDefect)
}
