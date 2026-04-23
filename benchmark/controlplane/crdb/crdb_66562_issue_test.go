package crdb

import (
	"errors"
	"slices"
	"strings"
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	protocol "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestControlPlaneCRDB66562IssueSchedule(t *testing.T) {
	t.Run("without_ccc_coverage", func(t *testing.T) {
		harness := newCRDB66562Harness(crdb66562Config{DisableCoverage: true})

		require.NoError(t, harness.AcquireFreshLease("n1", 0, 10))
		require.NoError(t, harness.ServeFutureRead("n1", "k", 9))
		require.NoError(t, harness.TransferLease("n1", "n2", 6, 8))
		require.Equal(t, uint64(9), harness.active.Summary.MaxTimestamp())
		require.Equal(t, protocol.LeaseAcquisitionTransfer, harness.active.Lease.Acquisition)
		require.NoError(t, harness.ExpireLease("n2", 8))
		require.Equal(t, uint64(9), harness.sealed.Summary.MaxTimestamp())

		require.NoError(t, harness.AcquireFreshLease("n3", 8, 12))
		require.Equal(t, protocol.LeaseAcquisitionFresh, harness.active.Lease.Acquisition)
		require.Empty(t, harness.active.Summary.Reads)
		require.Equal(t, uint64(8), harness.active.CacheFloor)
		require.NoError(t, harness.ServeWrite("n3", "k", 8, "v2"))

		report := harness.CoverageReport(harness.active.Lease)
		require.True(t, report.Anomalies.LeaseStartCoverageViolation)
		require.False(t, report.Coverage.Covered())
		require.Len(t, report.Coverage.Violations(), 1)
		require.Equal(t, "n2", report.Predecessor.HolderID)
		require.Equal(t, uint64(6), report.Predecessor.LeaseStart)
		require.Equal(t, uint64(8), report.Predecessor.LeaseExpiration)
		require.Equal(t, "n3", report.Successor.HolderID)
		require.Equal(t, uint64(8), report.Successor.LeaseStart)
		require.Equal(t, uint64(9), report.Coverage.Violations()[0].ServedTimestamp)

		require.Len(t, harness.writes, 1)
		require.True(t, harness.writes[0].Accepted)
		require.Equal(t, uint64(8), harness.writes[0].Timestamp)

		traceRecords := mustDecodeCRDBLeaseStartTrace(t, []crdb66562TraceRecord{
			harness.TraceRecord(harness.active.Lease, true),
		})
		traceAnomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, traceRecords)
		require.Equal(t, []string{"lease_start_coverage_violation"}, anomalyKinds(traceAnomalies))

		t.Logf(
			"crdb_66562 scenario=without_ccc_coverage n1_lease=[%d,%d] transfer=n1->n2@%d expiry=%d n3_fresh_start=%d served_key=%s served_ts=%d write_ts=%d snapshot_anomalies=%s trace_anomalies=%s",
			uint64(0),
			uint64(10),
			uint64(6),
			uint64(8),
			report.Successor.LeaseStart,
			report.Coverage.Violations()[0].Key,
			report.Coverage.Violations()[0].ServedTimestamp,
			harness.writes[0].Timestamp,
			"lease_start_coverage_violation",
			formatAnomalyKinds(traceAnomalies),
		)
	})

	t.Run("with_ccc_coverage", func(t *testing.T) {
		harness := newCRDB66562Harness(crdb66562Config{})

		require.NoError(t, harness.AcquireFreshLease("n1", 0, 10))
		require.NoError(t, harness.ServeFutureRead("n1", "k", 9))
		require.NoError(t, harness.TransferLease("n1", "n2", 6, 8))
		require.NoError(t, harness.ExpireLease("n2", 8))

		err := harness.AcquireFreshLease("n3", 8, 12)
		require.ErrorIs(t, err, protocol.ErrLeaseStartCoverage)

		rejectedLease := protocol.LeaseView{
			HolderID:        "n3",
			LeaseStart:      8,
			LeaseExpiration: 12,
			Acquisition:     protocol.LeaseAcquisitionFresh,
		}
		violationReport := harness.CoverageReport(rejectedLease)
		require.True(t, violationReport.Anomalies.LeaseStartCoverageViolation)
		require.False(t, violationReport.Coverage.Covered())

		rejectedTrace := mustDecodeCRDBLeaseStartTrace(t, []crdb66562TraceRecord{
			harness.TraceRecord(rejectedLease, false),
		})
		require.Empty(t, coordaudit.EvaluateReplyTrace(coordaudit.Report{}, rejectedTrace))

		require.NoError(t, harness.AcquireFreshLease("n3", 10, 12))
		require.NoError(t, harness.ServeWrite("n3", "k", 10, "v3"))

		report := harness.CoverageReport(harness.active.Lease)
		require.False(t, report.Anomalies.LeaseStartCoverageViolation)
		require.True(t, report.Coverage.Covered())
		require.Len(t, harness.writes, 1)
		require.True(t, harness.writes[0].Accepted)
		require.Equal(t, uint64(10), harness.writes[0].Timestamp)

		acceptedTrace := mustDecodeCRDBLeaseStartTrace(t, []crdb66562TraceRecord{
			harness.TraceRecord(harness.active.Lease, true),
		})
		acceptedTraceAnomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, acceptedTrace)
		require.Empty(t, acceptedTraceAnomalies)

		t.Logf(
			"crdb_66562 scenario=with_ccc_coverage rejected_n3_fresh_start=%d accepted_n3_fresh_start=%d carried_served_ts=%d write_ts=%d snapshot_anomalies=%s trace_anomalies=%s",
			uint64(8),
			report.Successor.LeaseStart,
			harness.sealed.Summary.MaxTimestamp(),
			harness.writes[0].Timestamp,
			"none",
			formatAnomalyKinds(acceptedTraceAnomalies),
		)
	})
}

func TestControlPlaneCRDB66562RootedGate(t *testing.T) {
	h := newCRDB66562Harness(crdb66562Config{DisableCoverage: true})
	require.NoError(t, h.AcquireFreshLease("n1", 0, 10))
	require.NoError(t, h.ServeFutureRead("n1", "k", 9))
	require.NoError(t, h.TransferLease("n1", "n2", 6, 8))
	require.NoError(t, h.ExpireLease("n2", 8))

	servedFrontier := h.sealed.Summary.MaxTimestamp()
	require.Equal(t, uint64(9), servedFrontier)

	seal := rootstate.Legacy{
		HolderID: "n2",
		Epoch:    1,
		Mandate:  rootproto.MandateDefault,
	}
	seal = rootstate.LegacyWithServedFrontier(seal, servedFrontier)

	require.Equal(t, servedFrontier, seal.Frontiers.Frontier(rootproto.MandateLeaseStart))

	rejectErr := rootstate.ValidateLeaseStartInheritance(seal, 8)
	require.ErrorIs(t, rejectErr, rootstate.ErrInheritance)
	require.NoError(t, rootstate.ValidateLeaseStartInheritance(seal, 10))

	candidate := protocol.LeaseView{HolderID: "n3", LeaseStart: 8, LeaseExpiration: 12, Acquisition: protocol.LeaseAcquisitionFresh}
	protocolErr := protocol.ValidateLeaseStartCoverage(candidate, h.sealed.Summary)
	require.True(t, errors.Is(protocolErr, protocol.ErrLeaseStartCoverage))

	t.Logf(
		"crdb_66562 rooted_gate served_frontier=%d rejected_lease_start=8 rooted_err=%v protocol_err=%v accepted_lease_start=10",
		servedFrontier, rejectErr, protocolErr,
	)
}

func anomalyKinds(anomalies []coordaudit.ReplyTraceAnomaly) []string {
	if len(anomalies) == 0 {
		return nil
	}
	out := make([]string, 0, len(anomalies))
	for _, anomaly := range anomalies {
		out = append(out, anomaly.Kind)
	}
	slices.Sort(out)
	return out
}

func formatAnomalyKinds(anomalies []coordaudit.ReplyTraceAnomaly) string {
	kinds := anomalyKinds(anomalies)
	if len(kinds) == 0 {
		return "none"
	}
	return strings.Join(kinds, ",")
}
