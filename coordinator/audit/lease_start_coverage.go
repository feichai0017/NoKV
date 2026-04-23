package audit

import succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"

// LeaseStartCoverageReport is the minimal succession-audit bridge for the NoKV-native
// CRDB #66562 reproduction schedule. It intentionally stays outside rooted
// persisted schema.
type LeaseStartCoverageReport struct {
	Predecessor succession.LeaseView                `json:"predecessor"`
	Successor   succession.LeaseView                `json:"successor"`
	ReadSummary succession.ReadSummary              `json:"read_summary"`
	Coverage    succession.LeaseStartCoverageStatus `json:"coverage"`
	Anomalies   SnapshotAnomalies                   `json:"anomalies"`
}

func BuildLeaseStartCoverageReport(predecessor, successor succession.LeaseView, summary succession.ReadSummary) LeaseStartCoverageReport {
	coverage := succession.EvaluateLeaseStartCoverage(successor, summary)
	return LeaseStartCoverageReport{
		Predecessor: predecessor,
		Successor:   successor,
		ReadSummary: summary,
		Coverage:    coverage,
		Anomalies: SnapshotAnomalies{
			LeaseStartCoverageViolation: len(summary.Reads) != 0 && !coverage.Covered(),
		},
	}
}
