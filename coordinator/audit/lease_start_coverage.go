package audit

import controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"

// LeaseStartCoverageReport is the minimal ccc-audit bridge for the NoKV-native
// CRDB #66562 reproduction schedule. It intentionally stays outside rooted
// persisted schema.
type LeaseStartCoverageReport struct {
	Predecessor controlplane.LeaseView                `json:"predecessor"`
	Successor   controlplane.LeaseView                `json:"successor"`
	ReadSummary controlplane.ReadSummary              `json:"read_summary"`
	Coverage    controlplane.LeaseStartCoverageStatus `json:"coverage"`
	Anomalies   SnapshotAnomalies                     `json:"anomalies"`
}

func BuildLeaseStartCoverageReport(predecessor, successor controlplane.LeaseView, summary controlplane.ReadSummary) LeaseStartCoverageReport {
	coverage := controlplane.EvaluateLeaseStartCoverage(successor, summary)
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
