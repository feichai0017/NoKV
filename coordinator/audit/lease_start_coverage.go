package audit

import eunomia "github.com/feichai0017/NoKV/coordinator/protocol/eunomia"

// LeaseStartCoverageReport is the minimal eunomia-audit bridge for the NoKV-native
// CRDB #66562 reproduction schedule. It intentionally stays outside rooted
// persisted schema.
type LeaseStartCoverageReport struct {
	Predecessor eunomia.LeaseView                `json:"predecessor"`
	Successor   eunomia.LeaseView                `json:"successor"`
	ReadSummary eunomia.ReadSummary              `json:"read_summary"`
	Coverage    eunomia.LeaseStartCoverageStatus `json:"coverage"`
	Anomalies   SnapshotAnomalies                `json:"anomalies"`
}

func BuildLeaseStartCoverageReport(predecessor, successor eunomia.LeaseView, summary eunomia.ReadSummary) LeaseStartCoverageReport {
	coverage := eunomia.EvaluateLeaseStartCoverage(successor, summary)
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
