package audit

import eunomia "github.com/feichai0017/NoKV/meta/root/protocol/eunomia"

// LeaseStartCoverageReport is the minimal audit bridge for lease-start
// coverage diagnostics. It intentionally stays outside rooted persisted
// schema.
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
