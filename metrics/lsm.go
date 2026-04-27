package metrics

// LevelMetrics captures aggregated statistics for a single LSM level.
type LevelMetrics struct {
	Level                int
	TableCount           int
	SizeBytes            int64
	ValueBytes           int64
	StaleBytes           int64
	SpillTableCount      int
	SpillSizeBytes       int64
	SpillValueBytes      int64
	ValueDensity         float64
	SpillValueDensity    float64
	SpillRuns            int64
	SpillMs              float64
	SpillTablesCompacted int64
	SpillMergeRuns       int64
	SpillMergeMs         float64
	SpillMergeTables     int64
}
