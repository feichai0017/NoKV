package metrics

// LevelMetrics captures aggregated statistics for a single LSM level.
type LevelMetrics struct {
	Level                  int
	TableCount             int
	SizeBytes              int64
	ValueBytes             int64
	StaleBytes             int64
	StagingTableCount      int
	StagingSizeBytes       int64
	StagingValueBytes      int64
	ValueDensity           float64
	StagingValueDensity    float64
	StagingRuns            int64
	StagingMs              float64
	StagingTablesCompacted int64
	StagingMergeRuns       int64
	StagingMergeMs         float64
	StagingMergeTables     int64
}
