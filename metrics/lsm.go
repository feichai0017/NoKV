package metrics

// LevelMetrics captures aggregated statistics for a single LSM level.
type LevelMetrics struct {
	Level                 int
	TableCount            int
	SizeBytes             int64
	ValueBytes            int64
	StaleBytes            int64
	IngestTableCount      int
	IngestSizeBytes       int64
	IngestValueBytes      int64
	ValueDensity          float64
	IngestValueDensity    float64
	IngestRuns            int64
	IngestMs              float64
	IngestTablesCompacted int64
	IngestMergeRuns       int64
	IngestMergeMs         float64
	IngestMergeTables     int64
}
