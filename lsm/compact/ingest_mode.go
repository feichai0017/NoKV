package compact

// IngestMode describes how a compaction interacts with ingest tables.
type IngestMode uint8

const (
	// IngestNone indicates a regular compaction using main tables only.
	IngestNone IngestMode = iota
	// IngestDrain compacts ingest tables and writes output into main tables.
	IngestDrain
	// IngestKeep compacts ingest tables and keeps output in ingest buffers.
	IngestKeep
)

func (m IngestMode) UsesIngest() bool {
	return m != IngestNone
}

func (m IngestMode) KeepsIngest() bool {
	return m == IngestKeep
}
