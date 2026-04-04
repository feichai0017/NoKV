package file

const (
	// CheckpointFileName stores one compact rooted metadata checkpoint as a
	// single binary protobuf blob. The payload type is metapb.RootCheckpoint.
	CheckpointFileName = "root.checkpoint.binpb"
	// LogFileName stores the retained committed rooted event stream as a framed
	// append-only WAL. Each record carries one protobuf-encoded RootEvent.
	LogFileName        = "root.events.wal"
)
