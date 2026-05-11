package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/stretchr/testify/require"
)

func TestPerasWireRoundTrip(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: fsmeta.MountKeyID(9),
		Buckets:    []fsmeta.AffinityBucket{1, 3},
		Parents:    []fsmeta.InodeID{11, 12},
		Inodes:     []fsmeta.InodeID{21},
	}
	decodedScope, err := ScopeFromProto(ScopeToProto(scope))
	require.NoError(t, err)
	require.Equal(t, scope, decodedScope)

	record := wireTestSegmentRecord()
	decodedRecord, err := SegmentWitnessRecordFromProto(SegmentWitnessRecordToProto(record))
	require.NoError(t, err)
	require.Equal(t, record, decodedRecord)

	snapshot := fsperas.WitnessSnapshot{Segments: []fsperas.SegmentWitnessRecord{record}}
	decoded, err := SnapshotFromProto(SnapshotToProto(snapshot))
	require.NoError(t, err)
	require.Equal(t, snapshot, decoded)
}

func TestPerasWireRejectsWrongFixedDigestLength(t *testing.T) {
	record := SegmentWitnessRecordToProto(wireTestSegmentRecord())
	record.SegmentRoot = record.SegmentRoot[:31]
	_, err := SegmentWitnessRecordFromProto(record)
	require.ErrorContains(t, err, "segment_root length")
}

func wireTestSegmentRecord() fsperas.SegmentWitnessRecord {
	var root [32]byte
	root[0] = 9
	payload := []byte("wire-segment-payload")
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	if err != nil {
		panic(err)
	}
	return fsperas.SegmentWitnessRecord{
		EpochID:              7,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   uint64(len(payload)),
		SegmentPayload:       payload,
		OperationCount:       128,
		EntryCount:           256,
		TimestampUnixNano:    1234,
		HolderID:             "holder-a",
	}
}
