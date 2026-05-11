package peras

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/stretchr/testify/require"
)

func TestSegmentWitnessRecordCodecRoundTrip(t *testing.T) {
	record := testSegmentWitnessRecord()

	payload, err := EncodeSegmentWitnessRecord(record)
	require.NoError(t, err)
	frame, err := DecodeWitnessFrame(payload)
	require.NoError(t, err)

	require.Equal(t, WitnessRecordSegment, frame.Kind)
	require.Equal(t, record, frame.Segment)
}

func TestSegmentWitnessCodecRejectsInvalidRecords(t *testing.T) {
	_, err := EncodeSegmentWitnessRecord(SegmentWitnessRecord{EpochID: 1})
	require.ErrorIs(t, err, ErrInvalidWitnessRecord)

	payload, err := EncodeSegmentWitnessRecord(testSegmentWitnessRecord())
	require.NoError(t, err)
	payload[0] ^= 0xff
	_, err = DecodeWitnessFrame(payload)
	require.ErrorIs(t, err, ErrInvalidWitnessRecord)
}

func TestWALWitnessLogAppendSegmentProbeAndReopen(t *testing.T) {
	dir := t.TempDir()
	manager, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	log, err := NewWALWitnessLog(manager, wal.DurabilityFsync)
	require.NoError(t, err)

	segment := testSegmentWitnessRecord()
	other := segment
	other.EpochID = 2
	other.SegmentRoot[0] ^= 0xff

	info, err := log.AppendSegment(context.Background(), segment)
	require.NoError(t, err)
	require.Equal(t, wal.RecordTypePerasWitness, info.Type)
	_, err = log.AppendSegment(context.Background(), other)
	require.NoError(t, err)
	require.NoError(t, manager.Close())

	reopened, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	reopenedLog, err := NewWALWitnessLog(reopened, wal.DurabilityFsync)
	require.NoError(t, err)

	snapshot, err := reopenedLog.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []SegmentWitnessRecord{segment}, snapshot.Segments)

	snapshot, err = reopenedLog.Probe(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, []SegmentWitnessRecord{other}, snapshot.Segments)
}

func BenchmarkEncodeSegmentWitnessRecord(b *testing.B) {
	record := testSegmentWitnessRecord()

	b.ReportAllocs()
	for b.Loop() {
		payload, err := EncodeSegmentWitnessRecord(record)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func testSegmentWitnessRecord() SegmentWitnessRecord {
	var root [32]byte
	root[0] = 7
	payload := []byte("segment-payload")
	digest, err := PerasSegmentPayloadDigest(payload)
	if err != nil {
		panic(err)
	}
	return SegmentWitnessRecord{
		EpochID:              1,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   uint64(len(payload)),
		SegmentPayload:       payload,
		OperationCount:       64,
		EntryCount:           128,
		TimestampUnixNano:    100,
		HolderID:             "holder-a",
	}
}
