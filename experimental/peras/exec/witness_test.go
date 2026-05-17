// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"testing"

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
