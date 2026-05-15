// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"testing"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/stretchr/testify/require"
)

func TestSplitSegmentWitnessRecordsCapsBatchBytes(t *testing.T) {
	records := []fsperas.SegmentWitnessRecord{
		testSegmentWitnessRecordWithPayload(10),
		testSegmentWitnessRecordWithPayload(10),
		testSegmentWitnessRecordWithPayload(10),
	}

	batches := splitSegmentWitnessRecords(records, 600)

	require.Len(t, batches, 2)
	require.Equal(t, records[:2], batches[0])
	require.Equal(t, records[2:], batches[1])
}

func TestSplitSegmentWitnessRecordsAllowsSingleOversizedRecord(t *testing.T) {
	records := []fsperas.SegmentWitnessRecord{
		testSegmentWitnessRecordWithPayload(1024),
		testSegmentWitnessRecordWithPayload(10),
	}

	batches := splitSegmentWitnessRecords(records, 600)

	require.Len(t, batches, 2)
	require.Equal(t, records[:1], batches[0])
	require.Equal(t, records[1:], batches[1])
}

func testSegmentWitnessRecordWithPayload(size int) fsperas.SegmentWitnessRecord {
	return fsperas.SegmentWitnessRecord{SegmentPayload: make([]byte, size)}
}
