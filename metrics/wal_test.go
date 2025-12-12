package metrics

import (
	"testing"

	"github.com/feichai0017/NoKV/manifest"
)

func TestWALRecordMetricsTotals(t *testing.T) {
	m := WALRecordMetrics{Entries: 1, RaftEntries: 2, RaftStates: 3, RaftSnapshots: 4, Other: 5}
	if got := m.Total(); got != 15 {
		t.Fatalf("Total = %d, want 15", got)
	}
	if got := m.RaftRecords(); got != 9 {
		t.Fatalf("RaftRecords = %d, want 9", got)
	}
}

func TestAnalyzeWALBacklogAndWarning(t *testing.T) {
	metrics := &WALMetrics{
		ActiveSegment:           5,
		ActiveSize:              0,
		SegmentCount:            6,
		RecordCounts:            WALRecordMetrics{Entries: 10, RaftEntries: 4},
		SegmentsWithRaftRecords: 2,
	}
	segMetrics := map[uint32]WALRecordMetrics{
		1: {RaftEntries: 1},
		3: {RaftEntries: 1},
		5: {Entries: 5},
	}
	ptrs := map[uint64]manifest.RaftLogPointer{
		1: {GroupID: 1, Segment: 4},
		2: {GroupID: 2, SegmentIndex: 3},
	}

	analysis := AnalyzeWALBacklog(metrics, segMetrics, ptrs)
	if analysis.ActiveSegment != 5 || analysis.SegmentCount != 6 {
		t.Fatalf("unexpected active/segments: %+v", analysis)
	}
	if analysis.TypedRecordRatio <= 0 {
		t.Fatalf("expected typed ratio > 0")
	}
	if analysis.RetainSegment != 3 {
		t.Fatalf("retain segment = %d, want 3", analysis.RetainSegment)
	}
	wantRemovable := []uint32{1}
	if len(analysis.RemovableSegments) != len(wantRemovable) || analysis.RemovableSegments[0] != wantRemovable[0] {
		t.Fatalf("removable = %v, want %v", analysis.RemovableSegments, wantRemovable)
	}

	warn, reason := WALTypedWarning(analysis.TypedRecordRatio, analysis.SegmentsWithRaft, 0.1, 1)
	if !warn || reason == "" {
		t.Fatalf("expected warning, got warn=%v reason=%q", warn, reason)
	}
}
