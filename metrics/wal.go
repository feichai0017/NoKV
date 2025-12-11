package metrics

import (
	"fmt"
	"math"
	"slices"

	"github.com/feichai0017/NoKV/manifest"
)

// WALRecordMetrics summarises counts per record type.
type WALRecordMetrics struct {
	Entries       uint64 `json:"entries"`
	RaftEntries   uint64 `json:"raft_entries"`
	RaftStates    uint64 `json:"raft_states"`
	RaftSnapshots uint64 `json:"raft_snapshots"`
	Other         uint64 `json:"other"`
}

// Total returns the sum across all record types.
func (m WALRecordMetrics) Total() uint64 {
	return m.Entries + m.RaftEntries + m.RaftStates + m.RaftSnapshots + m.Other
}

// RaftRecords returns the total count of raft-specific records.
func (m WALRecordMetrics) RaftRecords() uint64 {
	return m.RaftEntries + m.RaftStates + m.RaftSnapshots
}

// WALMetrics captures runtime information about WAL manager state.
type WALMetrics struct {
	ActiveSegment           uint32
	SegmentCount            int
	ActiveSize              int64
	RemovedSegments         uint64
	RecordCounts            WALRecordMetrics
	SegmentsWithRaftRecords int
}

// WALBacklogAnalysis summarizes WAL backlog state and GC candidates.
type WALBacklogAnalysis struct {
	ActiveSegment       uint32
	ActiveSize          int64
	SegmentCount        int
	RecordCounts        WALRecordMetrics
	SegmentsWithRaft    int
	RemovableSegments   []uint32
	TypedRecordRatio    float64
	RetainSegment       uint32
	SegmentMetricsCount int
}

// AnalyzeWALBacklog inspects WAL metrics, per-segment metrics, and raft pointers to
// derive GC candidates and typed record ratios.
func AnalyzeWALBacklog(metrics *WALMetrics, segmentMetrics map[uint32]WALRecordMetrics, ptrs map[uint64]manifest.RaftLogPointer) WALBacklogAnalysis {
	var analysis WALBacklogAnalysis
	if metrics != nil {
		analysis.ActiveSegment = metrics.ActiveSegment
		analysis.ActiveSize = metrics.ActiveSize
		analysis.SegmentCount = metrics.SegmentCount
		analysis.RecordCounts = metrics.RecordCounts
		analysis.SegmentsWithRaft = metrics.SegmentsWithRaftRecords
	}
	if len(segmentMetrics) > 0 {
		analysis.SegmentMetricsCount = len(segmentMetrics)
		if analysis.SegmentsWithRaft == 0 {
			for _, m := range segmentMetrics {
				if m.RaftRecords() > 0 {
					analysis.SegmentsWithRaft++
				}
			}
		}
	}
	if total := analysis.RecordCounts.Total(); total > 0 {
		analysis.TypedRecordRatio = float64(analysis.RecordCounts.RaftRecords()) / float64(total)
	}

	retainSegment := uint32(math.MaxUint32)
	if len(ptrs) > 0 {
		effectiveActive := analysis.ActiveSegment
		if analysis.ActiveSize == 0 && effectiveActive > 0 {
			effectiveActive--
		}
		for _, ptr := range ptrs {
			if ptr.Segment > 0 && ptr.Segment < retainSegment {
				retainSegment = ptr.Segment
			}
			if ptr.SegmentIndex > 0 {
				if idx := uint32(ptr.SegmentIndex); idx < retainSegment {
					retainSegment = idx
				}
			}
			if ptr.Segment == 0 && ptr.SegmentIndex == 0 && effectiveActive > 0 && retainSegment == math.MaxUint32 {
				retainSegment = effectiveActive
			}
		}
	}
	if retainSegment == math.MaxUint32 {
		retainSegment = 0
	}
	analysis.RetainSegment = retainSegment

	if retainSegment > 0 && len(segmentMetrics) > 0 {
		candidates := make([]uint32, 0, len(segmentMetrics))
		for id, metrics := range segmentMetrics {
			if metrics.RaftRecords() == 0 {
				continue
			}
			if id < retainSegment {
				candidates = append(candidates, id)
			}
		}
		slices.Sort(candidates)
		analysis.RemovableSegments = candidates
	}

	return analysis
}

// WALTypedWarning reports whether typed-record ratio or segment count exceeds thresholds.
func WALTypedWarning(ratio float64, segments int, ratioThreshold float64, segmentThreshold int64) (bool, string) {
	if ratioThreshold > 0 && ratio >= ratioThreshold {
		return true, fmt.Sprintf("typed record ratio %.2f exceeds threshold %.2f", ratio, ratioThreshold)
	}
	if segmentThreshold > 0 && int64(segments) >= segmentThreshold {
		return true, fmt.Sprintf("segments with raft records %d exceeds threshold %d", segments, segmentThreshold)
	}
	return false, ""
}
