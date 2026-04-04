package state

import (
	"fmt"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

type RangeChangeLifecycleDecision uint8

const (
	RangeChangeLifecycleApply RangeChangeLifecycleDecision = iota
	RangeChangeLifecycleSkip
)

type RangeChangeCompletionState uint8

const (
	RangeChangeCompletionOpen RangeChangeCompletionState = iota
	RangeChangeCompletionPending
	RangeChangeCompletionCompleted
)

func PendingRangeChangeFromEvent(event rootevent.Event) (uint64, PendingRangeChange, bool) {
	switch event.Kind {
	case rootevent.KindRegionSplitPlanned, rootevent.KindRegionSplitCommitted:
		if event.RangeSplit == nil {
			return 0, PendingRangeChange{}, false
		}
		return event.RangeSplit.ParentRegionID, PendingRangeChange{
			Kind:           PendingRangeChangeSplit,
			ParentRegionID: event.RangeSplit.ParentRegionID,
			LeftRegionID:   event.RangeSplit.Left.RegionID,
			RightRegionID:  event.RangeSplit.Right.RegionID,
			Left:           event.RangeSplit.Left.Clone(),
			Right:          event.RangeSplit.Right.Clone(),
		}, true
	case rootevent.KindRegionMergePlanned, rootevent.KindRegionMerged:
		if event.RangeMerge == nil {
			return 0, PendingRangeChange{}, false
		}
		return event.RangeMerge.Merged.RegionID, PendingRangeChange{
			Kind:          PendingRangeChangeMerge,
			LeftRegionID:  event.RangeMerge.LeftRegionID,
			RightRegionID: event.RangeMerge.RightRegionID,
			Merged:        event.RangeMerge.Merged.Clone(),
		}, true
	default:
		return 0, PendingRangeChange{}, false
	}
}

func PendingRangeChangeMatchesEvent(change PendingRangeChange, event rootevent.Event) bool {
	_, expected, ok := PendingRangeChangeFromEvent(event)
	if !ok {
		return false
	}
	switch expected.Kind {
	case PendingRangeChangeSplit:
		return change.Kind == expected.Kind &&
			change.ParentRegionID == expected.ParentRegionID &&
			change.Left.Equal(expected.Left) &&
			change.Right.Equal(expected.Right)
	case PendingRangeChangeMerge:
		return change.Kind == expected.Kind &&
			change.LeftRegionID == expected.LeftRegionID &&
			change.RightRegionID == expected.RightRegionID &&
			change.Merged.Equal(expected.Merged)
	default:
		return false
	}
}

func RangeChangeStateMatches(descriptors map[uint64]descriptor.Descriptor, event rootevent.Event) bool {
	switch event.Kind {
	case rootevent.KindRegionSplitPlanned, rootevent.KindRegionSplitCommitted:
		if event.RangeSplit == nil {
			return false
		}
		return splitStateMatches(descriptors, event.RangeSplit.ParentRegionID, event.RangeSplit.Left, event.RangeSplit.Right)
	case rootevent.KindRegionMergePlanned, rootevent.KindRegionMerged:
		if event.RangeMerge == nil {
			return false
		}
		return mergeStateMatches(descriptors, event.RangeMerge.LeftRegionID, event.RangeMerge.RightRegionID, event.RangeMerge.Merged)
	default:
		return false
	}
}

func ObserveRangeChangeCompletion(pendingRangeChanges map[uint64]PendingRangeChange, descriptors map[uint64]descriptor.Descriptor, event rootevent.Event) RangeChangeCompletionState {
	key, _, ok := PendingRangeChangeFromEvent(event)
	if !ok {
		return RangeChangeCompletionOpen
	}
	if pending, exists := pendingRangeChanges[key]; exists && PendingRangeChangeMatchesEvent(pending, event) {
		return RangeChangeCompletionPending
	}
	if RangeChangeStateMatches(descriptors, event) {
		return RangeChangeCompletionCompleted
	}
	return RangeChangeCompletionOpen
}

func EvaluateRangeChangeLifecycle(pendingRangeChanges map[uint64]PendingRangeChange, descriptors map[uint64]descriptor.Descriptor, event rootevent.Event) (RangeChangeLifecycleDecision, error) {
	key, _, ok := PendingRangeChangeFromEvent(event)
	if !ok {
		return RangeChangeLifecycleApply, nil
	}
	pending, exists := pendingRangeChanges[key]
	completion := ObserveRangeChangeCompletion(pendingRangeChanges, descriptors, event)
	switch event.Kind {
	case rootevent.KindRegionSplitPlanned, rootevent.KindRegionMergePlanned:
		switch completion {
		case RangeChangeCompletionCompleted, RangeChangeCompletionPending:
			return RangeChangeLifecycleSkip, nil
		default:
			if !exists {
				return RangeChangeLifecycleApply, nil
			}
			return RangeChangeLifecycleApply, fmt.Errorf("pending range change already exists for region %d", key)
		}
	case rootevent.KindRegionSplitCommitted, rootevent.KindRegionMerged:
		switch completion {
		case RangeChangeCompletionPending:
			return RangeChangeLifecycleApply, nil
		case RangeChangeCompletionCompleted:
			return RangeChangeLifecycleSkip, nil
		default:
			if exists {
				if PendingRangeChangeMatchesEvent(pending, event) {
					return RangeChangeLifecycleApply, nil
				}
				return RangeChangeLifecycleApply, fmt.Errorf("range change apply does not match pending target for region %d", key)
			}
			return RangeChangeLifecycleApply, nil
		}
	}
	return RangeChangeLifecycleApply, nil
}

func splitStateMatches(descriptors map[uint64]descriptor.Descriptor, parentRegionID uint64, left, right descriptor.Descriptor) bool {
	if parentRegionID != left.RegionID {
		if _, ok := descriptors[parentRegionID]; ok {
			return false
		}
	}
	gotLeft, ok := descriptors[left.RegionID]
	if !ok || !gotLeft.Equal(left) {
		return false
	}
	gotRight, ok := descriptors[right.RegionID]
	return ok && gotRight.Equal(right)
}

func mergeStateMatches(descriptors map[uint64]descriptor.Descriptor, leftRegionID, rightRegionID uint64, merged descriptor.Descriptor) bool {
	if _, ok := descriptors[leftRegionID]; ok {
		return false
	}
	if _, ok := descriptors[rightRegionID]; ok {
		return false
	}
	gotMerged, ok := descriptors[merged.RegionID]
	return ok && gotMerged.Equal(merged)
}
