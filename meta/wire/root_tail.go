package wire

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

// RootObservedToProto encodes one ObservedCommitted as its RootCheckpoint +
// RootCommittedTail pair used on the meta-root gRPC service.
func RootObservedToProto(observed rootstorage.ObservedCommitted) (*metapb.RootCheckpoint, *metapb.RootCommittedTail) {
	return RootCheckpointToProto(observed.Checkpoint), RootCommittedTailToProto(observed.Tail)
}

// RootObservedFromProto decodes a RootCheckpoint + RootCommittedTail pair back
// into ObservedCommitted.
func RootObservedFromProto(checkpoint *metapb.RootCheckpoint, tail *metapb.RootCommittedTail) rootstorage.ObservedCommitted {
	return rootstorage.ObservedCommitted{
		Checkpoint: RootCheckpointFromProto(checkpoint),
		Tail:       RootCommittedTailFromProto(tail),
	}
}

// RootCheckpointToProto encodes a storage Checkpoint as a RootCheckpoint proto.
func RootCheckpointToProto(checkpoint rootstorage.Checkpoint) *metapb.RootCheckpoint {
	return RootSnapshotToProto(checkpoint.Snapshot, uint64(checkpoint.TailOffset))
}

// RootCheckpointFromProto decodes a RootCheckpoint proto back into storage.Checkpoint.
func RootCheckpointFromProto(pbCheckpoint *metapb.RootCheckpoint) rootstorage.Checkpoint {
	snapshot, tailOffset := RootSnapshotFromProto(pbCheckpoint)
	return rootstorage.Checkpoint{Snapshot: snapshot, TailOffset: int64(tailOffset)}
}

// RootTailTokenToProto encodes a TailToken as its proto form.
func RootTailTokenToProto(token rootstorage.TailToken) *metapb.RootTailToken {
	return &metapb.RootTailToken{
		Cursor:   RootCursorToProto(token.Cursor),
		Revision: token.Revision,
	}
}

// RootTailTokenFromProto decodes a RootTailToken proto back into TailToken.
func RootTailTokenFromProto(token *metapb.RootTailToken) rootstorage.TailToken {
	if token == nil {
		return rootstorage.TailToken{}
	}
	return rootstorage.TailToken{
		Cursor:   RootCursorFromProto(token.GetCursor()),
		Revision: token.GetRevision(),
	}
}

// RootCommittedTailToProto encodes a storage CommittedTail as its proto form.
func RootCommittedTailToProto(tail rootstorage.CommittedTail) *metapb.RootCommittedTail {
	records := make([]*metapb.RootCommittedEvent, 0, len(tail.Records))
	for _, record := range tail.Records {
		records = append(records, &metapb.RootCommittedEvent{
			Cursor: RootCursorToProto(record.Cursor),
			Event:  RootEventToProto(record.Event),
		})
	}
	return &metapb.RootCommittedTail{
		RequestedOffset: tail.RequestedOffset,
		StartOffset:     tail.StartOffset,
		EndOffset:       tail.EndOffset,
		Records:         records,
	}
}

// RootCommittedTailFromProto decodes a RootCommittedTail proto back into storage.CommittedTail.
func RootCommittedTailFromProto(tail *metapb.RootCommittedTail) rootstorage.CommittedTail {
	if tail == nil {
		return rootstorage.CommittedTail{}
	}
	records := make([]rootstorage.CommittedEvent, 0, len(tail.GetRecords()))
	for _, record := range tail.GetRecords() {
		event := RootEventFromProto(record.GetEvent())
		if event.Kind == rootevent.KindUnknown {
			continue
		}
		records = append(records, rootstorage.CommittedEvent{
			Cursor: RootCursorFromProto(record.GetCursor()),
			Event:  event,
		})
	}
	return rootstorage.CommittedTail{
		RequestedOffset: tail.GetRequestedOffset(),
		StartOffset:     tail.GetStartOffset(),
		EndOffset:       tail.GetEndOffset(),
		Records:         records,
	}
}

// RootTailAdvanceToObservedResponse splits a TailAdvance into the four proto
// fields (after token, advanced token, checkpoint, tail) that ObserveTail and
// WaitTail responses carry.
func RootTailAdvanceToObservedResponse(advance rootstorage.TailAdvance) (*metapb.RootTailToken, *metapb.RootTailToken, *metapb.RootCheckpoint, *metapb.RootCommittedTail) {
	checkpoint, tail := RootObservedToProto(advance.Observed)
	return RootTailTokenToProto(advance.After), RootTailTokenToProto(advance.Token), checkpoint, tail
}

// RootTailAdvanceFromProto reassembles a TailAdvance from its four proto pieces.
func RootTailAdvanceFromProto(after, token *metapb.RootTailToken, checkpoint *metapb.RootCheckpoint, tail *metapb.RootCommittedTail) rootstorage.TailAdvance {
	return rootstorage.TailAdvance{
		After:    RootTailTokenFromProto(after),
		Token:    RootTailTokenFromProto(token),
		Observed: RootObservedFromProto(checkpoint, tail),
	}
}

// RootFallbackObservedFromSnapshot builds an empty-tail ObservedCommitted
// from a plain rooted snapshot. Used by server-side fallback when the backend
// does not implement ObserveCommitted directly.
func RootFallbackObservedFromSnapshot(snapshot rootstate.Snapshot) rootstorage.ObservedCommitted {
	return rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{Snapshot: rootstate.CloneSnapshot(snapshot)},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 0,
			StartOffset:     0,
			EndOffset:       0,
		},
	}
}

// RootAllocatorKindToProto encodes an allocator kind.
func RootAllocatorKindToProto(kind rootstate.AllocatorKind) metapb.RootAllocatorKind {
	switch kind {
	case rootstate.AllocatorKindID:
		return metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_ID
	case rootstate.AllocatorKindTSO:
		return metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_TSO
	default:
		return metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_UNSPECIFIED
	}
}

// RootAllocatorKindFromProto decodes an allocator kind, rejecting the
// unspecified value.
func RootAllocatorKindFromProto(kind metapb.RootAllocatorKind) (rootstate.AllocatorKind, bool) {
	switch kind {
	case metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_ID:
		return rootstate.AllocatorKindID, true
	case metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_TSO:
		return rootstate.AllocatorKindTSO, true
	default:
		return rootstate.AllocatorKindUnknown, false
	}
}
