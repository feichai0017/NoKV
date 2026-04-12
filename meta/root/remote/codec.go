package remote

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

func observedToProto(observed rootstorage.ObservedCommitted) (*metapb.RootCheckpoint, *metapb.RootCommittedTail) {
	return checkpointToProto(observed.Checkpoint), committedTailToProto(observed.Tail)
}

func observedFromProto(checkpoint *metapb.RootCheckpoint, tail *metapb.RootCommittedTail) rootstorage.ObservedCommitted {
	return rootstorage.ObservedCommitted{
		Checkpoint: checkpointFromProto(checkpoint),
		Tail:       committedTailFromProto(tail),
	}
}

func checkpointToProto(checkpoint rootstorage.Checkpoint) *metapb.RootCheckpoint {
	return metawire.RootSnapshotToProto(checkpoint.Snapshot, uint64(checkpoint.TailOffset))
}

func checkpointFromProto(pbCheckpoint *metapb.RootCheckpoint) rootstorage.Checkpoint {
	snapshot, tailOffset := metawire.RootSnapshotFromProto(pbCheckpoint)
	return rootstorage.Checkpoint{Snapshot: snapshot, TailOffset: int64(tailOffset)}
}

func tailTokenToProto(token rootstorage.TailToken) *metapb.RootTailToken {
	return &metapb.RootTailToken{
		Cursor:   metawire.RootCursorToProto(token.Cursor),
		Revision: token.Revision,
	}
}

func tailTokenFromProto(token *metapb.RootTailToken) rootstorage.TailToken {
	if token == nil {
		return rootstorage.TailToken{}
	}
	return rootstorage.TailToken{
		Cursor:   metawire.RootCursorFromProto(token.GetCursor()),
		Revision: token.GetRevision(),
	}
}

func committedTailToProto(tail rootstorage.CommittedTail) *metapb.RootCommittedTail {
	records := make([]*metapb.RootCommittedEvent, 0, len(tail.Records))
	for _, record := range tail.Records {
		records = append(records, &metapb.RootCommittedEvent{
			Cursor: metawire.RootCursorToProto(record.Cursor),
			Event:  metawire.RootEventToProto(record.Event),
		})
	}
	return &metapb.RootCommittedTail{
		RequestedOffset: tail.RequestedOffset,
		StartOffset:     tail.StartOffset,
		EndOffset:       tail.EndOffset,
		Records:         records,
	}
}

func committedTailFromProto(tail *metapb.RootCommittedTail) rootstorage.CommittedTail {
	if tail == nil {
		return rootstorage.CommittedTail{}
	}
	records := make([]rootstorage.CommittedEvent, 0, len(tail.GetRecords()))
	for _, record := range tail.GetRecords() {
		event := metawire.RootEventFromProto(record.GetEvent())
		if event.Kind == rootevent.KindUnknown {
			continue
		}
		records = append(records, rootstorage.CommittedEvent{
			Cursor: metawire.RootCursorFromProto(record.GetCursor()),
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

func tailAdvanceToObservedResponse(advance rootstorage.TailAdvance) (*metapb.RootTailToken, *metapb.RootTailToken, *metapb.RootCheckpoint, *metapb.RootCommittedTail) {
	checkpoint, tail := observedToProto(advance.Observed)
	return tailTokenToProto(advance.After), tailTokenToProto(advance.Token), checkpoint, tail
}

func tailAdvanceFromProto(after, token *metapb.RootTailToken, checkpoint *metapb.RootCheckpoint, tail *metapb.RootCommittedTail) rootstorage.TailAdvance {
	return rootstorage.TailAdvance{
		After:    tailTokenFromProto(after),
		Token:    tailTokenFromProto(token),
		Observed: observedFromProto(checkpoint, tail),
	}
}

func fallbackObservedFromSnapshot(snapshot rootstate.Snapshot) rootstorage.ObservedCommitted {
	return rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{Snapshot: rootstate.CloneSnapshot(snapshot)},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 0,
			StartOffset:     0,
			EndOffset:       0,
		},
	}
}
