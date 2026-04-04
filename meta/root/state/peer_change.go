package state

import rootevent "github.com/feichai0017/NoKV/meta/root/event"

func IsPeerChangePlannedEvent(event rootevent.Event) bool {
	switch event.Kind {
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerRemovalPlanned:
		return true
	default:
		return false
	}
}

func IsPeerChangeAppliedEvent(event rootevent.Event) bool {
	switch event.Kind {
	case rootevent.KindPeerAdded, rootevent.KindPeerRemoved:
		return true
	default:
		return false
	}
}

func PendingPeerChangeFromEvent(event rootevent.Event, stage PendingPeerChangeStage) (PendingPeerChange, bool) {
	if event.PeerChange == nil {
		return PendingPeerChange{}, false
	}
	var kind PendingPeerChangeKind
	switch event.Kind {
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerAdded:
		kind = PendingPeerChangeAddition
	case rootevent.KindPeerRemovalPlanned, rootevent.KindPeerRemoved:
		kind = PendingPeerChangeRemoval
	default:
		return PendingPeerChange{}, false
	}
	return PendingPeerChange{
		Kind:    kind,
		Stage:   stage,
		StoreID: event.PeerChange.StoreID,
		PeerID:  event.PeerChange.PeerID,
		Target:  event.PeerChange.Region.Clone(),
	}, true
}

func PendingPeerChangeMatchesEvent(change PendingPeerChange, event rootevent.Event) bool {
	if event.PeerChange == nil {
		return false
	}
	expected, ok := PendingPeerChangeFromEvent(event, change.Stage)
	if !ok {
		return false
	}
	return change.Kind == expected.Kind &&
		change.StoreID == expected.StoreID &&
		change.PeerID == expected.PeerID &&
		change.Target.Equal(expected.Target)
}
