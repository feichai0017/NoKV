package state

import (
	"fmt"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
)

// TransitionIDFromEvent returns one stable identity for a rooted transition target.
func TransitionIDFromEvent(event rootevent.Event) string {
	switch {
	case event.PeerChange != nil:
		return fmt.Sprintf("peer:%d:%s:%d:%d", event.PeerChange.RegionID, peerTransitionAction(event.Kind), event.PeerChange.StoreID, event.PeerChange.PeerID)
	case event.RangeSplit != nil:
		return fmt.Sprintf("split:%d:%x", event.RangeSplit.ParentRegionID, event.RangeSplit.SplitKey)
	case event.RangeMerge != nil:
		return fmt.Sprintf("merge:%d:%d", event.RangeMerge.LeftRegionID, event.RangeMerge.RightRegionID)
	default:
		return ""
	}
}

func peerTransitionAction(kind rootevent.Kind) string {
	switch kind {
	case rootevent.KindPeerAdditionPlanned, rootevent.KindPeerAdded, rootevent.KindPeerAdditionCancelled:
		return "add"
	case rootevent.KindPeerRemovalPlanned, rootevent.KindPeerRemoved, rootevent.KindPeerRemovalCancelled:
		return "remove"
	default:
		return "unknown"
	}
}
