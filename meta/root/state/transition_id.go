package state

import (
	"fmt"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
)

// TransitionIDFromEvent returns one stable identity for a rooted transition target.
func TransitionIDFromEvent(event rootevent.Event) string {
	switch {
	case event.PeerChange != nil:
		return fmt.Sprintf("peer:%d:%d:%d", event.PeerChange.RegionID, event.PeerChange.StoreID, event.PeerChange.PeerID)
	case event.RangeSplit != nil:
		return fmt.Sprintf("split:%d:%x", event.RangeSplit.ParentRegionID, event.RangeSplit.SplitKey)
	case event.RangeMerge != nil:
		return fmt.Sprintf("merge:%d:%d", event.RangeMerge.LeftRegionID, event.RangeMerge.RightRegionID)
	default:
		return ""
	}
}
