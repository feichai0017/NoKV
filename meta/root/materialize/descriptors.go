// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package materialize

import (
	"slices"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/meta/topology"
)

// ApplyEventToDescriptors applies one rooted topology event into a materialized descriptor catalog.
func ApplyEventToDescriptors(descriptors map[uint64]topology.Descriptor, event rootevent.Event) {
	if descriptors == nil {
		return
	}
	switch {
	case event.RegionDescriptor != nil:
		descriptors[event.RegionDescriptor.Descriptor.RegionID] = event.RegionDescriptor.Descriptor.Clone()
	case event.RegionRemoval != nil:
		delete(descriptors, event.RegionRemoval.RegionID)
	case event.PeerChange != nil && (event.Kind == rootevent.KindPeerAdditionCancelled || event.Kind == rootevent.KindPeerRemovalCancelled):
		if event.PeerChange.Base.RegionID == 0 {
			delete(descriptors, event.PeerChange.RegionID)
			return
		}
		descriptors[event.PeerChange.Base.RegionID] = event.PeerChange.Base.Clone()
	case event.PeerChange != nil:
		descriptors[event.PeerChange.RegionID] = event.PeerChange.Region.Clone()
	}
}

// SnapshotDescriptorEvents materializes descriptor truth into a stable event sequence for bootstrap/recovery callers.
func SnapshotDescriptorEvents(descs map[uint64]topology.Descriptor) []rootevent.Event {
	if len(descs) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(descs))
	for id := range descs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	events := make([]rootevent.Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, rootevent.RegionDescriptorPublished(descs[id]))
	}
	return events
}
