package catalog

import (
	pdoperator "github.com/feichai0017/NoKV/coordinator/operator"
	pdview "github.com/feichai0017/NoKV/coordinator/view"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"time"
)

// Cluster stores in-memory Coordinator metadata and provides route lookups.
//
// NOTE: Cluster intentionally keeps only the in-memory metadata/state model.
// Coordinator RPC wiring and persistence are handled by higher layers
// (coordinator/server and coordinator/storage).
type Cluster struct {
	stores      *pdview.StoreHealthView
	regions     *pdview.RegionDirectoryView
	transitions *pdview.PendingView
	operators   *pdoperator.Runtime
	rootToken   rootstorage.TailToken
}

// NewCluster creates an empty in-memory cluster metadata view.
func NewCluster() *Cluster {
	return &Cluster{
		stores:      pdview.NewStoreHealthView(),
		regions:     pdview.NewRegionDirectoryView(),
		transitions: pdview.NewPendingView(),
		operators:   pdoperator.NewRuntime(),
	}
}

// UpsertStoreHeartbeat updates store metadata from a store heartbeat.
func (c *Cluster) UpsertStoreHeartbeat(stats pdview.StoreStats) error {
	if c == nil {
		return nil
	}
	return c.stores.Upsert(stats)
}

// RemoveStore removes a store from Coordinator metadata.
func (c *Cluster) RemoveStore(storeID uint64) {
	if c == nil {
		return
	}
	c.stores.Remove(storeID)
}

// StoreSnapshot returns a stable copy of tracked store metadata.
func (c *Cluster) StoreSnapshot() []pdview.StoreStats {
	if c == nil {
		return nil
	}
	return c.stores.Snapshot()
}

// PublishRegionDescriptor applies one rooted region descriptor into the runtime
// Coordinator route view.
func (c *Cluster) PublishRegionDescriptor(desc descriptor.Descriptor) error {
	if c == nil {
		return nil
	}
	return c.regions.Upsert(desc)
}

// ValidateRegionDescriptor checks whether one rooted descriptor can be applied
// to the current runtime view without mutating in-memory state.
func (c *Cluster) ValidateRegionDescriptor(desc descriptor.Descriptor) error {
	if c == nil {
		return nil
	}
	clone := c.clone()
	return clone.PublishRegionDescriptor(desc)
}

// PublishRootEvent applies one explicit rooted truth event into the runtime Coordinator
// route view.
func (c *Cluster) PublishRootEvent(event rootevent.Event) error {
	if c == nil {
		return nil
	}
	if err := c.applyRootEventToRegions(event); err != nil {
		return err
	}
	c.applyRootEventToTransitions(event)
	return nil
}

func (c *Cluster) applyRootEventToRegions(event rootevent.Event) error {
	if c == nil {
		return nil
	}
	switch {
	case event.RegionDescriptor != nil:
		return c.PublishRegionDescriptor(event.RegionDescriptor.Descriptor)
	case event.RegionRemoval != nil:
		c.RemoveRegion(event.RegionRemoval.RegionID)
		return nil
	case event.PeerChange != nil && (event.Kind == rootevent.KindPeerAdditionCancelled || event.Kind == rootevent.KindPeerRemovalCancelled):
		if event.PeerChange.Base.RegionID == 0 {
			c.RemoveRegion(event.PeerChange.RegionID)
			return nil
		}
		return c.PublishRegionDescriptor(event.PeerChange.Base)
	case event.RangeSplit != nil && event.Kind == rootevent.KindRegionSplitCancelled:
		c.RemoveRegion(event.RangeSplit.Left.RegionID)
		c.RemoveRegion(event.RangeSplit.Right.RegionID)
		if event.RangeSplit.BaseParent.RegionID != 0 {
			return c.PublishRegionDescriptor(event.RangeSplit.BaseParent)
		}
		return nil
	case event.RangeMerge != nil && event.Kind == rootevent.KindRegionMergeCancelled:
		c.RemoveRegion(event.RangeMerge.Merged.RegionID)
		if event.RangeMerge.BaseLeft.RegionID != 0 {
			if err := c.PublishRegionDescriptor(event.RangeMerge.BaseLeft); err != nil {
				return err
			}
		}
		if event.RangeMerge.BaseRight.RegionID != 0 {
			return c.PublishRegionDescriptor(event.RangeMerge.BaseRight)
		}
		return nil
	case event.RangeSplit != nil:
		c.RemoveRegion(event.RangeSplit.ParentRegionID)
		if err := c.PublishRegionDescriptor(event.RangeSplit.Left); err != nil {
			return err
		}
		return c.PublishRegionDescriptor(event.RangeSplit.Right)
	case event.RangeMerge != nil:
		c.RemoveRegion(event.RangeMerge.LeftRegionID)
		c.RemoveRegion(event.RangeMerge.RightRegionID)
		return c.PublishRegionDescriptor(event.RangeMerge.Merged)
	case event.PeerChange != nil:
		return c.PublishRegionDescriptor(event.PeerChange.Region)
	default:
		return nil
	}
}

// ValidateRootEvent checks whether one rooted truth event can be applied to the
// current runtime view without mutating in-memory state.
func (c *Cluster) ValidateRootEvent(event rootevent.Event) error {
	if c == nil {
		return nil
	}
	clone := c.clone()
	return clone.PublishRootEvent(event)
}

// RemoveRegion removes a region from Coordinator metadata and reports whether the region existed before removal.
func (c *Cluster) RemoveRegion(regionID uint64) bool {
	if c == nil {
		return false
	}
	return c.regions.Remove(regionID)
}

// HasRegion reports whether the runtime view currently tracks regionID.
func (c *Cluster) HasRegion(regionID uint64) bool {
	if c == nil || regionID == 0 {
		return false
	}
	for _, info := range c.RegionSnapshot() {
		if info.Descriptor.RegionID == regionID {
			return true
		}
	}
	return false
}

// GetRegionDescriptor returns the rooted descriptor tracked for regionID.
func (c *Cluster) GetRegionDescriptor(regionID uint64) (descriptor.Descriptor, bool) {
	if c == nil {
		return descriptor.Descriptor{}, false
	}
	return c.regions.Descriptor(regionID)
}

// TouchRegionHeartbeat refreshes the runtime heartbeat timestamp without
// mutating rooted topology truth.
func (c *Cluster) TouchRegionHeartbeat(regionID uint64) bool {
	if c == nil {
		return false
	}
	return c.regions.Touch(regionID, time.Now())
}

// RegionSnapshot returns a stable copy of tracked region metadata.
func (c *Cluster) RegionSnapshot() []pdview.RegionInfo {
	if c == nil {
		return nil
	}
	return c.regions.Snapshot()
}

// ReplaceRegionSnapshot replaces the region directory view from one rooted
// snapshot while preserving store-health runtime observations.
func (c *Cluster) ReplaceRegionSnapshot(descriptors map[uint64]descriptor.Descriptor) {
	c.ReplaceRootSnapshot(descriptors, nil, nil, rootstorage.TailToken{})
}

// ReplaceRootSnapshot replaces the runtime rooted view from one rooted durable
// snapshot while preserving store-heartbeat observations.
func (c *Cluster) ReplaceRootSnapshot(
	descriptors map[uint64]descriptor.Descriptor,
	pendingPeerChanges map[uint64]rootstate.PendingPeerChange,
	pendingRangeChanges map[uint64]rootstate.PendingRangeChange,
	token rootstorage.TailToken,
) {
	if c == nil {
		return
	}
	c.regions.Replace(descriptors)
	c.rootToken = token
	c.replaceTransitionRuntime(rootstate.Snapshot{
		Descriptors:         rootstate.CloneDescriptors(descriptors),
		PendingPeerChanges:  pendingPeerChanges,
		PendingRangeChanges: pendingRangeChanges,
	})
}

func (c *Cluster) CatalogRootToken() rootstorage.TailToken {
	if c == nil {
		return rootstorage.TailToken{}
	}
	return c.rootToken
}

// TransitionSnapshot returns a stable copy of rooted pending execution state.
func (c *Cluster) TransitionSnapshot() pdview.PendingSnapshot {
	if c == nil {
		return pdview.PendingSnapshot{
			PendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange),
			PendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange),
		}
	}
	return c.transitions.Snapshot()
}

// OperatorSnapshot returns a stable copy of the operator runtime derived from
// rooted transitions.
func (c *Cluster) OperatorSnapshot() pdoperator.RuntimeSnapshot {
	if c == nil {
		return pdoperator.RuntimeSnapshot{}
	}
	return c.operators.Snapshot()
}

// ObserveRootEventLifecycle evaluates one rooted transition event against the
// current rooted runtime snapshot materialized in Coordinator.
func (c *Cluster) ObserveRootEventLifecycle(event rootevent.Event) rootstate.TransitionAssessment {
	if c == nil {
		return rootstate.TransitionAssessment{}
	}
	transitions := c.TransitionSnapshot()
	return rootstate.AssessTransition(rootstate.Snapshot{
		Descriptors:         descriptorsFromRegionInfos(c.RegionSnapshot()),
		PendingPeerChanges:  transitions.PendingPeerChanges,
		PendingRangeChanges: transitions.PendingRangeChanges,
	}, event)
}

// GetRegionDescriptorByKey returns the rooted descriptor containing key
// ([start, end)).
func (c *Cluster) GetRegionDescriptorByKey(key []byte) (descriptor.Descriptor, bool) {
	if c == nil {
		return descriptor.Descriptor{}, false
	}
	desc, ok := c.regions.LookupDescriptor(key)
	if !ok {
		return descriptor.Descriptor{}, false
	}
	return desc, true
}

// RegionLastHeartbeat returns the latest heartbeat timestamp for regionID.
func (c *Cluster) RegionLastHeartbeat(regionID uint64) (time.Time, bool) {
	if c == nil {
		return time.Time{}, false
	}
	return c.regions.LastHeartbeat(regionID)
}

func (c *Cluster) clone() *Cluster {
	if c == nil {
		return NewCluster()
	}
	out := NewCluster()
	for _, store := range c.StoreSnapshot() {
		_ = out.UpsertStoreHeartbeat(store)
	}
	for _, region := range c.RegionSnapshot() {
		_ = out.regions.UpsertAt(region.Descriptor, region.LastHeartbeat)
	}
	transitions := c.TransitionSnapshot()
	out.replaceTransitionRuntime(rootstate.Snapshot{
		Descriptors:         descriptorsFromRegionInfos(c.RegionSnapshot()),
		PendingPeerChanges:  transitions.PendingPeerChanges,
		PendingRangeChanges: transitions.PendingRangeChanges,
	})
	out.rootToken = c.rootToken
	return out
}

func (c *Cluster) applyRootEventToTransitions(event rootevent.Event) {
	if c == nil || c.transitions == nil {
		return
	}
	transitions := c.transitions.Snapshot()
	snapshot := rootstate.Snapshot{
		Descriptors:         descriptorsFromRegionInfos(c.RegionSnapshot()),
		PendingPeerChanges:  transitions.PendingPeerChanges,
		PendingRangeChanges: transitions.PendingRangeChanges,
	}
	rootstate.ApplyEventToSnapshot(&snapshot, snapshot.State.LastCommitted, event)
	c.replaceTransitionRuntime(snapshot)
}

func (c *Cluster) replaceTransitionRuntime(snapshot rootstate.Snapshot) {
	if c == nil {
		return
	}
	c.transitions.Replace(snapshot.PendingPeerChanges, snapshot.PendingRangeChanges)
	if c.operators != nil {
		c.operators.ReplaceRootedTransitions(rootstate.BuildTransitionEntries(snapshot))
	}
}

func descriptorsFromRegionInfos(in []pdview.RegionInfo) map[uint64]descriptor.Descriptor {
	if len(in) == 0 {
		return make(map[uint64]descriptor.Descriptor)
	}
	out := make(map[uint64]descriptor.Descriptor, len(in))
	for _, region := range in {
		desc := region.Descriptor
		if desc.RegionID == 0 {
			continue
		}
		out[desc.RegionID] = desc.Clone()
	}
	return out
}
