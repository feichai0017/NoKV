package catalog

import (
	pdview "github.com/feichai0017/NoKV/coordinator/view"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"sync"
	"time"
)

type PendingSnapshot struct {
	PendingPeerChanges  map[uint64]rootstate.PendingPeerChange
	PendingRangeChanges map[uint64]rootstate.PendingRangeChange
}

// Cluster stores in-memory Coordinator metadata and provides route lookups.
//
// NOTE: Cluster intentionally keeps only the in-memory metadata/state model.
// Coordinator RPC wiring and persistence are handled by higher layers
// (coordinator/server and coordinator/rootview).
type Cluster struct {
	stores              *pdview.StoreHealthView
	regions             *pdview.RegionDirectoryView
	pendingMu           sync.RWMutex
	pendingPeerChanges  map[uint64]rootstate.PendingPeerChange
	pendingRangeChanges map[uint64]rootstate.PendingRangeChange
	rootMu              sync.RWMutex
	rootToken           rootstorage.TailToken
}

// NewCluster creates an empty in-memory cluster metadata view.
func NewCluster() *Cluster {
	return &Cluster{
		stores:              pdview.NewStoreHealthView(),
		regions:             pdview.NewRegionDirectoryView(),
		pendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange),
		pendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange),
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
	return c.regions.Validate(desc)
}

// PublishRootEvent applies one explicit rooted truth event into the runtime Coordinator
// route view.
func (c *Cluster) PublishRootEvent(event rootevent.Event) error {
	if c == nil {
		return nil
	}
	snapshot := c.rootedSnapshot()
	if err := c.validateRootEventAgainstSnapshot(snapshot, event); err != nil {
		return err
	}
	if err := c.applyRootEventToRegions(event); err != nil {
		return err
	}
	c.applyRootEventToTransitions(snapshot, event)
	return nil
}

func (c *Cluster) applyRootEventToRegions(event rootevent.Event) error {
	return applyRootEventToRegionView(c.regions, event)
}

func applyRootEventToRegionView(regions *pdview.RegionDirectoryView, event rootevent.Event) error {
	if regions == nil {
		return nil
	}
	switch {
	case event.RegionDescriptor != nil:
		return regions.Upsert(event.RegionDescriptor.Descriptor)
	case event.RegionRemoval != nil:
		regions.Remove(event.RegionRemoval.RegionID)
		return nil
	case event.PeerChange != nil && (event.Kind == rootevent.KindPeerAdditionCancelled || event.Kind == rootevent.KindPeerRemovalCancelled):
		if event.PeerChange.Base.RegionID == 0 {
			regions.Remove(event.PeerChange.RegionID)
			return nil
		}
		return regions.Upsert(event.PeerChange.Base)
	case event.RangeSplit != nil && event.Kind == rootevent.KindRegionSplitCancelled:
		regions.Remove(event.RangeSplit.Left.RegionID)
		regions.Remove(event.RangeSplit.Right.RegionID)
		if event.RangeSplit.BaseParent.RegionID != 0 {
			return regions.Upsert(event.RangeSplit.BaseParent)
		}
		return nil
	case event.RangeMerge != nil && event.Kind == rootevent.KindRegionMergeCancelled:
		regions.Remove(event.RangeMerge.Merged.RegionID)
		if event.RangeMerge.BaseLeft.RegionID != 0 {
			if err := regions.Upsert(event.RangeMerge.BaseLeft); err != nil {
				return err
			}
		}
		if event.RangeMerge.BaseRight.RegionID != 0 {
			return regions.Upsert(event.RangeMerge.BaseRight)
		}
		return nil
	case event.RangeSplit != nil:
		regions.Remove(event.RangeSplit.ParentRegionID)
		if err := regions.Upsert(event.RangeSplit.Left); err != nil {
			return err
		}
		return regions.Upsert(event.RangeSplit.Right)
	case event.RangeMerge != nil:
		regions.Remove(event.RangeMerge.LeftRegionID)
		regions.Remove(event.RangeMerge.RightRegionID)
		return regions.Upsert(event.RangeMerge.Merged)
	case event.PeerChange != nil:
		return regions.Upsert(event.PeerChange.Region)
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
	return c.validateRootEventAgainstSnapshot(c.rootedSnapshot(), event)
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

// MaxDescriptorRevision returns the highest rooted descriptor publication epoch
// currently reflected in the in-memory region directory.
func (c *Cluster) MaxDescriptorRevision() uint64 {
	if c == nil {
		return 0
	}
	return rootstate.MaxDescriptorRevision(c.regions.DescriptorsSnapshot())
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
	c.rootMu.Lock()
	c.rootToken = token
	c.rootMu.Unlock()
	c.replaceTransitionRuntime(rootstate.Snapshot{
		PendingPeerChanges:  pendingPeerChanges,
		PendingRangeChanges: pendingRangeChanges,
	})
}

func (c *Cluster) CatalogRootToken() rootstorage.TailToken {
	if c == nil {
		return rootstorage.TailToken{}
	}
	c.rootMu.RLock()
	defer c.rootMu.RUnlock()
	return c.rootToken
}

// TransitionSnapshot returns a stable copy of rooted pending execution state.
func (c *Cluster) TransitionSnapshot() PendingSnapshot {
	if c == nil {
		return PendingSnapshot{
			PendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange),
			PendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange),
		}
	}
	c.pendingMu.RLock()
	defer c.pendingMu.RUnlock()
	return PendingSnapshot{
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(c.pendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(c.pendingRangeChanges),
	}
}

// ObserveRootEventLifecycle evaluates one rooted transition event against the
// current rooted runtime snapshot materialized in Coordinator.
func (c *Cluster) ObserveRootEventLifecycle(event rootevent.Event) rootstate.TransitionAssessment {
	if c == nil {
		return rootstate.TransitionAssessment{}
	}
	return rootstate.AssessTransition(c.rootedSnapshot(), event)
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

// PendingRangeChangeForDescriptor reports whether the served descriptor is only
// visible because a rooted split/merge is still in its planned state.
func (c *Cluster) PendingRangeChangeForDescriptor(regionID uint64) (rootstate.PendingRangeChange, bool) {
	if c == nil || regionID == 0 {
		return rootstate.PendingRangeChange{}, false
	}
	c.pendingMu.RLock()
	defer c.pendingMu.RUnlock()
	for _, change := range c.pendingRangeChanges {
		switch change.Kind {
		case rootstate.PendingRangeChangeSplit:
			if change.LeftRegionID == regionID || change.RightRegionID == regionID {
				return change, true
			}
		case rootstate.PendingRangeChangeMerge:
			if change.Merged.RegionID == regionID {
				return change, true
			}
		}
	}
	return rootstate.PendingRangeChange{}, false
}

// RegionLastHeartbeat returns the latest heartbeat timestamp for regionID.
func (c *Cluster) RegionLastHeartbeat(regionID uint64) (time.Time, bool) {
	if c == nil {
		return time.Time{}, false
	}
	return c.regions.LastHeartbeat(regionID)
}

func (c *Cluster) applyRootEventToTransitions(snapshot rootstate.Snapshot, event rootevent.Event) {
	if c == nil {
		return
	}
	rootstate.ApplyEventToSnapshot(&snapshot, snapshot.State.LastCommitted, event)
	c.replaceTransitionRuntime(snapshot)
}

func (c *Cluster) replaceTransitionRuntime(snapshot rootstate.Snapshot) {
	if c == nil {
		return
	}
	c.pendingMu.Lock()
	c.pendingPeerChanges = rootstate.ClonePendingPeerChanges(snapshot.PendingPeerChanges)
	c.pendingRangeChanges = rootstate.ClonePendingRangeChanges(snapshot.PendingRangeChanges)
	c.pendingMu.Unlock()
}

func (c *Cluster) rootedSnapshot() rootstate.Snapshot {
	if c == nil {
		return rootstate.Snapshot{
			Descriptors:         make(map[uint64]descriptor.Descriptor),
			PendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange),
			PendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange),
		}
	}
	return rootstate.Snapshot{
		Descriptors:         c.regions.DescriptorsSnapshot(),
		PendingPeerChanges:  c.clonePendingPeerChanges(),
		PendingRangeChanges: c.clonePendingRangeChanges(),
	}
}

func (c *Cluster) validateRootEventAgainstSnapshot(snapshot rootstate.Snapshot, event rootevent.Event) error {
	if c == nil {
		return nil
	}
	regions := pdview.NewRegionDirectoryView()
	regions.Replace(snapshot.Descriptors)
	return applyRootEventToRegionView(regions, event)
}

func (c *Cluster) clonePendingPeerChanges() map[uint64]rootstate.PendingPeerChange {
	if c == nil {
		return make(map[uint64]rootstate.PendingPeerChange)
	}
	c.pendingMu.RLock()
	defer c.pendingMu.RUnlock()
	return rootstate.ClonePendingPeerChanges(c.pendingPeerChanges)
}

func (c *Cluster) clonePendingRangeChanges() map[uint64]rootstate.PendingRangeChange {
	if c == nil {
		return make(map[uint64]rootstate.PendingRangeChange)
	}
	c.pendingMu.RLock()
	defer c.pendingMu.RUnlock()
	return rootstate.ClonePendingRangeChanges(c.pendingRangeChanges)
}
