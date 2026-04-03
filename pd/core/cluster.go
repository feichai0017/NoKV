package core

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	pdview "github.com/feichai0017/NoKV/pd/view"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"time"
)

// StoreStats captures store-level heartbeat data tracked by PD.
type StoreStats = pdview.StoreStats

// RegionInfo captures region metadata with heartbeat timestamp.
type RegionInfo = pdview.RegionInfo

// Cluster stores in-memory PD metadata and provides route lookups.
//
// NOTE: Cluster intentionally keeps only the in-memory metadata/state model.
// PD RPC wiring and persistence are handled by higher layers (pd/server and
// pd/storage).
type Cluster struct {
	stores  *pdview.StoreHealthView
	regions *pdview.RegionDirectoryView
}

// NewCluster creates an empty in-memory cluster metadata view.
func NewCluster() *Cluster {
	return &Cluster{
		stores:  pdview.NewStoreHealthView(),
		regions: pdview.NewRegionDirectoryView(),
	}
}

// UpsertStoreHeartbeat updates store metadata from a store heartbeat.
func (c *Cluster) UpsertStoreHeartbeat(stats StoreStats) error {
	if c == nil {
		return nil
	}
	return c.stores.Upsert(stats)
}

// RemoveStore removes a store from PD metadata.
func (c *Cluster) RemoveStore(storeID uint64) {
	if c == nil {
		return
	}
	c.stores.Remove(storeID)
}

// StoreSnapshot returns a stable copy of tracked store metadata.
func (c *Cluster) StoreSnapshot() []StoreStats {
	if c == nil {
		return nil
	}
	return c.stores.Snapshot()
}

// PublishRegionDescriptor applies one rooted region descriptor into the runtime
// PD route view.
func (c *Cluster) PublishRegionDescriptor(desc descriptor.Descriptor) error {
	if c == nil {
		return nil
	}
	return c.regions.Upsert(desc)
}

// PublishRootEvent applies one explicit rooted truth event into the runtime PD
// route view.
func (c *Cluster) PublishRootEvent(event rootevent.Event) error {
	if c == nil {
		return nil
	}
	switch {
	case event.RegionDescriptor != nil:
		return c.PublishRegionDescriptor(event.RegionDescriptor.Descriptor)
	case event.RegionRemoval != nil:
		c.RemoveRegion(event.RegionRemoval.RegionID)
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

// RemoveRegion removes a region from PD metadata and reports whether the region existed before removal.
func (c *Cluster) RemoveRegion(regionID uint64) bool {
	if c == nil {
		return false
	}
	return c.regions.Remove(regionID)
}

// RegionSnapshot returns a stable copy of tracked region metadata.
func (c *Cluster) RegionSnapshot() []RegionInfo {
	if c == nil {
		return nil
	}
	return c.regions.Snapshot()
}

// ReplaceRegionSnapshot replaces the region directory view from one rooted
// snapshot while preserving store-health runtime observations.
func (c *Cluster) ReplaceRegionSnapshot(descriptors map[uint64]descriptor.Descriptor) {
	if c == nil {
		return
	}
	c.regions.Replace(descriptors)
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
