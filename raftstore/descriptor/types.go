package descriptor

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/localmeta"
)

// LineageKind classifies how one descriptor references an older topology
// object.
type LineageKind uint8

const (
	LineageKindUnknown LineageKind = iota
	LineageKindSplitParent
	LineageKindMergeSource
)

// LineageRef points at one predecessor descriptor used to derive the current
// descriptor.
type LineageRef struct {
	RegionID uint64
	Epoch    metaregion.Epoch
	Hash     []byte
	Kind     LineageKind
}

// Descriptor is the distributed topology descriptor of one region.
//
// Unlike store-local recovery state, this object carries globally meaningful
// routing and membership information and is intended to be served by region
// quorums, snapshots, and route caches.
type Descriptor struct {
	RegionID  uint64
	StartKey  []byte
	EndKey    []byte
	Epoch     metaregion.Epoch
	Peers     []metaregion.Peer
	State     metaregion.ReplicaState
	Lineage   []LineageRef
	RootEpoch uint64
	Hash      []byte
}

// FromRegionMeta lifts store-local region state into the distributed topology
// descriptor shape used by root, views, and routing.
func FromRegionMeta(meta localmeta.RegionMeta, rootEpoch uint64) Descriptor {
	return Descriptor{
		RegionID:  meta.ID,
		StartKey:  append([]byte(nil), meta.StartKey...),
		EndKey:    append([]byte(nil), meta.EndKey...),
		Epoch:     meta.Epoch,
		Peers:     append([]metaregion.Peer(nil), meta.Peers...),
		State:     meta.State,
		RootEpoch: rootEpoch,
	}
}

// Clone returns a detached copy of the descriptor.
func (d Descriptor) Clone() Descriptor {
	cp := d
	if d.StartKey != nil {
		cp.StartKey = append([]byte(nil), d.StartKey...)
	}
	if d.EndKey != nil {
		cp.EndKey = append([]byte(nil), d.EndKey...)
	}
	if len(d.Peers) > 0 {
		cp.Peers = append([]metaregion.Peer(nil), d.Peers...)
	}
	if len(d.Lineage) > 0 {
		cp.Lineage = append([]LineageRef(nil), d.Lineage...)
		for i := range cp.Lineage {
			if cp.Lineage[i].Hash != nil {
				cp.Lineage[i].Hash = append([]byte(nil), cp.Lineage[i].Hash...)
			}
		}
	}
	if d.Hash != nil {
		cp.Hash = append([]byte(nil), d.Hash...)
	}
	return cp
}
