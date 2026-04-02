package descriptor

import (
	"github.com/feichai0017/NoKV/raftstore/meta"
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
	Epoch    meta.RegionEpoch
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
	Epoch     meta.RegionEpoch
	Peers     []meta.PeerMeta
	State     meta.RegionState
	Lineage   []LineageRef
	RootEpoch uint64
	Hash      []byte
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
		cp.Peers = append([]meta.PeerMeta(nil), d.Peers...)
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
