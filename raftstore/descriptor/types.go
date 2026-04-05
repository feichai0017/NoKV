package descriptor

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	metaregion "github.com/feichai0017/NoKV/meta/region"
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

// EnsureHash populates the descriptor hash if it is missing.
func (d *Descriptor) EnsureHash() {
	if d == nil || len(d.Hash) != 0 {
		return
	}
	sum := sha256.New()
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, d.RegionID)
	_, _ = sum.Write(buf)
	_, _ = sum.Write(d.StartKey)
	_, _ = sum.Write(d.EndKey)
	binary.LittleEndian.PutUint64(buf, d.Epoch.Version)
	_, _ = sum.Write(buf)
	binary.LittleEndian.PutUint64(buf, d.Epoch.ConfVersion)
	_, _ = sum.Write(buf)
	for _, p := range d.Peers {
		binary.LittleEndian.PutUint64(buf, p.StoreID)
		_, _ = sum.Write(buf)
		binary.LittleEndian.PutUint64(buf, p.PeerID)
		_, _ = sum.Write(buf)
	}
	binary.LittleEndian.PutUint64(buf, uint64(d.State))
	_, _ = sum.Write(buf)
	d.Hash = sum.Sum(nil)
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

// Equal reports whether two rooted descriptors carry the same topology truth.
func (d Descriptor) Equal(other Descriptor) bool {
	if d.RegionID != other.RegionID ||
		d.State != other.State ||
		d.Epoch != other.Epoch ||
		d.RootEpoch != other.RootEpoch ||
		!bytes.Equal(d.StartKey, other.StartKey) ||
		!bytes.Equal(d.EndKey, other.EndKey) ||
		!bytes.Equal(d.Hash, other.Hash) {
		return false
	}
	if len(d.Peers) != len(other.Peers) || len(d.Lineage) != len(other.Lineage) {
		return false
	}
	for i := range d.Peers {
		if d.Peers[i] != other.Peers[i] {
			return false
		}
	}
	for i := range d.Lineage {
		if d.Lineage[i].RegionID != other.Lineage[i].RegionID ||
			d.Lineage[i].Epoch != other.Lineage[i].Epoch ||
			d.Lineage[i].Kind != other.Lineage[i].Kind ||
			!bytes.Equal(d.Lineage[i].Hash, other.Lineage[i].Hash) {
			return false
		}
	}
	return true
}
