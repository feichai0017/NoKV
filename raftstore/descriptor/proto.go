package descriptor

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

// ToProto lifts one runtime descriptor into the shared metadata protobuf shape.
func (d Descriptor) ToProto() *metapb.RegionDescriptor {
	pbDesc := &metapb.RegionDescriptor{
		RegionId:  d.RegionID,
		StartKey:  append([]byte(nil), d.StartKey...),
		EndKey:    append([]byte(nil), d.EndKey...),
		State:     metapb.RegionReplicaState(d.State),
		RootEpoch: d.RootEpoch,
		Hash:      append([]byte(nil), d.Hash...),
	}
	pbDesc.Epoch = &metapb.RegionEpoch{
		Version:     d.Epoch.Version,
		ConfVersion: d.Epoch.ConfVersion,
	}
	if len(d.Peers) > 0 {
		pbDesc.Peers = make([]*metapb.RegionPeer, 0, len(d.Peers))
		for _, peer := range d.Peers {
			pbDesc.Peers = append(pbDesc.Peers, &metapb.RegionPeer{StoreId: peer.StoreID, PeerId: peer.PeerID})
		}
	}
	if len(d.Lineage) > 0 {
		pbDesc.Lineage = make([]*metapb.DescriptorLineageRef, 0, len(d.Lineage))
		for _, ref := range d.Lineage {
			pbRef := &metapb.DescriptorLineageRef{
				RegionId: ref.RegionID,
				Hash:     append([]byte(nil), ref.Hash...),
				Kind:     metapb.DescriptorLineageKind(ref.Kind),
				Epoch: &metapb.RegionEpoch{
					Version:     ref.Epoch.Version,
					ConfVersion: ref.Epoch.ConfVersion,
				},
			}
			pbDesc.Lineage = append(pbDesc.Lineage, pbRef)
		}
	}
	return pbDesc
}

// FromProto converts the shared metadata protobuf shape into one runtime descriptor.
func FromProto(pbDesc *metapb.RegionDescriptor) Descriptor {
	if pbDesc == nil {
		return Descriptor{}
	}
	out := Descriptor{
		RegionID:  pbDesc.RegionId,
		StartKey:  append([]byte(nil), pbDesc.StartKey...),
		EndKey:    append([]byte(nil), pbDesc.EndKey...),
		State:     metaregion.ReplicaState(pbDesc.State),
		RootEpoch: pbDesc.RootEpoch,
		Hash:      append([]byte(nil), pbDesc.Hash...),
	}
	if pbDesc.Epoch != nil {
		out.Epoch.Version = pbDesc.Epoch.Version
		out.Epoch.ConfVersion = pbDesc.Epoch.ConfVersion
	}
	if len(pbDesc.Peers) > 0 {
		out.Peers = make([]metaregion.Peer, 0, len(pbDesc.Peers))
		for _, peer := range pbDesc.Peers {
			if peer == nil {
				continue
			}
			out.Peers = append(out.Peers, metaregion.Peer{StoreID: peer.StoreId, PeerID: peer.PeerId})
		}
	}
	if len(pbDesc.Lineage) > 0 {
		out.Lineage = make([]LineageRef, 0, len(pbDesc.Lineage))
		for _, ref := range pbDesc.Lineage {
			if ref == nil {
				continue
			}
			lineage := LineageRef{
				RegionID: ref.RegionId,
				Hash:     append([]byte(nil), ref.Hash...),
				Kind:     LineageKind(ref.Kind),
			}
			if ref.Epoch != nil {
				lineage.Epoch.Version = ref.Epoch.Version
				lineage.Epoch.ConfVersion = ref.Epoch.ConfVersion
			}
			out.Lineage = append(out.Lineage, lineage)
		}
	}
	return out
}
