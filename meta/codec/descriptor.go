package codec

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/pb"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// DescriptorToProto lifts one runtime descriptor into the shared metadata
// protobuf shape.
func DescriptorToProto(d descriptor.Descriptor) *metapb.RegionDescriptor {
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

// DescriptorFromProto converts the shared metadata protobuf shape into one
// runtime descriptor.
func DescriptorFromProto(pbDesc *metapb.RegionDescriptor) descriptor.Descriptor {
	if pbDesc == nil {
		return descriptor.Descriptor{}
	}
	out := descriptor.Descriptor{
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
		out.Lineage = make([]descriptor.LineageRef, 0, len(pbDesc.Lineage))
		for _, ref := range pbDesc.Lineage {
			if ref == nil {
				continue
			}
			lineage := descriptor.LineageRef{
				RegionID: ref.RegionId,
				Hash:     append([]byte(nil), ref.Hash...),
				Kind:     descriptor.LineageKind(ref.Kind),
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

// DescriptorFromLegacyRegionMeta converts the older RegionMeta wire shape into
// a descriptor. Keep this only at compatibility boundaries such as RegionError
// payloads and legacy test scaffolding.
func DescriptorFromLegacyRegionMeta(meta *pb.RegionMeta) descriptor.Descriptor {
	if meta == nil {
		return descriptor.Descriptor{}
	}
	out := descriptor.Descriptor{
		RegionID: meta.GetId(),
		StartKey: append([]byte(nil), meta.GetStartKey()...),
		EndKey:   append([]byte(nil), meta.GetEndKey()...),
		Epoch: metaregion.Epoch{
			Version:     meta.GetEpochVersion(),
			ConfVersion: meta.GetEpochConfVersion(),
		},
		State: metaregion.ReplicaStateRunning,
	}
	if peers := meta.GetPeers(); len(peers) > 0 {
		out.Peers = make([]metaregion.Peer, 0, len(peers))
		for _, peer := range peers {
			if peer == nil {
				continue
			}
			out.Peers = append(out.Peers, metaregion.Peer{
				StoreID: peer.GetStoreId(),
				PeerID:  peer.GetPeerId(),
			})
		}
	}
	out.EnsureHash()
	return out
}
