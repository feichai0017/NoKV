package localmeta

import (
	"fmt"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// DescriptorToProto lifts one store-local region shape into the distributed
// descriptor wire shape.
func DescriptorToProto(meta RegionMeta) *metapb.RegionDescriptor {
	return metawire.DescriptorToProto(Descriptor(meta, 0))
}

// Descriptor lifts one store-local region shape into the distributed descriptor
// domain object.
func Descriptor(meta RegionMeta, rootEpoch uint64) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID:  meta.ID,
		StartKey:  append([]byte(nil), meta.StartKey...),
		EndKey:    append([]byte(nil), meta.EndKey...),
		Epoch:     meta.Epoch,
		Peers:     append([]metaregion.Peer(nil), meta.Peers...),
		State:     meta.State,
		RootEpoch: rootEpoch,
	}
	desc.EnsureHash()
	return desc
}

// FromDescriptorProto lowers one distributed descriptor wire shape into the
// store-local runtime shape needed by snapshot install and raft admin paths.
func FromDescriptorProto(pbDesc *metapb.RegionDescriptor) (RegionMeta, error) {
	if pbDesc == nil {
		return RegionMeta{}, fmt.Errorf("region descriptor is nil")
	}
	return FromDescriptor(metawire.DescriptorFromProto(pbDesc)), nil
}

// FromDescriptor lowers one distributed descriptor into the store-local runtime
// shape.
func FromDescriptor(desc descriptor.Descriptor) RegionMeta {
	return RegionMeta{
		ID:       desc.RegionID,
		StartKey: append([]byte(nil), desc.StartKey...),
		EndKey:   append([]byte(nil), desc.EndKey...),
		Epoch:    desc.Epoch,
		Peers:    append([]metaregion.Peer(nil), desc.Peers...),
		State:    desc.State,
	}
}
