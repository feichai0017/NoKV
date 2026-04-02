package codec

import (
	"fmt"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/feichai0017/NoKV/raftstore/localmeta"
)

// LocalRegionMetaToDescriptorProto lifts one store-local region shape into the
// distributed descriptor wire shape. Keep this at admin/raft compatibility
// boundaries rather than reintroducing Descriptor -> RegionMeta methods.
func LocalRegionMetaToDescriptorProto(meta localmeta.RegionMeta) *metapb.RegionDescriptor {
	return DescriptorToProto(descriptor.FromRegionMeta(meta, 0))
}

// LocalRegionMetaFromDescriptorProto lowers one distributed descriptor wire
// shape into the store-local runtime shape needed by snapshot install and raft
// admin paths.
func LocalRegionMetaFromDescriptorProto(pbDesc *metapb.RegionDescriptor) (localmeta.RegionMeta, error) {
	if pbDesc == nil {
		return localmeta.RegionMeta{}, fmt.Errorf("region descriptor is nil")
	}
	desc := DescriptorFromProto(pbDesc)
	return LocalRegionMetaFromDescriptor(desc), nil
}

// LocalRegionMetaFromDescriptor lowers one distributed descriptor into the
// store-local runtime shape. Use this only at boundaries that must feed local
// recovery/runtime code.
func LocalRegionMetaFromDescriptor(desc descriptor.Descriptor) localmeta.RegionMeta {
	return localmeta.RegionMeta{
		ID:       desc.RegionID,
		StartKey: append([]byte(nil), desc.StartKey...),
		EndKey:   append([]byte(nil), desc.EndKey...),
		Epoch:    desc.Epoch,
		Peers:    append([]metaregion.Peer(nil), desc.Peers...),
		State:    desc.State,
	}
}
