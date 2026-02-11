package regionutil

import "github.com/feichai0017/NoKV/manifest"

// PeerIDForStore returns the peer ID for the provided store in a region.
func PeerIDForStore(meta manifest.RegionMeta, storeID uint64) uint64 {
	for _, p := range meta.Peers {
		if p.StoreID == storeID {
			return p.PeerID
		}
	}
	return 0
}
