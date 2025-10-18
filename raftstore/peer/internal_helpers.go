package peer

import "github.com/feichai0017/NoKV/manifest"

func cloneRegionMeta(meta *manifest.RegionMeta) *manifest.RegionMeta {
    if meta == nil {
        return nil
    }
    cp := *meta
    cp.StartKey = append([]byte(nil), meta.StartKey...)
    cp.EndKey = append([]byte(nil), meta.EndKey...)
    cp.Peers = append([]manifest.PeerMeta(nil), meta.Peers...)
    return &cp
}
