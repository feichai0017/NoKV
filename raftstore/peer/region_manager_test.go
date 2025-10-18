package peer_test

import (
    "testing"

    "github.com/feichai0017/NoKV/manifest"
)

type regionManagerStub struct{
    meta map[uint64]manifest.RegionMeta
}

func newRegionManagerStub() *regionManagerStub {
    return &regionManagerStub{meta: make(map[uint64]manifest.RegionMeta)}
}

func (s *regionManagerStub) add(meta manifest.RegionMeta) {
    s.meta[meta.ID] = meta
}

func (s *regionManagerStub) get(id uint64) (manifest.RegionMeta, bool) {
    meta, ok := s.meta[id]
    return meta, ok
}

func TestRegionStub(t *testing.T) {
    mgr := newRegionManagerStub()
    meta := manifest.RegionMeta{ID: 1}
    mgr.add(meta)
    if _, ok := mgr.get(1); !ok {
        t.Fatalf("expected meta present")
    }
}
