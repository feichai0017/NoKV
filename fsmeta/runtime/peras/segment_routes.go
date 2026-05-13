package peras

import fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"

func SegmentInstallRoutingKeys(segment fsperas.PerasSegment, materialize bool) ([][]byte, error) {
	if materialize {
		key, err := segment.FirstKey()
		if err != nil {
			return nil, err
		}
		return [][]byte{key}, nil
	}
	return fsperas.PerasSegmentCatalogObjectKeys(segment)
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
