package raftstore

import (
	"context"

	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
)

type raftstoreSegmentCatalogScanner struct {
	runner *Runner
}

func (s raftstoreSegmentCatalogScanner) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]runtimeperas.KV, error) {
	if s.runner == nil {
		return nil, runtimeperas.ErrRuntimeInvalid
	}
	rows, err := s.runner.Scan(ctx, startKey, limit, version)
	if err != nil {
		return nil, err
	}
	out := make([]runtimeperas.KV, 0, len(rows))
	for _, row := range rows {
		out = append(out, runtimeperas.KV{
			Key:   runtimeCloneBytes(row.Key),
			Value: runtimeCloneBytes(row.Value),
		})
	}
	return out, nil
}
