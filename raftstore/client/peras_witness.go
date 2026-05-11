package client

import (
	"context"
	"errors"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	rsperas "github.com/feichai0017/NoKV/raftstore/peras"
)

var ErrPerasWitnessClientInvalid = errors.New("client: invalid peras witness client")

// RemotePerasWitness adapts one StoreKV connection into the fsmeta Peras
// witness interface. Routing and quorum policy stay in the holder; this object
// is only one durable witness endpoint.
type RemotePerasWitness struct {
	id     string
	client kvrpcpb.StoreKVClient
}

func NewRemotePerasWitness(id string, client kvrpcpb.StoreKVClient) (*RemotePerasWitness, error) {
	if id == "" || client == nil {
		return nil, ErrPerasWitnessClientInvalid
	}
	return &RemotePerasWitness{id: id, client: client}, nil
}

func (w *RemotePerasWitness) ID() string {
	if w == nil {
		return ""
	}
	return w.id
}

func (w *RemotePerasWitness) AppendSegment(ctx context.Context, scope compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	if w == nil || w.client == nil {
		return ErrPerasWitnessClientInvalid
	}
	_, err := w.client.PerasWitnessSegment(ctx, &kvrpcpb.PerasWitnessSegmentRequest{
		Scope:  rsperas.ScopeToProto(scope),
		Record: rsperas.SegmentWitnessRecordToProto(record),
	})
	return normalizeRPCError(err)
}

func (w *RemotePerasWitness) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if w == nil || w.client == nil {
		return fsperas.WitnessSnapshot{}, ErrPerasWitnessClientInvalid
	}
	resp, err := w.client.PerasWitnessProbe(ctx, &kvrpcpb.PerasWitnessProbeRequest{EpochId: epochID})
	if err != nil {
		return fsperas.WitnessSnapshot{}, normalizeRPCError(err)
	}
	return rsperas.SnapshotFromProto(resp)
}
