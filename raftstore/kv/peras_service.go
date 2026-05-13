package kv

import (
	"context"
	"errors"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	rsperas "github.com/feichai0017/NoKV/raftstore/peras"
)

func (s *Service) PerasWitnessSegment(ctx context.Context, req *kvrpcpb.PerasWitnessSegmentRequest) (*kvrpcpb.PerasWitnessSegmentResponse, error) {
	if s == nil || s.perasWitness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: peras witness is not configured")
	}
	scope, err := rsperas.ScopeFromProto(req.GetScope())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	record, err := rsperas.SegmentWitnessRecordFromProto(req.GetRecord())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if err := s.perasWitness.AppendSegment(ctx, scope, record); err != nil {
		return nil, rpcPerasWitnessStatus(err)
	}
	return &kvrpcpb.PerasWitnessSegmentResponse{}, nil
}

func (s *Service) PerasWitnessProbe(ctx context.Context, req *kvrpcpb.PerasWitnessProbeRequest) (*kvrpcpb.PerasWitnessProbeResponse, error) {
	if s == nil || s.perasWitness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: peras witness is not configured")
	}
	if req.GetEpochId() == 0 {
		return nil, rpcInvalidArgument("peras witness probe requires epoch_id")
	}
	snapshot, err := s.perasWitness.Probe(ctx, req.GetEpochId())
	if err != nil {
		return nil, rpcPerasWitnessStatus(err)
	}
	return rsperas.SnapshotToProto(snapshot), nil
}

func rpcPerasWitnessStatus(err error) error {
	switch {
	case errors.Is(err, rsperas.ErrWitnessNodeConfigInvalid),
		errors.Is(err, rsperas.ErrWitnessAuthorityMissing),
		errors.Is(err, rsperas.ErrWitnessAuthorityMismatch):
		return rpcProtocolPrecondition(err.Error())
	default:
		return rpcStatus(err)
	}
}
