package kv

import (
	"context"
	"errors"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	rsperas "github.com/feichai0017/NoKV/raftstore/peras"
)

func (s *Service) PerasWitnessPrepare(ctx context.Context, req *kvrpcpb.PerasWitnessPrepareRequest) (*kvrpcpb.PerasWitnessPrepareResponse, error) {
	if s == nil || s.perasWitness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: peras witness is not configured")
	}
	scope, err := rsperas.ScopeFromProto(req.GetScope())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	record, err := rsperas.PrepareRecordFromProto(req.GetRecord())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if err := s.perasWitness.AppendPrepare(ctx, scope, record); err != nil {
		return nil, rpcPerasWitnessStatus(err)
	}
	return &kvrpcpb.PerasWitnessPrepareResponse{}, nil
}

func (s *Service) PerasWitnessCommit(ctx context.Context, req *kvrpcpb.PerasWitnessCommitRequest) (*kvrpcpb.PerasWitnessCommitResponse, error) {
	if s == nil || s.perasWitness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: peras witness is not configured")
	}
	scope, err := rsperas.ScopeFromProto(req.GetScope())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	record, err := rsperas.CommitCertificateRecordFromProto(req.GetRecord())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if err := s.perasWitness.AppendCommitCertificate(ctx, scope, record); err != nil {
		return nil, rpcPerasWitnessStatus(err)
	}
	return &kvrpcpb.PerasWitnessCommitResponse{}, nil
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
		errors.Is(err, rsperas.ErrWitnessAuthorityMismatch),
		errors.Is(err, rsperas.ErrWitnessDuplicateRecord),
		errors.Is(err, rsperas.ErrWitnessPrepareMissing),
		errors.Is(err, rsperas.ErrWitnessPrepareMismatch):
		return rpcProtocolPrecondition(err.Error())
	default:
		return rpcStatus(err)
	}
}
