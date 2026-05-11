package kv

import (
	"context"
	"errors"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	rscapsule "github.com/feichai0017/NoKV/raftstore/capsule"
)

func (s *Service) CapsuleWitnessPrepare(ctx context.Context, req *kvrpcpb.CapsuleWitnessPrepareRequest) (*kvrpcpb.CapsuleWitnessPrepareResponse, error) {
	if s == nil || s.capsuleWitness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: capsule witness is not configured")
	}
	scope, err := rscapsule.ScopeFromProto(req.GetScope())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	record, err := rscapsule.PrepareRecordFromProto(req.GetRecord())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if err := s.capsuleWitness.AppendPrepare(ctx, scope, record); err != nil {
		return nil, rpcCapsuleWitnessStatus(err)
	}
	return &kvrpcpb.CapsuleWitnessPrepareResponse{}, nil
}

func (s *Service) CapsuleWitnessCommit(ctx context.Context, req *kvrpcpb.CapsuleWitnessCommitRequest) (*kvrpcpb.CapsuleWitnessCommitResponse, error) {
	if s == nil || s.capsuleWitness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: capsule witness is not configured")
	}
	scope, err := rscapsule.ScopeFromProto(req.GetScope())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	record, err := rscapsule.CommitCertificateRecordFromProto(req.GetRecord())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if err := s.capsuleWitness.AppendCommitCertificate(ctx, scope, record); err != nil {
		return nil, rpcCapsuleWitnessStatus(err)
	}
	return &kvrpcpb.CapsuleWitnessCommitResponse{}, nil
}

func (s *Service) CapsuleWitnessProbe(ctx context.Context, req *kvrpcpb.CapsuleWitnessProbeRequest) (*kvrpcpb.CapsuleWitnessProbeResponse, error) {
	if s == nil || s.capsuleWitness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: capsule witness is not configured")
	}
	if req.GetEpochId() == 0 {
		return nil, rpcInvalidArgument("capsule witness probe requires epoch_id")
	}
	snapshot, err := s.capsuleWitness.Probe(ctx, req.GetEpochId())
	if err != nil {
		return nil, rpcCapsuleWitnessStatus(err)
	}
	return rscapsule.SnapshotToProto(snapshot), nil
}

func rpcCapsuleWitnessStatus(err error) error {
	switch {
	case errors.Is(err, rscapsule.ErrWitnessNodeConfigInvalid),
		errors.Is(err, rscapsule.ErrWitnessAuthorityMissing),
		errors.Is(err, rscapsule.ErrWitnessAuthorityMismatch),
		errors.Is(err, rscapsule.ErrWitnessDuplicateRecord),
		errors.Is(err, rscapsule.ErrWitnessPrepareMissing),
		errors.Is(err, rscapsule.ErrWitnessPrepareMismatch):
		return rpcProtocolPrecondition(err.Error())
	default:
		return rpcStatus(err)
	}
}
