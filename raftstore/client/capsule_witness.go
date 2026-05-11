package client

import (
	"context"
	"errors"

	fscapsule "github.com/feichai0017/NoKV/fsmeta/exec/capsule"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	rscapsule "github.com/feichai0017/NoKV/raftstore/capsule"
)

var ErrCapsuleWitnessClientInvalid = errors.New("client: invalid capsule witness client")

// RemoteCapsuleWitness adapts one StoreKV connection into the fsmeta Capsule
// witness interface. Routing and quorum policy stay in the holder; this object
// is only one durable witness endpoint.
type RemoteCapsuleWitness struct {
	id     string
	client kvrpcpb.StoreKVClient
}

func NewRemoteCapsuleWitness(id string, client kvrpcpb.StoreKVClient) (*RemoteCapsuleWitness, error) {
	if id == "" || client == nil {
		return nil, ErrCapsuleWitnessClientInvalid
	}
	return &RemoteCapsuleWitness{id: id, client: client}, nil
}

func (w *RemoteCapsuleWitness) ID() string {
	if w == nil {
		return ""
	}
	return w.id
}

func (w *RemoteCapsuleWitness) AppendPrepare(ctx context.Context, scope compile.AuthorityScope, record fscapsule.PrepareRecord) error {
	if w == nil || w.client == nil {
		return ErrCapsuleWitnessClientInvalid
	}
	_, err := w.client.CapsuleWitnessPrepare(ctx, &kvrpcpb.CapsuleWitnessPrepareRequest{
		Scope:  rscapsule.ScopeToProto(scope),
		Record: rscapsule.PrepareRecordToProto(record),
	})
	return normalizeRPCError(err)
}

func (w *RemoteCapsuleWitness) AppendCommitCertificate(ctx context.Context, scope compile.AuthorityScope, record fscapsule.CommitCertificateRecord) error {
	if w == nil || w.client == nil {
		return ErrCapsuleWitnessClientInvalid
	}
	_, err := w.client.CapsuleWitnessCommit(ctx, &kvrpcpb.CapsuleWitnessCommitRequest{
		Scope:  rscapsule.ScopeToProto(scope),
		Record: rscapsule.CommitCertificateRecordToProto(record),
	})
	return normalizeRPCError(err)
}

func (w *RemoteCapsuleWitness) Probe(ctx context.Context, epochID uint64) (fscapsule.WitnessSnapshot, error) {
	if w == nil || w.client == nil {
		return fscapsule.WitnessSnapshot{}, ErrCapsuleWitnessClientInvalid
	}
	resp, err := w.client.CapsuleWitnessProbe(ctx, &kvrpcpb.CapsuleWitnessProbeRequest{EpochId: epochID})
	if err != nil {
		return fscapsule.WitnessSnapshot{}, normalizeRPCError(err)
	}
	return rscapsule.SnapshotFromProto(resp)
}
