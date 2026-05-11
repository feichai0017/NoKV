package client

import (
	"context"
	"net"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	fscapsule "github.com/feichai0017/NoKV/fsmeta/exec/capsule"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type remoteCapsuleWitnessStub struct {
	prepare fscapsule.PrepareRecord
	commit  fscapsule.CommitCertificateRecord
	scope   compile.AuthorityScope
}

func (s *remoteCapsuleWitnessStub) AppendPrepare(_ context.Context, scope compile.AuthorityScope, record fscapsule.PrepareRecord) error {
	s.scope = scope
	s.prepare = record
	return nil
}

func (s *remoteCapsuleWitnessStub) AppendCommitCertificate(_ context.Context, _ compile.AuthorityScope, record fscapsule.CommitCertificateRecord) error {
	s.commit = record
	return nil
}

func (s *remoteCapsuleWitnessStub) Probe(_ context.Context, _ uint64) (fscapsule.WitnessSnapshot, error) {
	return fscapsule.WitnessSnapshot{
		Prepares: []fscapsule.PrepareRecord{s.prepare},
		Commits:  []fscapsule.CommitCertificateRecord{s.commit},
	}, nil
}

func TestRemoteCapsuleWitnessRoundTrip(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: 8,
		Buckets:    []fsmeta.AffinityBucket{2},
		Parents:    []fsmeta.InodeID{100},
		Inodes:     []fsmeta.InodeID{200},
	}
	prepare := remoteCapsulePrepareRecord(t, scope)
	prepareDigest, err := fscapsule.PrepareDigest(prepare)
	require.NoError(t, err)
	commit := fscapsule.CommitCertificateRecord{
		EpochID:           prepare.EpochID,
		OpID:              prepare.OpID,
		PrepareDigest:     prepareDigest,
		QuorumAckSet:      []string{"store-1", "store-2"},
		TimestampUnixNano: 200,
		HolderID:          prepare.HolderID,
	}

	stub := &remoteCapsuleWitnessStub{}
	conn, closeServer := startCapsuleWitnessServer(t, stub)
	defer closeServer()
	remote, err := NewRemoteCapsuleWitness("store-1", kvrpcpb.NewStoreKVClient(conn))
	require.NoError(t, err)

	require.NoError(t, remote.AppendPrepare(context.Background(), scope, prepare))
	require.Equal(t, scope, stub.scope)
	require.Equal(t, prepare, stub.prepare)

	require.NoError(t, remote.AppendCommitCertificate(context.Background(), scope, commit))
	require.Equal(t, commit, stub.commit)

	snapshot, err := remote.Probe(context.Background(), prepare.EpochID)
	require.NoError(t, err)
	require.Equal(t, []fscapsule.PrepareRecord{prepare}, snapshot.Prepares)
	require.Equal(t, []fscapsule.CommitCertificateRecord{commit}, snapshot.Commits)
}

func BenchmarkRemoteCapsuleWitnessAppendPrepare(b *testing.B) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: 8,
		Buckets:    []fsmeta.AffinityBucket{2},
		Parents:    []fsmeta.InodeID{100},
		Inodes:     []fsmeta.InodeID{200},
	}
	prepare := remoteCapsulePrepareRecord(b, scope)
	stub := &remoteCapsuleWitnessStub{}
	conn, closeServer := startCapsuleWitnessServer(b, stub)
	defer closeServer()
	remote, err := NewRemoteCapsuleWitness("store-1", kvrpcpb.NewStoreKVClient(conn))
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, remote.AppendPrepare(context.Background(), scope, prepare))
	}
}

func TestRemoteCapsuleWitnessRequiresClient(t *testing.T) {
	_, err := NewRemoteCapsuleWitness("store-1", nil)
	require.ErrorIs(t, err, ErrCapsuleWitnessClientInvalid)
}

func startCapsuleWitnessServer(tb testing.TB, witness kv.CapsuleWitness) (*grpc.ClientConn, func()) {
	tb.Helper()
	srv := grpc.NewServer()
	kvrpcpb.RegisterStoreKVServer(srv, kv.NewService(nil, kv.WithCapsuleWitness(witness)))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	go func() {
		_ = srv.Serve(lis)
	}()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(tb, err)
	return conn, func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}
}

func remoteCapsulePrepareRecord(tb testing.TB, scope compile.AuthorityScope) fscapsule.PrepareRecord {
	tb.Helper()
	payload, err := fscapsule.EncodeSemanticDeltaPayload(compile.SemanticDelta{
		Kind:        fsmeta.OperationCreate,
		Authority:   scope,
		Eligibility: compile.EligibilityFastPath,
	})
	require.NoError(tb, err)
	digest, err := fscapsule.SemanticDeltaPayloadDigest(payload)
	require.NoError(tb, err)
	record := fscapsule.PrepareRecord{
		EpochID:           5,
		OpID:              fscapsule.OperationID{ClientID: "client", Seq: 6},
		DeltaPayload:      payload,
		DeltaDigest:       digest,
		TimestampUnixNano: 100,
		HolderID:          "holder",
	}
	for i := range record.PredicateDigest {
		record.PredicateDigest[i] = 1
	}
	for i := range record.AuthorityProofDigest {
		record.AuthorityProofDigest[i] = 2
	}
	for i := range record.HolderSignature {
		record.HolderSignature[i] = 3
	}
	_, err = fscapsule.EncodePrepareRecord(record)
	require.NoError(tb, err)
	return record
}
