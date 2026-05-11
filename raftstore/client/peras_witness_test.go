package client

import (
	"context"
	"net"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type remotePerasWitnessStub struct {
	prepare fsperas.PrepareRecord
	commit  fsperas.CommitCertificateRecord
	scope   compile.AuthorityScope
}

func (s *remotePerasWitnessStub) AppendPrepare(_ context.Context, scope compile.AuthorityScope, record fsperas.PrepareRecord) error {
	s.scope = scope
	s.prepare = record
	return nil
}

func (s *remotePerasWitnessStub) AppendCommitCertificate(_ context.Context, _ compile.AuthorityScope, record fsperas.CommitCertificateRecord) error {
	s.commit = record
	return nil
}

func (s *remotePerasWitnessStub) Probe(_ context.Context, _ uint64) (fsperas.WitnessSnapshot, error) {
	return fsperas.WitnessSnapshot{
		Prepares: []fsperas.PrepareRecord{s.prepare},
		Commits:  []fsperas.CommitCertificateRecord{s.commit},
	}, nil
}

func TestRemotePerasWitnessRoundTrip(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: 8,
		Buckets:    []fsmeta.AffinityBucket{2},
		Parents:    []fsmeta.InodeID{100},
		Inodes:     []fsmeta.InodeID{200},
	}
	prepare := remotePerasPrepareRecord(t, scope)
	prepareDigest, err := fsperas.PrepareDigest(prepare)
	require.NoError(t, err)
	commit := fsperas.CommitCertificateRecord{
		EpochID:           prepare.EpochID,
		OpID:              prepare.OpID,
		PrepareDigest:     prepareDigest,
		QuorumAckSet:      []string{"store-1", "store-2"},
		TimestampUnixNano: 200,
		HolderID:          prepare.HolderID,
	}

	stub := &remotePerasWitnessStub{}
	conn, closeServer := startPerasWitnessServer(t, stub)
	defer closeServer()
	remote, err := NewRemotePerasWitness("store-1", kvrpcpb.NewStoreKVClient(conn))
	require.NoError(t, err)

	require.NoError(t, remote.AppendPrepare(context.Background(), scope, prepare))
	require.Equal(t, scope, stub.scope)
	require.Equal(t, prepare, stub.prepare)

	require.NoError(t, remote.AppendCommitCertificate(context.Background(), scope, commit))
	require.Equal(t, commit, stub.commit)

	snapshot, err := remote.Probe(context.Background(), prepare.EpochID)
	require.NoError(t, err)
	require.Equal(t, []fsperas.PrepareRecord{prepare}, snapshot.Prepares)
	require.Equal(t, []fsperas.CommitCertificateRecord{commit}, snapshot.Commits)
}

func BenchmarkRemotePerasWitnessAppendPrepare(b *testing.B) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: 8,
		Buckets:    []fsmeta.AffinityBucket{2},
		Parents:    []fsmeta.InodeID{100},
		Inodes:     []fsmeta.InodeID{200},
	}
	prepare := remotePerasPrepareRecord(b, scope)
	stub := &remotePerasWitnessStub{}
	conn, closeServer := startPerasWitnessServer(b, stub)
	defer closeServer()
	remote, err := NewRemotePerasWitness("store-1", kvrpcpb.NewStoreKVClient(conn))
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, remote.AppendPrepare(context.Background(), scope, prepare))
	}
}

func TestRemotePerasWitnessRequiresClient(t *testing.T) {
	_, err := NewRemotePerasWitness("store-1", nil)
	require.ErrorIs(t, err, ErrPerasWitnessClientInvalid)
}

func startPerasWitnessServer(tb testing.TB, witness kv.PerasWitness) (*grpc.ClientConn, func()) {
	tb.Helper()
	srv := grpc.NewServer()
	kvrpcpb.RegisterStoreKVServer(srv, kv.NewService(nil, kv.WithPerasWitness(witness)))
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

func remotePerasPrepareRecord(tb testing.TB, scope compile.AuthorityScope) fsperas.PrepareRecord {
	tb.Helper()
	payload, err := fsperas.EncodeSemanticDeltaPayload(compile.SemanticDelta{
		Kind:        fsmeta.OperationCreate,
		Authority:   scope,
		Eligibility: compile.EligibilityFastPath,
	})
	require.NoError(tb, err)
	digest, err := fsperas.SemanticDeltaPayloadDigest(payload)
	require.NoError(tb, err)
	record := fsperas.PrepareRecord{
		EpochID:           5,
		OpID:              fsperas.OperationID{ClientID: "client", Seq: 6},
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
	_, err = fsperas.EncodePrepareRecord(record)
	require.NoError(tb, err)
	return record
}
