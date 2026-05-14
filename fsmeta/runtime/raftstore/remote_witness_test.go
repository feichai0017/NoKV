// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"net"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type remotePerasWitnessStub struct {
	segment fsperas.SegmentWitnessRecord
	scope   compile.AuthorityScope
}

func (s *remotePerasWitnessStub) AppendSegment(_ context.Context, scope compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	s.scope = scope
	s.segment = record
	return nil
}

func (s *remotePerasWitnessStub) Probe(_ context.Context, _ uint64) (fsperas.WitnessSnapshot, error) {
	return fsperas.WitnessSnapshot{Segments: []fsperas.SegmentWitnessRecord{s.segment}}, nil
}

func TestRemotePerasWitnessRoundTrip(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: 8,
		Buckets:    []fsmeta.AffinityBucket{2},
		Parents:    []fsmeta.InodeID{100},
		Inodes:     []fsmeta.InodeID{200},
	}
	record := remotePerasSegmentRecord()

	stub := &remotePerasWitnessStub{}
	conn, closeServer := startPerasWitnessServer(t, stub)
	defer closeServer()
	remote, err := newRemotePerasWitness("store-1", kvrpcpb.NewStoreKVClient(conn))
	require.NoError(t, err)

	require.NoError(t, remote.AppendSegment(context.Background(), scope, record))
	require.Equal(t, scope, stub.scope)
	require.Equal(t, record, stub.segment)

	snapshot, err := remote.Probe(context.Background(), record.EpochID)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{record}, snapshot.Segments)
}

func BenchmarkRemotePerasWitnessAppendSegment(b *testing.B) {
	scope := compile.AuthorityScope{
		Mount:      fsmeta.MountID("m1"),
		MountKeyID: 8,
		Buckets:    []fsmeta.AffinityBucket{2},
		Parents:    []fsmeta.InodeID{100},
		Inodes:     []fsmeta.InodeID{200},
	}
	record := remotePerasSegmentRecord()
	stub := &remotePerasWitnessStub{}
	conn, closeServer := startPerasWitnessServer(b, stub)
	defer closeServer()
	remote, err := newRemotePerasWitness("store-1", kvrpcpb.NewStoreKVClient(conn))
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, remote.AppendSegment(context.Background(), scope, record))
	}
}

func TestRemotePerasWitnessRequiresClient(t *testing.T) {
	_, err := newRemotePerasWitness("store-1", nil)
	require.ErrorIs(t, err, runtimeperas.ErrRuntimeInvalid)
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

func remotePerasSegmentRecord() fsperas.SegmentWitnessRecord {
	var root [32]byte
	root[0] = 5
	var digest [32]byte
	digest[0] = 6
	return fsperas.SegmentWitnessRecord{
		EpochID:              5,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   4096,
		SegmentPointer:       "inline",
		OperationCount:       64,
		EntryCount:           128,
		TimestampUnixNano:    100,
		HolderID:             "holder",
	}
}
