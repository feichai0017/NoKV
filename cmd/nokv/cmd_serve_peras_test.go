package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	perasauth "github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestStartServePerasWitnessUsesRootAuthorityFeed(t *testing.T) {
	scope := servePerasAuthorityScope()
	source := &servePerasAuthoritySource{
		grants: []perasauth.AuthorityGrant{servePerasAuthorityGrant(scope)},
	}
	opener := &servePerasTestWALOpener{dir: t.TempDir()}
	defer opener.close(t)

	witness, authorities, feed, err := startServePerasWitness(t.Context(), 1, source, opener, wal.DurabilityFsync)
	require.NoError(t, err)
	require.NotNil(t, witness)
	require.NotNil(t, authorities)
	require.NotNil(t, feed)
	defer func() { require.NoError(t, feed.Close()) }()

	record := servePerasSegmentRecord()
	require.Eventually(t, func() bool {
		return witness.AppendSegment(t.Context(), scope, record) == nil
	}, time.Second, 10*time.Millisecond)
}

type servePerasTestWALOpener struct {
	dir      string
	managers []*wal.Manager
}

func (o *servePerasTestWALOpener) OpenControlWAL(groupID uint64) (*wal.Manager, error) {
	manager, err := wal.Open(wal.Config{Dir: filepath.Join(o.dir, "wal", fmt.Sprintf("group-%d", groupID))})
	if err != nil {
		return nil, err
	}
	o.managers = append(o.managers, manager)
	return manager, nil
}

func (o *servePerasTestWALOpener) close(t *testing.T) {
	t.Helper()
	for _, manager := range o.managers {
		require.NoError(t, manager.Close())
	}
}

type servePerasAuthoritySource struct {
	grants []perasauth.AuthorityGrant
}

func (s *servePerasAuthoritySource) ListPerasAuthorityGrants(context.Context, *coordpb.ListPerasAuthorityGrantsRequest) (*coordpb.ListPerasAuthorityGrantsResponse, error) {
	out := make([]*metapb.RootPerasAuthorityGrant, 0, len(s.grants))
	for _, grant := range s.grants {
		out = append(out, metawire.RootPerasAuthorityGrantToProto(grant))
	}
	return &coordpb.ListPerasAuthorityGrantsResponse{Grants: out}, nil
}

func (s *servePerasAuthoritySource) WatchRootEvents(ctx context.Context, _ *coordpb.WatchRootEventsRequest, _ ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error) {
	return servePerasWatchStream{ctx: ctx}, nil
}

type servePerasWatchStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s servePerasWatchStream) Recv() (*coordpb.WatchRootEventsResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func servePerasAuthorityScope() compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      fsmeta.MountID("vol"),
		MountKeyID: fsmeta.MountKeyID(7),
		Buckets:    []fsmeta.AffinityBucket{3},
		Parents:    []fsmeta.InodeID{11},
		Inodes:     []fsmeta.InodeID{29},
	}
}

func servePerasAuthorityGrant(scope compile.AuthorityScope) perasauth.AuthorityGrant {
	return perasauth.AuthorityGrant{
		GrantID:         "grant-1",
		EpochID:         1,
		HolderID:        "holder-a",
		Scope:           perasauth.AuthorityScopeFromDelta(scope),
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
	}
}

func servePerasSegmentRecord() fsperas.SegmentWitnessRecord {
	var root [32]byte
	root[0] = 1
	var digest [32]byte
	digest[0] = 2
	return fsperas.SegmentWitnessRecord{
		EpochID:              1,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   4096,
		SegmentPointer:       "inline",
		OperationCount:       64,
		EntryCount:           128,
		TimestampUnixNano:    time.Now().UnixNano(),
		HolderID:             "holder-a",
	}
}
