package perasauth

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestMirrorPollsGrantSnapshotsWhileWatchIsQuiet(t *testing.T) {
	source := &pollingMirrorSource{}
	table := NewActiveAuthorities()
	ctx := t.Context()
	mirror := StartMirror(ctx, source, table, 5*time.Millisecond)
	require.NotNil(t, mirror)
	defer func() { require.NoError(t, mirror.Close()) }()

	scope := compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{7},
	}
	source.replace(testGrant("g1", "holder-a", scope))

	require.Eventually(t, func() bool {
		grant, ok, err := table.Find(scope, testNow)
		return err == nil && ok && grant.HolderID == "holder-a"
	}, time.Second, 10*time.Millisecond)
}

type pollingMirrorSource struct {
	mu     sync.Mutex
	grants []AuthorityGrant
}

func (s *pollingMirrorSource) replace(grants ...AuthorityGrant) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants = append([]AuthorityGrant(nil), grants...)
}

func (s *pollingMirrorSource) ListPerasAuthorityGrants(context.Context, *coordpb.ListPerasAuthorityGrantsRequest) (*coordpb.ListPerasAuthorityGrantsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*metapb.RootPerasAuthorityGrant, 0, len(s.grants))
	for _, grant := range s.grants {
		out = append(out, wire.RootPerasAuthorityGrantToProto(grant))
	}
	return &coordpb.ListPerasAuthorityGrantsResponse{Grants: out}, nil
}

func (s *pollingMirrorSource) WatchRootEvents(ctx context.Context, _ *coordpb.WatchRootEventsRequest, _ ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error) {
	return pollingWatchStream{ctx: ctx}, nil
}

type pollingWatchStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s pollingWatchStream) Recv() (*coordpb.WatchRootEventsResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}
