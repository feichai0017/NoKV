package perasauthority

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestRootAuthorityFeedPollsGrantSnapshotsWhileWatchIsQuiet(t *testing.T) {
	source := &pollingAuthoritySource{}
	table := NewActiveAuthorities()
	ctx := t.Context()
	feed := StartRootAuthorityFeed(ctx, source, table, 5*time.Millisecond)
	require.NotNil(t, feed)
	defer func() { require.NoError(t, feed.Close()) }()

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

func TestRootAuthorityFeedAppliesGrantEvents(t *testing.T) {
	source := &pollingAuthoritySource{}
	table := NewActiveAuthorities()
	feed := &RootAuthorityFeed{source: source, table: table}

	scope := compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{11},
	}
	grant := testGrant("g-event", "holder-event", scope)

	feed.applyRootEvent(rootevent.PerasAuthorityGranted(grant))
	grant, ok, err := table.Find(scope, testNow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "holder-event", grant.HolderID)
}

type pollingAuthoritySource struct {
	mu     sync.Mutex
	grants []AuthorityGrant
}

func (s *pollingAuthoritySource) replace(grants ...AuthorityGrant) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants = append([]AuthorityGrant(nil), grants...)
}

func (s *pollingAuthoritySource) ListPerasAuthorityGrants(context.Context, *coordpb.ListPerasAuthorityGrantsRequest) (*coordpb.ListPerasAuthorityGrantsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*metapb.RootPerasAuthorityGrant, 0, len(s.grants))
	for _, grant := range s.grants {
		out = append(out, wire.RootPerasAuthorityGrantToProto(grant))
	}
	return &coordpb.ListPerasAuthorityGrantsResponse{Grants: out}, nil
}

func (s *pollingAuthoritySource) WatchRootEvents(ctx context.Context, _ *coordpb.WatchRootEventsRequest, _ ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error) {
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
