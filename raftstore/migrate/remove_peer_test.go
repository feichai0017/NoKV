package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"github.com/stretchr/testify/require"
)

func TestRemovePeerWaitsForTargetDrop(t *testing.T) {
	leader := &fakeAdminClient{
		statuses: []*pb.RegionStatusResponse{
			{Known: true, Region: &pb.RegionMeta{Id: 8, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
			{Known: true, Region: &pb.RegionMeta{Id: 8}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*pb.RegionStatusResponse{
			{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1},
			{},
		},
	}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		switch addr {
		case "leader":
			return leader, func() error { return nil }, nil
		case "target":
			return target, func() error { return nil }, nil
		default:
			t.Fatalf("unexpected addr %q", addr)
			return nil, nil, nil
		}
	}

	result, err := RemovePeer(context.Background(), RemovePeerConfig{
		Addr:         "leader",
		TargetAddr:   "target",
		RegionID:     8,
		PeerID:       22,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
	})
	require.NoError(t, err)
	require.False(t, result.TargetHosted)
}
