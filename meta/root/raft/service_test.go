package rootraft

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	rootpkg "github.com/feichai0017/NoKV/meta/root"
	"github.com/stretchr/testify/require"
)

func reserveAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func waitForServiceLeader(t *testing.T, services []*Service) *Service {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, svc := range services {
			if svc.node.IsLeader() {
				return svc
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("meta/root/raft: service leader election did not converge")
	return nil
}

func TestGRPCServiceReplicatesAcrossCluster(t *testing.T) {
	base := t.TempDir()
	peers := []Peer{
		{ID: 1, Address: reserveAddr(t)},
		{ID: 2, Address: reserveAddr(t)},
		{ID: 3, Address: reserveAddr(t)},
	}

	services := make([]*Service, 0, len(peers))
	for _, peer := range peers {
		svc, err := OpenService(Config{
			NodeID:        peer.ID,
			Peers:         peers,
			Bootstrap:     true,
			WorkDir:       filepath.Join(base, fmt.Sprintf("n%d", peer.ID)),
			TickInterval:  20 * time.Millisecond,
			SnapshotEvery: 4,
		})
		require.NoError(t, err)
		services = append(services, svc)
	}
	for _, svc := range services {
		defer func(svc *Service) {
			require.NoError(t, svc.Close())
		}(svc)
	}

	leader := waitForServiceLeader(t, services)
	client, err := Dial(context.Background(), localPeerAddress(leader.cfg))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Close())
	}()

	_, err = client.Append(rootpkg.RegionDescriptorPublished(testDescriptor(71, "a", "z")))
	require.NoError(t, err)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, svc := range services {
			state, err := svc.currentState()
			require.NoError(t, err)
			if state.ClusterEpoch != 1 {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("meta/root/raft: grpc service cluster did not replicate descriptor")
}
