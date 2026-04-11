package testcluster

import (
	"context"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	"github.com/feichai0017/NoKV/coordinator/tso"
	rootreplicated "github.com/feichai0017/NoKV/meta/root/backend/replicated"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type Cluster struct {
	tb         testing.TB
	Drivers    map[uint64]*rootreplicated.NetworkDriver
	RootStores map[uint64]*coordstorage.RootStore
	Services   map[uint64]*coordserver.Service
}

func OpenReplicated(tb testing.TB) *Cluster {
	return OpenReplicatedWithTickIntervals(tb, nil)
}

func OpenReplicatedWithTickIntervals(tb testing.TB, tickIntervals map[uint64]time.Duration) *Cluster {
	tb.Helper()

	peerAddrs := reservePeerAddrs(tb)
	transports := make(map[uint64]rootreplicated.Transport, 3)
	c := &Cluster{
		tb:         tb,
		Drivers:    make(map[uint64]*rootreplicated.NetworkDriver, 3),
		RootStores: make(map[uint64]*coordstorage.RootStore, 3),
		Services:   make(map[uint64]*coordserver.Service, 3),
	}
	for _, id := range []uint64{1, 2, 3} {
		transport, err := rootreplicated.NewGRPCTransport(id, peerAddrs[id])
		require.NoError(tb, err)
		transports[id] = transport
	}
	for _, transport := range transports {
		transport.SetPeers(peerAddrs)
	}
	for _, id := range []uint64{1, 2, 3} {
		driver, err := rootreplicated.NewNetworkDriver(rootreplicated.NetworkConfig{
			ID:           id,
			WorkDir:      filepath.Join(tb.TempDir(), "root", "node-"+strconv.FormatUint(id, 10)),
			PeerIDs:      []uint64{1, 2, 3},
			Transport:    transports[id],
			TickInterval: tickIntervals[id],
		})
		require.NoError(tb, err)
		c.Drivers[id] = driver

		root, err := rootreplicated.Open(rootreplicated.Config{Driver: driver})
		require.NoError(tb, err)
		store, err := coordstorage.OpenRootStore(root)
		require.NoError(tb, err)
		c.RootStores[id] = store

		cluster := catalog.NewCluster()
		bootstrap, err := coordstorage.Bootstrap(store, cluster.PublishRegionDescriptor, 1, 1)
		require.NoError(tb, err)
		svc := coordserver.NewService(cluster, idalloc.NewIDAllocator(bootstrap.IDStart), tso.NewAllocator(bootstrap.TSStart), store)
		c.Services[id] = svc
	}
	tb.Cleanup(func() { c.Close() })
	return c
}

func (c *Cluster) Close() {
	if c == nil {
		return
	}
	for id := range c.RootStores {
		c.CloseNode(id)
	}
}

func (c *Cluster) CloseNode(nodeID uint64) {
	if c == nil {
		return
	}
	store, ok := c.RootStores[nodeID]
	if !ok || store == nil {
		return
	}
	require.NoError(c.tb, store.Close(), "close root store %d", nodeID)
	delete(c.RootStores, nodeID)
	delete(c.Services, nodeID)
	delete(c.Drivers, nodeID)
}

func (c *Cluster) WaitLeader(excluded ...uint64) uint64 {
	c.tb.Helper()
	skip := make(map[uint64]struct{}, len(excluded))
	for _, id := range excluded {
		skip[id] = struct{}{}
	}
	var leaderID uint64
	require.Eventually(c.tb, func() bool {
		for id, store := range c.RootStores {
			if _, excluded := skip[id]; excluded || store == nil {
				continue
			}
			if store.IsLeader() {
				leaderID = id
				return true
			}
		}
		return false
	}, 8*time.Second, 50*time.Millisecond)
	return leaderID
}

func (c *Cluster) LeaderService() (uint64, *coordserver.Service) {
	c.tb.Helper()
	id := c.WaitLeader()
	return id, c.Services[id]
}

func (c *Cluster) Campaign(nodeID uint64) {
	c.tb.Helper()
	driver, ok := c.Drivers[nodeID]
	require.True(c.tb, ok, "missing root driver %d", nodeID)
	_ = driver.Campaign()
}

func (c *Cluster) PauseTicks(nodeID uint64) {
	c.tb.Helper()
	driver, ok := c.Drivers[nodeID]
	require.True(c.tb, ok, "missing root driver %d", nodeID)
	driver.PauseTicks()
}

func (c *Cluster) ResumeTicks(nodeID uint64) {
	c.tb.Helper()
	driver, ok := c.Drivers[nodeID]
	require.True(c.tb, ok, "missing root driver %d", nodeID)
	driver.ResumeTicks()
}

func (c *Cluster) TickNode(nodeID uint64, n int) {
	c.tb.Helper()
	driver, ok := c.Drivers[nodeID]
	require.True(c.tb, ok, "missing root driver %d", nodeID)
	for range n {
		_ = driver.Tick()
	}
}

func (c *Cluster) FollowerIDs(leaderID uint64) []uint64 {
	c.tb.Helper()
	out := make([]uint64, 0, len(c.RootStores)-1)
	for _, id := range []uint64{1, 2, 3} {
		if id == leaderID {
			continue
		}
		if _, ok := c.RootStores[id]; ok {
			out = append(out, id)
		}
	}
	require.NotEmpty(c.tb, out, "no followers available for leader %d", leaderID)
	return out
}

func (c *Cluster) SubscribeTail(nodeID uint64, after rootstorage.TailToken) *rootstorage.TailSubscription {
	c.tb.Helper()
	store, ok := c.RootStores[nodeID]
	require.True(c.tb, ok, "missing root store %d", nodeID)
	return store.SubscribeTail(after)
}

func (c *Cluster) WaitRegionVisible(serviceID, regionID uint64, key []byte) {
	c.tb.Helper()
	require.Eventually(c.tb, func() bool {
		resp, err := c.Services[serviceID].GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: key})
		return err == nil && !resp.GetNotFound() && resp.GetRegionDescriptor().GetRegionId() == regionID
	}, 8*time.Second, 50*time.Millisecond)
}

func (c *Cluster) WaitReloaded(serviceID uint64, sub *rootstorage.TailSubscription, key []byte, regionID uint64) {
	c.tb.Helper()
	require.Eventually(c.tb, func() bool {
		advance, err := sub.Next(context.Background(), 500*time.Millisecond)
		if err != nil {
			return false
		}
		switch advance.CatchUpAction() {
		case rootstorage.TailCatchUpRefreshState, rootstorage.TailCatchUpInstallBootstrap:
			if err := c.Services[serviceID].ReloadFromStorage(); err != nil {
				return false
			}
			sub.Acknowledge(advance)
		case rootstorage.TailCatchUpAcknowledgeWindow:
			sub.Acknowledge(advance)
		default:
			return false
		}
		resp, err := c.Services[serviceID].GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: key})
		return err == nil && !resp.GetNotFound() && resp.GetRegionDescriptor().GetRegionId() == regionID
	}, 8*time.Second, 50*time.Millisecond)
}

func (c *Cluster) WaitNotFoundReloaded(serviceID uint64, sub *rootstorage.TailSubscription, key []byte) {
	c.tb.Helper()
	require.Eventually(c.tb, func() bool {
		advance, err := sub.Next(context.Background(), 500*time.Millisecond)
		if err != nil {
			return false
		}
		switch advance.CatchUpAction() {
		case rootstorage.TailCatchUpRefreshState, rootstorage.TailCatchUpInstallBootstrap:
			if err := c.Services[serviceID].ReloadFromStorage(); err != nil {
				return false
			}
			sub.Acknowledge(advance)
		case rootstorage.TailCatchUpAcknowledgeWindow:
			sub.Acknowledge(advance)
		default:
			return false
		}
		resp, err := c.Services[serviceID].GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: key})
		return err == nil && resp.GetNotFound()
	}, 8*time.Second, 50*time.Millisecond)
}

func reservePeerAddrs(tb testing.TB) map[uint64]string {
	tb.Helper()
	out := make(map[uint64]string, 3)
	for _, id := range []uint64{1, 2, 3} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(tb, err)
		out[id] = ln.Addr().String()
		require.NoError(tb, ln.Close())
	}
	return out
}
