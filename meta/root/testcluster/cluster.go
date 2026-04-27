package testcluster

import (
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	rootreplicated "github.com/feichai0017/NoKV/meta/root/replicated"
	"github.com/stretchr/testify/require"
)

var clusterNodeIDs = []uint64{1, 2, 3}

type Options struct {
	BaseDir            string
	MaxRetainedRecords int
	TickIntervals      map[uint64]time.Duration
}

type Cluster struct {
	tb                 testing.TB
	baseDir            string
	maxRetainedRecords int
	PeerAddrs          map[uint64]string
	blockedPeerAddrs   map[uint64]string
	Transports         map[uint64]rootreplicated.Transport
	Drivers            map[uint64]*rootreplicated.NetworkDriver
	Stores             map[uint64]*rootreplicated.Store
}

func Open(tb testing.TB) *Cluster {
	tb.Helper()
	return OpenWithOptions(tb, Options{})
}

func OpenWithTickIntervals(tb testing.TB, tickIntervals map[uint64]time.Duration) *Cluster {
	tb.Helper()
	return OpenWithOptions(tb, Options{TickIntervals: tickIntervals})
}

func OpenWithOptions(tb testing.TB, opts Options) *Cluster {
	tb.Helper()

	if opts.BaseDir == "" {
		opts.BaseDir = tb.TempDir()
	}
	c := &Cluster{
		tb:                 tb,
		baseDir:            opts.BaseDir,
		maxRetainedRecords: opts.MaxRetainedRecords,
		PeerAddrs:          reservePeerAddrs(tb),
		blockedPeerAddrs:   reservePeerAddrs(tb),
		Transports:         make(map[uint64]rootreplicated.Transport, len(clusterNodeIDs)),
		Drivers:            make(map[uint64]*rootreplicated.NetworkDriver, len(clusterNodeIDs)),
		Stores:             make(map[uint64]*rootreplicated.Store, len(clusterNodeIDs)),
	}

	for _, id := range clusterNodeIDs {
		transport, err := rootreplicated.NewGRPCTransport(id, c.PeerAddrs[id])
		require.NoError(tb, err)
		c.Transports[id] = transport
	}
	for _, transport := range c.Transports {
		transport.SetPeers(c.PeerAddrs)
	}
	for _, id := range clusterNodeIDs {
		driver, err := rootreplicated.NewNetworkDriver(rootreplicated.NetworkConfig{
			ID:           id,
			WorkDir:      filepath.Join(c.baseDir, "node", strconv.FormatUint(id, 10)),
			PeerIDs:      slices.Clone(clusterNodeIDs),
			Transport:    c.Transports[id],
			TickInterval: opts.TickIntervals[id],
		})
		require.NoError(tb, err)
		c.Drivers[id] = driver
		store, err := rootreplicated.Open(rootreplicated.Config{
			Driver:             driver,
			MaxRetainedRecords: c.maxRetainedRecords,
		})
		require.NoError(tb, err)
		c.Stores[id] = store
	}

	require.NoError(tb, c.Drivers[1].Campaign())
	c.WaitLeader()
	tb.Cleanup(c.Close)
	return c
}

func (c *Cluster) Close() {
	if c == nil {
		return
	}
	for id, driver := range c.Drivers {
		if driver == nil {
			continue
		}
		require.NoError(c.tb, driver.Close(), "close root driver %d", id)
	}
}

// WaitLeader waits for unanimous LeaderID agreement across non-excluded
// peers AND for the elected leader's Store to be operational (its
// gRPC-backed Refresh completes without error). The two-stage wait
// matters under leader-transition fault tests: raft may have elected a
// new leader (LeaderID set on every driver) while the new leader's
// Store + transport is still finishing a no-op commit / connection
// re-establishment. Returning before the leader is operational lets a
// follow-up Append race the still-closing gRPC connection with
// "code = Canceled desc = grpc: the client connection is closing".
func (c *Cluster) WaitLeader(excluded ...uint64) uint64 {
	c.tb.Helper()
	skip := make(map[uint64]struct{}, len(excluded))
	for _, id := range excluded {
		skip[id] = struct{}{}
	}
	var leaderID uint64
	require.Eventually(c.tb, func() bool {
		var candidate uint64
		for _, id := range clusterNodeIDs {
			if _, excluded := skip[id]; excluded {
				continue
			}
			driver := c.Drivers[id]
			if driver == nil {
				return false
			}
			seen := driver.LeaderID()
			if seen == 0 {
				return false
			}
			if candidate == 0 {
				candidate = seen
				continue
			}
			if candidate != seen {
				return false
			}
		}
		if candidate == 0 {
			return false
		}
		if _, excluded := skip[candidate]; excluded {
			return false
		}
		// Stage 2: leader is elected by raft; confirm its Store is
		// serviceable. A failing Refresh here means the new leader's
		// transport / state machine is still settling — keep polling.
		store := c.Stores[candidate]
		if store == nil {
			return false
		}
		if err := store.Refresh(); err != nil {
			return false
		}
		leaderID = candidate
		return true
	}, 8*time.Second, 50*time.Millisecond)
	return leaderID
}

func (c *Cluster) Campaign(nodeID uint64) {
	c.tb.Helper()
	driver := c.driver(nodeID)
	require.NoError(c.tb, driver.Campaign())
}

func (c *Cluster) PauseTicks(nodeID uint64) {
	c.tb.Helper()
	c.driver(nodeID).PauseTicks()
}

func (c *Cluster) ResumeTicks(nodeID uint64) {
	c.tb.Helper()
	c.driver(nodeID).ResumeTicks()
}

func (c *Cluster) TickNode(nodeID uint64, n int) {
	c.tb.Helper()
	driver := c.driver(nodeID)
	for range n {
		require.NoError(c.tb, driver.Tick())
	}
}

func (c *Cluster) FollowerIDs(leaderID uint64) []uint64 {
	c.tb.Helper()
	out := make([]uint64, 0, len(clusterNodeIDs)-1)
	for _, id := range clusterNodeIDs {
		if id != leaderID {
			out = append(out, id)
		}
	}
	return out
}

func (c *Cluster) RefreshStore(nodeID uint64) {
	c.tb.Helper()
	store := c.store(nodeID)
	require.NoError(c.tb, store.Refresh())
}

func (c *Cluster) RefreshAll() {
	c.tb.Helper()
	for _, id := range clusterNodeIDs {
		c.RefreshStore(id)
	}
}

func (c *Cluster) ReopenStore(nodeID uint64) *rootreplicated.Store {
	c.tb.Helper()
	driver := c.driver(nodeID)
	store, err := rootreplicated.Open(rootreplicated.Config{
		Driver:             driver,
		MaxRetainedRecords: c.maxRetainedRecords,
	})
	require.NoError(c.tb, err)
	c.Stores[nodeID] = store
	return store
}

func (c *Cluster) IsolateNode(nodeID uint64) {
	c.tb.Helper()
	for _, id := range clusterNodeIDs {
		transport := c.transport(id)
		if id == nodeID {
			for _, peerID := range clusterNodeIDs {
				if peerID == nodeID {
					continue
				}
				transport.SetPeer(peerID, c.blockedPeerAddrs[peerID])
			}
			continue
		}
		transport.SetPeer(nodeID, c.blockedPeerAddrs[nodeID])
	}
}

func (c *Cluster) IsolateNodeEgress(nodeID uint64) {
	c.tb.Helper()
	transport := c.transport(nodeID)
	for _, peerID := range clusterNodeIDs {
		if peerID == nodeID {
			continue
		}
		transport.SetPeer(peerID, c.blockedPeerAddrs[peerID])
	}
}

func (c *Cluster) RestoreNode(nodeID uint64) {
	c.tb.Helper()
	for _, id := range clusterNodeIDs {
		transport := c.transport(id)
		if id == nodeID {
			for _, peerID := range clusterNodeIDs {
				if peerID == nodeID {
					continue
				}
				transport.SetPeer(peerID, c.PeerAddrs[peerID])
			}
			continue
		}
		transport.SetPeer(nodeID, c.PeerAddrs[nodeID])
	}
}

func (c *Cluster) driver(nodeID uint64) *rootreplicated.NetworkDriver {
	driver, ok := c.Drivers[nodeID]
	require.True(c.tb, ok, "missing root driver %d", nodeID)
	return driver
}

func (c *Cluster) store(nodeID uint64) *rootreplicated.Store {
	store, ok := c.Stores[nodeID]
	require.True(c.tb, ok, "missing root store %d", nodeID)
	return store
}

func (c *Cluster) transport(nodeID uint64) rootreplicated.Transport {
	transport, ok := c.Transports[nodeID]
	require.True(c.tb, ok, "missing root transport %d", nodeID)
	return transport
}

func reservePeerAddrs(tb testing.TB) map[uint64]string {
	tb.Helper()
	out := make(map[uint64]string, len(clusterNodeIDs))
	for _, id := range clusterNodeIDs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(tb, err)
		out[id] = ln.Addr().String()
		require.NoError(tb, ln.Close())
	}
	return out
}
