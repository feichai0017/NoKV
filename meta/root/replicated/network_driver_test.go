package replicated

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestNetworkDriverReplicatesAcrossThreeNodes(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)

	commit, err := stores[leaderID].Append(context.Background(),
		rootevent.StoreJoined(1),
		rootevent.RegionDescriptorPublished(testDescriptor(60, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, id := range []uint64{2, 3} {
			_ = stores[id].Refresh()
			current, err := stores[id].Current()
			if err != nil || !reflect.DeepEqual(current, commit.State) {
				return false
			}
		}
		return true
	}, 3*time.Second, 50*time.Millisecond)

	events, tail, err := stores[2].ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.Equal(t, uint64(60), events[len(events)-1].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, commit.Cursor, tail)
}

func TestNetworkDriverRestartsFromPersistedState(t *testing.T) {
	baseDir := t.TempDir()
	peerAddrs := reserveNetworkPeerAddrs(t)

	openCluster := func() (map[uint64]*Store, map[uint64]*NetworkDriver, uint64) {
		transports := map[uint64]Transport{}
		for _, id := range []uint64{1, 2, 3} {
			transport, err := NewGRPCTransport(id, peerAddrs[id])
			require.NoError(t, err)
			transports[id] = transport
		}
		for _, transport := range transports {
			transport.SetPeers(peerAddrs)
		}
		drivers := map[uint64]*NetworkDriver{}
		for _, id := range []uint64{1, 2, 3} {
			driver, err := NewNetworkDriver(NetworkConfig{
				ID:        id,
				WorkDir:   filepath.Join(baseDir, "node", fmt.Sprintf("%d", id)),
				PeerIDs:   []uint64{1, 2, 3},
				Transport: transports[id],
			})
			require.NoError(t, err)
			drivers[id] = driver
		}
		require.NoError(t, drivers[1].Campaign())

		var leaderID uint64
		require.Eventually(t, func() bool {
			id := drivers[1].LeaderID()
			if id == 0 {
				return false
			}
			for _, driver := range drivers {
				if driver.LeaderID() != id {
					return false
				}
			}
			leaderID = id
			return true
		}, 5*time.Second, 50*time.Millisecond)

		stores := map[uint64]*Store{}
		for _, id := range []uint64{1, 2, 3} {
			store, err := Open(Config{Driver: drivers[id], MaxRetainedRecords: 8})
			require.NoError(t, err)
			stores[id] = store
		}
		return stores, drivers, leaderID
	}

	stores, drivers, leaderID := openCluster()
	commit1, err := stores[leaderID].Append(context.Background(), rootevent.StoreJoined(1))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, id := range []uint64{1, 2, 3} {
			_ = stores[id].Refresh()
			current, err := stores[id].Current()
			if err != nil || !reflect.DeepEqual(current, commit1.State) {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)

	for _, store := range stores {
		require.NoError(t, store.Close())
	}
	for _, driver := range drivers {
		require.NoError(t, driver.Close())
	}

	stores, drivers, leaderID = openCluster()
	defer func() {
		for _, store := range stores {
			_ = store.Close()
		}
		for _, driver := range drivers {
			_ = driver.Close()
		}
	}()

	commit2, err := stores[leaderID].Append(context.Background(), rootevent.RegionDescriptorPublished(testDescriptor(88, []byte("a"), []byte("z"))))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, id := range []uint64{1, 2, 3} {
			_ = stores[id].Refresh()
			current, err := stores[id].Current()
			if err != nil || !reflect.DeepEqual(current, commit2.State) {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)

	current, err := stores[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, commit2.State, current)

	snapshot, err := stores[leaderID].Snapshot()
	require.NoError(t, err)
	require.Contains(t, snapshot.Descriptors, uint64(88))
}

func reserveNetworkPeerAddrs(t *testing.T) map[uint64]string {
	t.Helper()
	out := make(map[uint64]string, 3)
	for _, id := range []uint64{1, 2, 3} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		out[id] = ln.Addr().String()
		require.NoError(t, ln.Close())
	}
	return out
}
