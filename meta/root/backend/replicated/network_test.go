package replicated

import (
	"testing"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestNetworkDriverReplicatesAcrossThreeNodes(t *testing.T) {
	transports := map[uint64]Transport{}
	for _, id := range []uint64{1, 2, 3} {
		transport, err := NewGRPCTransport(id, "127.0.0.1:0")
		require.NoError(t, err)
		transports[id] = transport
	}
	peerAddrs := map[uint64]string{}
	for id, transport := range transports {
		peerAddrs[id] = transport.Addr()
	}
	for _, transport := range transports {
		transport.SetPeers(peerAddrs)
	}

	drivers := map[uint64]*NetworkDriver{}
	for _, id := range []uint64{1, 2, 3} {
		driver, err := NewNetworkDriver(NetworkConfig{
			ID:        id,
			PeerIDs:   []uint64{1, 2, 3},
			Transport: transports[id],
		})
		require.NoError(t, err)
		drivers[id] = driver
	}
	for _, driver := range drivers {
		t.Cleanup(func() { _ = driver.Close() })
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
	}, 3*time.Second, 50*time.Millisecond)

	stores := map[uint64]*Store{}
	for _, id := range []uint64{1, 2, 3} {
		store, err := Open(Config{Driver: drivers[id], MaxRetainedRecords: 4})
		require.NoError(t, err)
		stores[id] = store
	}

	commit, err := stores[leaderID].Append(
		rootevent.StoreJoined(1, "s1"),
		rootevent.RegionDescriptorPublished(testDescriptor(60, []byte("a"), []byte("z"))),
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, id := range []uint64{2, 3} {
			_ = stores[id].Refresh()
			current, err := stores[id].Current()
			if err != nil || current != commit.State {
				return false
			}
		}
		return true
	}, 3*time.Second, 50*time.Millisecond)

	events, tail, err := stores[2].ReadSince(rootstate.Cursor{})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, commit.Cursor, tail)
}
