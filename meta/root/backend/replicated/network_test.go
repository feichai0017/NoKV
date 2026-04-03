package replicated

import (
	"testing"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestNetworkDriverReplicatesAcrossThreeNodes(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)

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
	require.NotEmpty(t, events)
	require.Equal(t, uint64(60), events[len(events)-1].RegionDescriptor.Descriptor.RegionID)
	require.Equal(t, commit.Cursor, tail)
}
