package exec

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

type fakeMountList struct {
	calls  int
	mounts []*coordpb.MountInfo
	err    error
}

func (c *fakeMountList) ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error) {
	c.calls++
	return &coordpb.ListMountsResponse{Mounts: c.mounts}, c.err
}

type fakeRetireRouter struct {
	retired []fsmeta.MountID
}

func (r *fakeRetireRouter) RetireMount(m fsmeta.MountID) int {
	r.retired = append(r.retired, m)
	return 1
}

func TestMonitorRetiresWatchersAndCache(t *testing.T) {
	list := &fakeMountList{
		mounts: []*coordpb.MountInfo{{
			MountId:       "vol",
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_RETIRED,
		}},
	}
	cache := &mountCache{ttl: time.Minute}
	router := &fakeRetireRouter{}

	mon := &monitor{coord: list, router: router, cache: cache}
	require.NoError(t, mon.poll(context.Background()))

	require.Equal(t, 1, list.calls)
	require.Equal(t, []fsmeta.MountID{"vol"}, router.retired)

	entry, ok := cache.entries["vol"]
	require.True(t, ok)
	require.True(t, entry.record.Retired)
}
