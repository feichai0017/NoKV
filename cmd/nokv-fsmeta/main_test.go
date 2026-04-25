package main

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

type fakeMountLookupClient struct {
	calls     int
	listCalls int
	resp      *coordpb.GetMountResponse
	mounts    []*coordpb.MountInfo
	err       error
}

func (c *fakeMountLookupClient) GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error) {
	c.calls++
	return c.resp, c.err
}

func (c *fakeMountLookupClient) ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error) {
	c.listCalls++
	return &coordpb.ListMountsResponse{Mounts: c.mounts}, c.err
}

type fakeMountRetirementRouter struct {
	retired []fsmeta.MountID
}

func (r *fakeMountRetirementRouter) RetireMount(mount fsmeta.MountID) int {
	r.retired = append(r.retired, mount)
	return 1
}

func TestCoordinatorMountResolverCachesActiveMount(t *testing.T) {
	client := &fakeMountLookupClient{
		resp: &coordpb.GetMountResponse{Mount: &coordpb.MountInfo{
			MountId:       "vol",
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
		}},
	}
	now := time.Unix(100, 0)
	resolver := &coordinatorMountResolver{
		coord: client,
		ttl:   time.Minute,
		now:   func() time.Time { return now },
	}

	first, err := resolver.ResolveMount(context.Background(), fsmeta.MountID("vol"))
	require.NoError(t, err)
	second, err := resolver.ResolveMount(context.Background(), fsmeta.MountID("vol"))
	require.NoError(t, err)

	require.Equal(t, 1, client.calls)
	require.Equal(t, first, second)
	require.Equal(t, fsmeta.MountID("vol"), first.MountID)
	require.False(t, first.Retired)
}

func TestCoordinatorMountResolverRefreshesExpiredMount(t *testing.T) {
	client := &fakeMountLookupClient{
		resp: &coordpb.GetMountResponse{Mount: &coordpb.MountInfo{
			MountId:       "vol",
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
		}},
	}
	now := time.Unix(100, 0)
	resolver := &coordinatorMountResolver{
		coord: client,
		ttl:   time.Second,
		now:   func() time.Time { return now },
	}

	first, err := resolver.ResolveMount(context.Background(), fsmeta.MountID("vol"))
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(1), first.RootInode)

	now = now.Add(2 * time.Second)
	client.resp = &coordpb.GetMountResponse{Mount: &coordpb.MountInfo{
		MountId:       "vol",
		RootInode:     9,
		SchemaVersion: 2,
		State:         coordpb.MountState_MOUNT_STATE_RETIRED,
	}}
	second, err := resolver.ResolveMount(context.Background(), fsmeta.MountID("vol"))
	require.NoError(t, err)

	require.Equal(t, 2, client.calls)
	require.Equal(t, fsmeta.InodeID(9), second.RootInode)
	require.True(t, second.Retired)
}

func TestCoordinatorMountResolverCachesNotFound(t *testing.T) {
	client := &fakeMountLookupClient{
		resp: &coordpb.GetMountResponse{NotFound: true},
	}
	now := time.Unix(100, 0)
	resolver := &coordinatorMountResolver{
		coord: client,
		ttl:   time.Minute,
		now:   func() time.Time { return now },
	}

	_, err := resolver.ResolveMount(context.Background(), fsmeta.MountID("missing"))
	require.ErrorIs(t, err, fsmeta.ErrMountNotRegistered)
	_, err = resolver.ResolveMount(context.Background(), fsmeta.MountID("missing"))
	require.ErrorIs(t, err, fsmeta.ErrMountNotRegistered)
	require.Equal(t, 1, client.calls)
}

func TestMountLifecycleMonitorRetiresWatchersAndResolverCache(t *testing.T) {
	client := &fakeMountLookupClient{
		resp: &coordpb.GetMountResponse{Mount: &coordpb.MountInfo{
			MountId:       "vol",
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
		}},
		mounts: []*coordpb.MountInfo{{
			MountId:       "vol",
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_RETIRED,
		}},
	}
	now := time.Unix(100, 0)
	resolver := &coordinatorMountResolver{
		coord: client,
		ttl:   time.Minute,
		now:   func() time.Time { return now },
	}
	router := &fakeMountRetirementRouter{}

	record, err := resolver.ResolveMount(context.Background(), fsmeta.MountID("vol"))
	require.NoError(t, err)
	require.False(t, record.Retired)

	monitor := &mountLifecycleMonitor{coord: client, router: router, resolver: resolver}
	require.NoError(t, monitor.poll(context.Background()))

	record, err = resolver.ResolveMount(context.Background(), fsmeta.MountID("vol"))
	require.NoError(t, err)
	require.True(t, record.Retired)
	require.Equal(t, 1, client.calls)
	require.Equal(t, 1, client.listCalls)
	require.Equal(t, []fsmeta.MountID{"vol"}, router.retired)
}

func TestFSMetaWatchRuntimeRejectsRetiredMount(t *testing.T) {
	client := &fakeMountLookupClient{
		resp: &coordpb.GetMountResponse{Mount: &coordpb.MountInfo{
			MountId:       "vol",
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_RETIRED,
		}},
	}
	resolver := &coordinatorMountResolver{
		coord: client,
		ttl:   time.Minute,
	}
	watcher := fsmetaWatchRuntime{
		Router: fsmetawatch.NewRouter(),
		mounts: resolver,
	}

	_, err := watcher.Subscribe(context.Background(), fsmeta.WatchRequest{
		Mount:     "vol",
		RootInode: fsmeta.RootInode,
	})
	require.ErrorIs(t, err, fsmeta.ErrMountRetired)
	require.Equal(t, 1, client.calls)
}
