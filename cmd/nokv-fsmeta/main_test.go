package main

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

type fakeMountListClient struct {
	listCalls int
	mounts    []*coordpb.MountInfo
	err       error
}

func (c *fakeMountListClient) ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error) {
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

func TestMountLifecycleMonitorRetiresWatchersAndResolverCache(t *testing.T) {
	client := &fakeMountListClient{
		mounts: []*coordpb.MountInfo{{
			MountId:       "vol",
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_RETIRED,
		}},
	}
	resolver := &fakeMountRetirer{}
	router := &fakeMountRetirementRouter{}

	monitor := &mountLifecycleMonitor{coord: client, router: router, resolver: resolver}
	require.NoError(t, monitor.poll(context.Background()))

	require.Equal(t, 1, client.listCalls)
	require.Equal(t, []fsmeta.MountID{"vol"}, router.retired)
	require.Equal(t, []fsmeta.MountID{"vol"}, resolver.retired)
}

type fakeMountRetirer struct {
	retired []fsmeta.MountID
}

func (r *fakeMountRetirer) MarkMountRetired(mount fsmeta.MountID) {
	r.retired = append(r.retired, mount)
}
