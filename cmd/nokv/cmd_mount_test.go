package main

import (
	"bytes"
	"context"
	"testing"

	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

type fakeMountClient struct {
	published []*coordpb.PublishRootEventRequest
	mounts    []*coordpb.MountInfo
	closed    bool
}

func (c *fakeMountClient) PublishRootEvent(_ context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	c.published = append(c.published, req)
	return &coordpb.PublishRootEventResponse{Accepted: true}, nil
}

func (c *fakeMountClient) GetMount(_ context.Context, req *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error) {
	for _, mount := range c.mounts {
		if mount.GetMountId() == req.GetMountId() {
			return &coordpb.GetMountResponse{Mount: mount}, nil
		}
	}
	return &coordpb.GetMountResponse{NotFound: true}, nil
}

func (c *fakeMountClient) ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error) {
	return &coordpb.ListMountsResponse{Mounts: c.mounts}, nil
}

func (c *fakeMountClient) Close() error {
	c.closed = true
	return nil
}

func TestRunMountRegisterCmdPublishesRootEvent(t *testing.T) {
	client := &fakeMountClient{}
	orig := newMountCoordinatorClient
	newMountCoordinatorClient = func(context.Context, string) (mountCoordinatorClient, error) {
		return client, nil
	}
	t.Cleanup(func() { newMountCoordinatorClient = orig })

	var out bytes.Buffer
	err := runMountCmd(&out, []string{
		"register",
		"--coordinator-addr", "127.0.0.1:2379",
		"--mount", "vol",
		"--root-inode", "1",
		"--schema-version", "1",
	})
	require.NoError(t, err)
	require.True(t, client.closed)
	require.Len(t, client.published, 2)
	event := client.published[0].GetEvent()
	require.NotNil(t, event.GetMount())
	require.Equal(t, "vol", event.GetMount().GetMountId())
	authority := client.published[1].GetEvent().GetSubtreeAuthority()
	require.NotNil(t, authority)
	require.Equal(t, "vol", authority.GetMount())
	require.Equal(t, uint64(1), authority.GetRootInode())
	require.Equal(t, "vol", authority.GetAuthorityId())
	require.Equal(t, uint64(0), authority.GetEra())
	require.Equal(t, uint64(0), authority.GetFrontier())
	require.Contains(t, out.String(), "registered")
}

func TestRunMountRegisterCmdBackfillsRootAuthorityForExistingMount(t *testing.T) {
	client := &fakeMountClient{mounts: []*coordpb.MountInfo{{
		MountId:       "vol",
		RootInode:     1,
		SchemaVersion: 1,
		State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
	}}}
	orig := newMountCoordinatorClient
	newMountCoordinatorClient = func(context.Context, string) (mountCoordinatorClient, error) {
		return client, nil
	}
	t.Cleanup(func() { newMountCoordinatorClient = orig })

	var out bytes.Buffer
	err := runMountCmd(&out, []string{
		"register",
		"--coordinator-addr", "127.0.0.1:2379",
		"--mount", "vol",
		"--root-inode", "1",
		"--schema-version", "1",
	})
	require.NoError(t, err)
	require.Len(t, client.published, 1)
	authority := client.published[0].GetEvent().GetSubtreeAuthority()
	require.NotNil(t, authority)
	require.Equal(t, "vol", authority.GetMount())
	require.Equal(t, uint64(1), authority.GetRootInode())
	require.Equal(t, "vol", authority.GetAuthorityId())
	require.Contains(t, out.String(), "already registered")
}

func TestRunMountRetireCmdPublishesRootEvent(t *testing.T) {
	client := &fakeMountClient{}
	orig := newMountCoordinatorClient
	newMountCoordinatorClient = func(context.Context, string) (mountCoordinatorClient, error) {
		return client, nil
	}
	t.Cleanup(func() { newMountCoordinatorClient = orig })

	var out bytes.Buffer
	err := runMountCmd(&out, []string{
		"retire",
		"--coordinator-addr", "127.0.0.1:2379",
		"--mount", "vol",
	})
	require.NoError(t, err)
	require.Len(t, client.published, 1)
	require.NotNil(t, client.published[0].GetEvent().GetMount())
	require.Contains(t, out.String(), "retired")
}

func TestRunMountListCmdRendersMounts(t *testing.T) {
	client := &fakeMountClient{mounts: []*coordpb.MountInfo{{
		MountId:       "vol",
		RootInode:     1,
		SchemaVersion: 1,
		State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
	}}}
	orig := newMountCoordinatorClient
	newMountCoordinatorClient = func(context.Context, string) (mountCoordinatorClient, error) {
		return client, nil
	}
	t.Cleanup(func() { newMountCoordinatorClient = orig })

	var out bytes.Buffer
	err := runMountCmd(&out, []string{"list", "--coordinator-addr", "127.0.0.1:2379"})
	require.NoError(t, err)
	require.Contains(t, out.String(), "id=vol")
	require.Contains(t, out.String(), "MOUNT_STATE_ACTIVE")
}
