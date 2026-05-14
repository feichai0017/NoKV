// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

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
	alloc     *coordpb.AllocIDResponse
	nextID    uint64
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

func (c *fakeMountClient) AllocID(context.Context, *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	if c.alloc != nil {
		return c.alloc, nil
	}
	if c.nextID == 0 {
		c.nextID = 42
	}
	id := c.nextID
	c.nextID++
	return &coordpb.AllocIDResponse{FirstId: id, Count: 1}, nil
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
	require.Len(t, client.published, 1)
	event := client.published[0].GetEvent()
	require.NotNil(t, event.GetMount())
	require.Equal(t, "vol", event.GetMount().GetMountId())
	require.Equal(t, uint64(42), event.GetMount().GetMountKeyId())
	require.Contains(t, out.String(), "registered")
}

func TestRunMountRegisterCmdRejectsInvalidAllocation(t *testing.T) {
	client := &fakeMountClient{alloc: &coordpb.AllocIDResponse{}}
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
	})
	require.ErrorContains(t, err, "invalid mount_key_id allocation")
	require.Empty(t, client.published)
}

func TestRunMountRegisterCmdLeavesExistingMountUnchanged(t *testing.T) {
	client := &fakeMountClient{mounts: []*coordpb.MountInfo{{
		MountId:       "vol",
		MountKeyId:    42,
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
	require.Empty(t, client.published)
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
		MountKeyId:    42,
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
