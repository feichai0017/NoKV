// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegisterFSMetaMountAllocatesAndPublishes(t *testing.T) {
	coord := &fakeMountRegistrationCoordinator{
		getResponses: []*coordpb.GetMountResponse{{NotFound: true}},
		allocResp:    &coordpb.AllocIDResponse{FirstId: 42, Count: 1},
	}
	registration, err := registerFSMetaMount(context.Background(), coord, fsmetaMountRegisterOptions{
		CoordinatorAddr: "127.0.0.1:1",
		MountID:         "vol",
		RootInode:       1,
		SchemaVersion:   1,
	})
	require.NoError(t, err)
	require.Equal(t, fsmetaMountRegistration{
		MountID:       "vol",
		MountKeyID:    42,
		RootInode:     1,
		SchemaVersion: 1,
	}, registration)
	require.Equal(t, 1, coord.allocCalls)
	require.Len(t, coord.published, 1)
	event := metawire.RootEventFromProto(coord.published[0].GetEvent())
	require.Equal(t, rootevent.KindMountRegistered, event.Kind)
	require.NotNil(t, event.Mount)
	require.Equal(t, "vol", event.Mount.MountID)
	require.Equal(t, uint64(42), event.Mount.MountKeyID)
}

func TestRegisterFSMetaMountExistingActiveDoesNotPublish(t *testing.T) {
	coord := &fakeMountRegistrationCoordinator{
		getResponses: []*coordpb.GetMountResponse{{Mount: &coordpb.MountInfo{
			MountId:       "vol",
			MountKeyId:    7,
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
		}}},
	}
	registration, err := registerFSMetaMount(context.Background(), coord, fsmetaMountRegisterOptions{
		CoordinatorAddr: "127.0.0.1:1",
		MountID:         "vol",
		MountKeyID:      7,
		RootInode:       1,
		SchemaVersion:   1,
	})
	require.NoError(t, err)
	require.True(t, registration.AlreadyExists)
	require.Zero(t, coord.allocCalls)
	require.Empty(t, coord.published)
}

func TestRegisterFSMetaMountRejectsExistingMismatch(t *testing.T) {
	coord := &fakeMountRegistrationCoordinator{
		getResponses: []*coordpb.GetMountResponse{{Mount: &coordpb.MountInfo{
			MountId:       "vol",
			MountKeyId:    7,
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
		}}},
	}
	_, err := registerFSMetaMount(context.Background(), coord, fsmetaMountRegisterOptions{
		CoordinatorAddr: "127.0.0.1:1",
		MountID:         "vol",
		MountKeyID:      8,
		RootInode:       1,
		SchemaVersion:   1,
	})
	require.ErrorContains(t, err, "mount_key_id=7")
	require.Zero(t, coord.allocCalls)
	require.Empty(t, coord.published)
}

func TestRegisterFSMetaMountConflictUsesExistingMount(t *testing.T) {
	coord := &fakeMountRegistrationCoordinator{
		getResponses: []*coordpb.GetMountResponse{
			{NotFound: true},
			{Mount: &coordpb.MountInfo{
				MountId:       "vol",
				MountKeyId:    9,
				RootInode:     1,
				SchemaVersion: 1,
				State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
			}},
		},
		allocResp:  &coordpb.AllocIDResponse{FirstId: 9, Count: 1},
		publishErr: status.Error(codes.FailedPrecondition, "mount registration conflicts with rooted truth"),
	}
	registration, err := registerFSMetaMount(context.Background(), coord, fsmetaMountRegisterOptions{
		CoordinatorAddr: "127.0.0.1:1",
		MountID:         "vol",
		RootInode:       1,
		SchemaVersion:   1,
	})
	require.NoError(t, err)
	require.True(t, registration.AlreadyExists)
	require.Equal(t, uint64(9), registration.MountKeyID)
	require.Equal(t, 1, coord.allocCalls)
	require.Len(t, coord.published, 1)
}

func TestRunFSMetaMountRegisterCmdRequiresMount(t *testing.T) {
	var buf bytes.Buffer
	err := runFSMetaMountRegisterCmd(&buf, []string{"-coordinator-addr", "127.0.0.1:1"})
	require.ErrorContains(t, err, "requires --mount")
}

type fakeMountRegistrationCoordinator struct {
	getResponses []*coordpb.GetMountResponse
	getErr       error
	allocResp    *coordpb.AllocIDResponse
	allocErr     error
	publishErr   error

	allocCalls int
	published  []*coordpb.PublishRootEventRequest
}

func (c *fakeMountRegistrationCoordinator) GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	if len(c.getResponses) == 0 {
		return &coordpb.GetMountResponse{NotFound: true}, nil
	}
	resp := c.getResponses[0]
	c.getResponses = c.getResponses[1:]
	return resp, nil
}

func (c *fakeMountRegistrationCoordinator) AllocID(context.Context, *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	c.allocCalls++
	if c.allocErr != nil {
		return nil, c.allocErr
	}
	return c.allocResp, nil
}

func (c *fakeMountRegistrationCoordinator) PublishRootEvent(_ context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	c.published = append(c.published, req)
	if c.publishErr != nil {
		return nil, c.publishErr
	}
	return &coordpb.PublishRootEventResponse{Accepted: true}, nil
}
