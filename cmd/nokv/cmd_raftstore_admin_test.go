// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestAddRaftstorePeerCallsAdminAndValidatesReturnedPeer(t *testing.T) {
	client := &fakeRaftstoreAddPeerClient{
		resp: &adminpb.AddPeerResponse{Region: &metapb.RegionDescriptor{
			RegionId: 1,
			Epoch:    &metapb.RegionEpoch{ConfVersion: 2},
			Peers: []*metapb.RegionPeer{
				{StoreId: 1, PeerId: 1},
				{StoreId: 2, PeerId: 2},
			},
		}},
	}
	region, err := addRaftstorePeer(context.Background(), client, raftstoreAddPeerOptions{
		AdminAddr: "127.0.0.1:23880",
		RegionID:  1,
		StoreID:   2,
		PeerID:    2,
		Timeout:   time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), region.GetRegionId())
	require.Equal(t, uint64(2), client.req.GetStoreId())
	require.Equal(t, uint64(2), client.req.GetPeerId())
}

func TestAddRaftstorePeerRejectsResponseWithoutNewPeer(t *testing.T) {
	client := &fakeRaftstoreAddPeerClient{
		resp: &adminpb.AddPeerResponse{Region: &metapb.RegionDescriptor{
			RegionId: 1,
			Epoch:    &metapb.RegionEpoch{ConfVersion: 1},
			Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 1}},
		}},
	}
	_, err := addRaftstorePeer(context.Background(), client, raftstoreAddPeerOptions{
		AdminAddr: "127.0.0.1:23880",
		RegionID:  1,
		StoreID:   2,
		PeerID:    2,
		Timeout:   time.Second,
	})
	require.ErrorContains(t, err, "does not include")
}

func TestAddRaftstorePeerWrapsAdminError(t *testing.T) {
	_, err := addRaftstorePeer(context.Background(), &fakeRaftstoreAddPeerClient{err: errors.New("not leader")}, raftstoreAddPeerOptions{
		AdminAddr: "127.0.0.1:23880",
		RegionID:  1,
		StoreID:   2,
		PeerID:    2,
		Timeout:   time.Second,
	})
	require.ErrorContains(t, err, "not leader")
	require.ErrorContains(t, err, "region=1 store=2 peer=2")
}

func TestRunRaftstoreAddPeerCmdRequiresPeer(t *testing.T) {
	err := runRaftstoreAddPeerCmd(nil, []string{"-admin-addr", "127.0.0.1:23880", "-store-id", "2"})
	require.ErrorContains(t, err, "requires non-zero --peer-id")
}

type fakeRaftstoreAddPeerClient struct {
	req  *adminpb.AddPeerRequest
	resp *adminpb.AddPeerResponse
	err  error
}

func (c *fakeRaftstoreAddPeerClient) AddPeer(_ context.Context, req *adminpb.AddPeerRequest, _ ...grpc.CallOption) (*adminpb.AddPeerResponse, error) {
	c.req = req
	if c.err != nil {
		return nil, c.err
	}
	return c.resp, nil
}
