// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	adminclient "github.com/feichai0017/NoKV/raftstore/admin"
	"github.com/stretchr/testify/require"
)

func TestRunRaftAdminAddPeerCmd(t *testing.T) {
	fake := &fakeAdminClient{
		addResp: &adminpb.AddPeerResponse{Region: testRaftAdminRegion(2)},
	}
	installRaftAdminDialer(t, "127.0.0.1:21170", fake)

	var buf bytes.Buffer
	err := runRaftAdminCmd(&buf, []string{
		"add-peer",
		"-addr", "127.0.0.1:21170",
		"-timeout", "2s",
		"-region", "1",
		"-store", "2",
		"-peer", "12",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), fake.addReq.GetRegionId())
	require.Equal(t, uint64(2), fake.addReq.GetStoreId())
	require.Equal(t, uint64(12), fake.addReq.GetPeerId())
	require.Contains(t, buf.String(), "Action                add-peer")
	require.Contains(t, buf.String(), "Region.Peers          1:11,2:12")
}

func TestRunRaftAdminRemovePeerCmdJSON(t *testing.T) {
	fake := &fakeAdminClient{
		removeResp: &adminpb.RemovePeerResponse{Region: testRaftAdminRegion(3)},
	}
	installRaftAdminDialer(t, "127.0.0.1:21170", fake)

	var buf bytes.Buffer
	err := runRaftAdminCmd(&buf, []string{
		"remove-peer",
		"-addr", "127.0.0.1:21170",
		"-region", "1",
		"-peer", "12",
		"-json",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), fake.removeReq.GetRegionId())
	require.Equal(t, uint64(12), fake.removeReq.GetPeerId())
	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, "remove-peer", payload["action"])
	region := payload["region"].(map[string]any)
	epoch := region["epoch"].(map[string]any)
	require.Equal(t, float64(3), epoch["conf_version"])
}

func TestRunRaftAdminTransferLeaderCmd(t *testing.T) {
	fake := &fakeAdminClient{
		transferResp: &adminpb.TransferLeaderResponse{Region: testRaftAdminRegion(4)},
	}
	installRaftAdminDialer(t, "127.0.0.1:21170", fake)

	var buf bytes.Buffer
	err := runRaftAdminCmd(&buf, []string{
		"transfer-leader",
		"-addr", "127.0.0.1:21170",
		"-region", "1",
		"-peer", "12",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), fake.transferReq.GetRegionId())
	require.Equal(t, uint64(12), fake.transferReq.GetPeerId())
	require.Contains(t, buf.String(), "Action                transfer-leader")
}

func TestRunRaftAdminRegionStatusCmdJSON(t *testing.T) {
	fake := &fakeAdminClient{
		runtimeResp: &adminpb.RegionRuntimeStatusResponse{
			Known:        true,
			Hosted:       true,
			LocalPeerId:  11,
			LeaderPeerId: 11,
			Leader:       true,
			AppliedIndex: 7,
			AppliedTerm:  2,
			Region:       testRaftAdminRegion(5),
		},
	}
	installRaftAdminDialer(t, "127.0.0.1:21170", fake)

	var buf bytes.Buffer
	err := runRaftAdminCmd(&buf, []string{
		"region-status",
		"-addr", "127.0.0.1:21170",
		"-region", "1",
		"-json",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), fake.runtimeReq.GetRegionId())
	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, true, payload["known"])
	require.Equal(t, true, payload["leader"])
	require.Equal(t, float64(7), payload["applied_index"])
}

func TestRunRaftAdminCmdValidatesRequiredFields(t *testing.T) {
	var buf bytes.Buffer
	require.ErrorContains(t, runRaftAdminCmd(&buf, nil), "subcommand is required")
	require.ErrorContains(t, runRaftAdminCmd(&buf, []string{"add-peer", "-addr", "x", "-region", "1"}), "--region, --store, and --peer")
	require.ErrorContains(t, runRaftAdminCmd(&buf, []string{"remove-peer", "-addr", "x", "-region", "1"}), "--region and --peer")
	require.ErrorContains(t, runRaftAdminCmd(&buf, []string{"transfer-leader", "-addr", "x", "-region", "1"}), "--region and --peer")
	require.ErrorContains(t, runRaftAdminCmd(&buf, []string{"region-status", "-addr", "x"}), "--region is required")
}

func installRaftAdminDialer(t *testing.T, wantAddr string, fake *fakeAdminClient) {
	t.Helper()
	origDial := dialAdmin
	dialAdmin = func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
		require.Equal(t, wantAddr, addr)
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.True(t, time.Until(deadline) > 0)
		return fake, func() error { return nil }, nil
	}
	t.Cleanup(func() { dialAdmin = origDial })
}

func testRaftAdminRegion(confVersion uint64) *metapb.RegionDescriptor {
	return &metapb.RegionDescriptor{
		RegionId: 1,
		Epoch: &metapb.RegionEpoch{
			Version:     1,
			ConfVersion: confVersion,
		},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 11},
			{StoreId: 2, PeerId: 12},
		},
	}
}
