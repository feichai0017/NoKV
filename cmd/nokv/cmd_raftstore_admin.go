// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type raftstoreAddPeerOptions struct {
	AdminAddr string
	RegionID  uint64
	StoreID   uint64
	PeerID    uint64
	Timeout   time.Duration
}

type raftstoreAddPeerClient interface {
	AddPeer(context.Context, *adminpb.AddPeerRequest, ...grpc.CallOption) (*adminpb.AddPeerResponse, error)
}

func runRaftstoreAddPeerCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("raftstore-add-peer", flag.ContinueOnError)
	adminAddr := fs.String("admin-addr", "127.0.0.1:23880", "Rust raftstore RaftAdmin gRPC endpoint")
	regionID := fs.Uint64("region-id", 1, "region id")
	storeID := fs.Uint64("store-id", 0, "store id for the new peer")
	peerID := fs.Uint64("peer-id", 0, "peer id to add")
	timeout := fs.Duration("timeout", 30*time.Second, "operation timeout")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts := raftstoreAddPeerOptions{
		AdminAddr: strings.TrimSpace(*adminAddr),
		RegionID:  *regionID,
		StoreID:   *storeID,
		PeerID:    *peerID,
		Timeout:   *timeout,
	}
	if err := validateRaftstoreAddPeerOptions(opts); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()
	conn, err := grpc.NewClient("dns:///"+opts.AdminAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial raftstore admin %s: %w", opts.AdminAddr, err)
	}
	defer func() { _ = conn.Close() }()
	region, err := addRaftstorePeer(ctx, adminpb.NewRaftAdminClient(conn), opts)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "Raftstore peer added: region_id=%d store_id=%d peer_id=%d peers=%d conf_version=%d\n",
		opts.RegionID, opts.StoreID, opts.PeerID, len(region.GetPeers()), region.GetEpoch().GetConfVersion())
	return nil
}

func validateRaftstoreAddPeerOptions(opts raftstoreAddPeerOptions) error {
	if strings.TrimSpace(opts.AdminAddr) == "" {
		return fmt.Errorf("raftstore add peer requires --admin-addr")
	}
	if opts.RegionID == 0 {
		return fmt.Errorf("raftstore add peer requires non-zero --region-id")
	}
	if opts.StoreID == 0 {
		return fmt.Errorf("raftstore add peer requires non-zero --store-id")
	}
	if opts.PeerID == 0 {
		return fmt.Errorf("raftstore add peer requires non-zero --peer-id")
	}
	if opts.Timeout <= 0 {
		return fmt.Errorf("raftstore add peer requires positive --timeout")
	}
	return nil
}

func addRaftstorePeer(ctx context.Context, admin raftstoreAddPeerClient, opts raftstoreAddPeerOptions) (*metapb.RegionDescriptor, error) {
	if admin == nil {
		return nil, fmt.Errorf("raftstore add peer requires admin client")
	}
	if err := validateRaftstoreAddPeerOptions(opts); err != nil {
		return nil, err
	}
	resp, err := admin.AddPeer(ctx, &adminpb.AddPeerRequest{
		RegionId: opts.RegionID,
		StoreId:  opts.StoreID,
		PeerId:   opts.PeerID,
	})
	if err != nil {
		return nil, fmt.Errorf("add raftstore peer region=%d store=%d peer=%d: %w", opts.RegionID, opts.StoreID, opts.PeerID, err)
	}
	if resp == nil || resp.GetRegion() == nil {
		return nil, fmt.Errorf("add raftstore peer returned no region descriptor")
	}
	region := resp.GetRegion()
	if region.GetRegionId() != opts.RegionID {
		return nil, fmt.Errorf("add raftstore peer returned region_id=%d, expected %d", region.GetRegionId(), opts.RegionID)
	}
	for _, peer := range region.GetPeers() {
		if peer.GetStoreId() == opts.StoreID && peer.GetPeerId() == opts.PeerID {
			return region, nil
		}
	}
	return nil, fmt.Errorf("add raftstore peer response does not include store=%d peer=%d", opts.StoreID, opts.PeerID)
}
