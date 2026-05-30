// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	adminclient "github.com/feichai0017/NoKV/raftstore/admin"
)

func runRaftAdminCmd(w io.Writer, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("raft-admin subcommand is required")
	}
	subcmd := args[0]
	args = args[1:]
	switch subcmd {
	case "add-peer":
		return runRaftAdminAddPeerCmd(w, args)
	case "remove-peer":
		return runRaftAdminRemovePeerCmd(w, args)
	case "transfer-leader":
		return runRaftAdminTransferLeaderCmd(w, args)
	case "region-status":
		return runRaftAdminRegionStatusCmd(w, args)
	default:
		return fmt.Errorf("unknown raft-admin subcommand %q", subcmd)
	}
}

func runRaftAdminAddPeerCmd(w io.Writer, args []string) error {
	fs := raftAdminFlagSet("raft-admin add-peer")
	opts := raftAdminCommonFlags(fs)
	regionID := fs.Uint64("region", 0, "region id")
	storeID := fs.Uint64("store", 0, "store id for the new peer")
	peerID := fs.Uint64("peer", 0, "peer id to add")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *regionID == 0 || *storeID == 0 || *peerID == 0 {
		return fmt.Errorf("--region, --store, and --peer are required")
	}
	client, closeFn, ctx, err := openRaftAdminClient(opts)
	if err != nil {
		return err
	}
	defer closeRaftAdminClient(closeFn)
	resp, err := client.AddPeer(ctx, &adminpb.AddPeerRequest{
		RegionId: *regionID,
		StoreId:  *storeID,
		PeerId:   *peerID,
	})
	if err != nil {
		return fmt.Errorf("add peer: %w", err)
	}
	return renderRaftAdminRegionResult(w, "add-peer", opts.addr(), resp.GetRegion(), *asJSON)
}

func runRaftAdminRemovePeerCmd(w io.Writer, args []string) error {
	fs := raftAdminFlagSet("raft-admin remove-peer")
	opts := raftAdminCommonFlags(fs)
	regionID := fs.Uint64("region", 0, "region id")
	peerID := fs.Uint64("peer", 0, "peer id to remove")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *regionID == 0 || *peerID == 0 {
		return fmt.Errorf("--region and --peer are required")
	}
	client, closeFn, ctx, err := openRaftAdminClient(opts)
	if err != nil {
		return err
	}
	defer closeRaftAdminClient(closeFn)
	resp, err := client.RemovePeer(ctx, &adminpb.RemovePeerRequest{
		RegionId: *regionID,
		PeerId:   *peerID,
	})
	if err != nil {
		return fmt.Errorf("remove peer: %w", err)
	}
	return renderRaftAdminRegionResult(w, "remove-peer", opts.addr(), resp.GetRegion(), *asJSON)
}

func runRaftAdminTransferLeaderCmd(w io.Writer, args []string) error {
	fs := raftAdminFlagSet("raft-admin transfer-leader")
	opts := raftAdminCommonFlags(fs)
	regionID := fs.Uint64("region", 0, "region id")
	peerID := fs.Uint64("peer", 0, "target leader peer id")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *regionID == 0 || *peerID == 0 {
		return fmt.Errorf("--region and --peer are required")
	}
	client, closeFn, ctx, err := openRaftAdminClient(opts)
	if err != nil {
		return err
	}
	defer closeRaftAdminClient(closeFn)
	resp, err := client.TransferLeader(ctx, &adminpb.TransferLeaderRequest{
		RegionId: *regionID,
		PeerId:   *peerID,
	})
	if err != nil {
		return fmt.Errorf("transfer leader: %w", err)
	}
	return renderRaftAdminRegionResult(w, "transfer-leader", opts.addr(), resp.GetRegion(), *asJSON)
}

func runRaftAdminRegionStatusCmd(w io.Writer, args []string) error {
	fs := raftAdminFlagSet("raft-admin region-status")
	opts := raftAdminCommonFlags(fs)
	regionID := fs.Uint64("region", 0, "region id")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *regionID == 0 {
		return fmt.Errorf("--region is required")
	}
	client, closeFn, ctx, err := openRaftAdminClient(opts)
	if err != nil {
		return err
	}
	defer closeRaftAdminClient(closeFn)
	resp, err := client.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{
		RegionId: *regionID,
	})
	if err != nil {
		return fmt.Errorf("query region status: %w", err)
	}
	return renderRaftAdminRegionStatus(w, opts.addr(), resp, *asJSON)
}

type raftAdminFlags struct {
	addrPtr    *string
	timeoutPtr *time.Duration
}

func (f raftAdminFlags) addr() string {
	if f.addrPtr == nil {
		return ""
	}
	return strings.TrimSpace(*f.addrPtr)
}

func (f raftAdminFlags) timeout() time.Duration {
	if f.timeoutPtr == nil {
		return 0
	}
	return *f.timeoutPtr
}

func raftAdminFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func raftAdminCommonFlags(fs *flag.FlagSet) raftAdminFlags {
	return raftAdminFlags{
		addrPtr:    fs.String("addr", "", "raftstore admin address"),
		timeoutPtr: fs.Duration("timeout", 5*time.Second, "admin RPC timeout"),
	}
}

func openRaftAdminClient(opts raftAdminFlags) (adminclient.Client, func() error, context.Context, error) {
	addr := opts.addr()
	if addr == "" {
		return nil, nil, nil, fmt.Errorf("--addr is required")
	}
	ctx := context.Background()
	if timeout := opts.timeout(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		closeFn := func() error {
			cancel()
			return nil
		}
		client, connClose, err := dialAdmin(ctx, addr)
		if err != nil {
			_ = closeFn()
			return nil, nil, nil, fmt.Errorf("dial raft admin %q: %w", addr, err)
		}
		return client, func() error {
			cancel()
			if connClose != nil {
				return connClose()
			}
			return nil
		}, ctx, nil
	}
	client, closeFn, err := dialAdmin(ctx, addr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dial raft admin %q: %w", addr, err)
	}
	return client, closeFn, ctx, nil
}

func closeRaftAdminClient(closeFn func() error) {
	if closeFn != nil {
		_ = closeFn()
	}
}

type raftAdminRegionResultView struct {
	Action string         `json:"action"`
	Addr   string         `json:"addr"`
	Region raftRegionView `json:"region"`
}

type raftAdminRegionStatusView struct {
	Addr         string         `json:"addr"`
	Known        bool           `json:"known"`
	Hosted       bool           `json:"hosted"`
	LocalPeerID  uint64         `json:"local_peer_id,omitempty"`
	LeaderPeerID uint64         `json:"leader_peer_id,omitempty"`
	Leader       bool           `json:"leader"`
	AppliedIndex uint64         `json:"applied_index,omitempty"`
	AppliedTerm  uint64         `json:"applied_term,omitempty"`
	Region       raftRegionView `json:"region"`
}

type raftRegionView struct {
	ID    uint64              `json:"id,omitempty"`
	Epoch raftRegionEpochView `json:"epoch"`
	Start string              `json:"start_key,omitempty"`
	End   string              `json:"end_key,omitempty"`
	Peers []raftPeerView      `json:"peers,omitempty"`
}

type raftRegionEpochView struct {
	Version     uint64 `json:"version,omitempty"`
	ConfVersion uint64 `json:"conf_version,omitempty"`
}

type raftPeerView struct {
	StoreID uint64 `json:"store_id"`
	PeerID  uint64 `json:"peer_id"`
}

func renderRaftAdminRegionResult(w io.Writer, action, addr string, region *metapb.RegionDescriptor, asJSON bool) error {
	view := raftAdminRegionResultView{
		Action: action,
		Addr:   addr,
		Region: buildRaftRegionView(region),
	}
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(view)
	}
	_, _ = fmt.Fprintf(w, "Action                %s\n", view.Action)
	_, _ = fmt.Fprintf(w, "Addr                  %s\n", view.Addr)
	renderRaftRegionText(w, view.Region)
	return nil
}

func renderRaftAdminRegionStatus(w io.Writer, addr string, resp *adminpb.RegionRuntimeStatusResponse, asJSON bool) error {
	view := raftAdminRegionStatusView{Addr: addr}
	if resp != nil {
		view.Known = resp.GetKnown()
		view.Hosted = resp.GetHosted()
		view.LocalPeerID = resp.GetLocalPeerId()
		view.LeaderPeerID = resp.GetLeaderPeerId()
		view.Leader = resp.GetLeader()
		view.AppliedIndex = resp.GetAppliedIndex()
		view.AppliedTerm = resp.GetAppliedTerm()
		view.Region = buildRaftRegionView(resp.GetRegion())
	}
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(view)
	}
	_, _ = fmt.Fprintf(w, "Addr                  %s\n", view.Addr)
	_, _ = fmt.Fprintf(w, "Region.Known          %t\n", view.Known)
	_, _ = fmt.Fprintf(w, "Region.Hosted         %t\n", view.Hosted)
	_, _ = fmt.Fprintf(w, "Region.LocalPeer      %d\n", view.LocalPeerID)
	_, _ = fmt.Fprintf(w, "Region.LeaderPeer     %d\n", view.LeaderPeerID)
	_, _ = fmt.Fprintf(w, "Region.Leader         %t\n", view.Leader)
	_, _ = fmt.Fprintf(w, "Region.AppliedIndex   %d\n", view.AppliedIndex)
	_, _ = fmt.Fprintf(w, "Region.AppliedTerm    %d\n", view.AppliedTerm)
	renderRaftRegionText(w, view.Region)
	return nil
}

func buildRaftRegionView(region *metapb.RegionDescriptor) raftRegionView {
	view := raftRegionView{}
	if region == nil {
		return view
	}
	view.ID = region.GetRegionId()
	view.Start = string(region.GetStartKey())
	view.End = string(region.GetEndKey())
	view.Epoch = raftRegionEpochView{
		Version:     region.GetEpoch().GetVersion(),
		ConfVersion: region.GetEpoch().GetConfVersion(),
	}
	view.Peers = make([]raftPeerView, 0, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		if peer == nil {
			continue
		}
		view.Peers = append(view.Peers, raftPeerView{
			StoreID: peer.GetStoreId(),
			PeerID:  peer.GetPeerId(),
		})
	}
	return view
}

func renderRaftRegionText(w io.Writer, region raftRegionView) {
	_, _ = fmt.Fprintf(w, "Region.ID             %d\n", region.ID)
	_, _ = fmt.Fprintf(w, "Region.Epoch          version=%d conf=%d\n", region.Epoch.Version, region.Epoch.ConfVersion)
	if region.Start != "" || region.End != "" {
		_, _ = fmt.Fprintf(w, "Region.Range          [%q,%q)\n", region.Start, region.End)
	}
	_, _ = fmt.Fprintf(w, "Region.Peers          %s\n", formatRaftPeers(region.Peers))
}

func formatRaftPeers(peers []raftPeerView) string {
	if len(peers) == 0 {
		return ""
	}
	parts := make([]string, 0, len(peers))
	for _, peer := range peers {
		parts = append(parts, fmt.Sprintf("%d:%d", peer.StoreID, peer.PeerID))
	}
	return strings.Join(parts, ",")
}
