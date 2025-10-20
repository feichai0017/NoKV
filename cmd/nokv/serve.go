package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

func runServeCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	listenAddr := fs.String("addr", "127.0.0.1:20160", "gRPC listen address for TinyKv + raft traffic")
	storeID := fs.Uint64("store-id", 0, "store ID assigned to this node")
	electionTick := fs.Int("election-tick", 10, "raft election tick")
	heartbeatTick := fs.Int("heartbeat-tick", 2, "raft heartbeat tick")
	maxMsgBytes := fs.Int("raft-max-msg-bytes", 1<<20, "raft max message bytes")
	maxInflight := fs.Int("raft-max-inflight", 256, "raft max inflight messages")
	var peerFlags []string
	fs.Func("peer", "remote store mapping in the form storeID=address (repeatable)", func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("peer value cannot be empty")
		}
		peerFlags = append(peerFlags, value)
		return nil
	})
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}
	if *storeID == 0 {
		return fmt.Errorf("--store-id is required")
	}
	if *electionTick <= 0 || *heartbeatTick <= 0 {
		return fmt.Errorf("heartbeat and election ticks must be > 0")
	}

	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = *workDir
	db := NoKV.Open(opt)
	defer func() {
		_ = db.Close()
	}()

	server, err := raftstore.NewServer(raftstore.ServerConfig{
		DB: db,
		Store: raftstore.StoreConfig{
			StoreID: *storeID,
		},
		Raft: myraft.Config{
			ElectionTick:    *electionTick,
			HeartbeatTick:   *heartbeatTick,
			MaxSizePerMsg:   uint64(*maxMsgBytes),
			MaxInflightMsgs: *maxInflight,
			PreVote:         true,
		},
		TransportAddr: *listenAddr,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = server.Close()
	}()

	transport := server.Transport()
	for _, mapping := range peerFlags {
		parts := strings.SplitN(mapping, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid --peer value %q (want storeID=address)", mapping)
		}
		id, err := parseUint(parts[0])
		if err != nil {
			return fmt.Errorf("invalid store id in --peer %q: %w", mapping, err)
		}
		if id == *storeID {
			continue
		}
		transport.SetPeer(id, parts[1])
	}

	startedRegions, totalRegions, err := startStorePeers(server, db, *storeID, *electionTick, *heartbeatTick, *maxMsgBytes, *maxInflight)
	if err != nil {
		return err
	}
	if totalRegions == 0 {
		fmt.Fprintln(w, "Manifest contains no regions; waiting for bootstrap")
	} else {
		fmt.Fprintf(w, "Manifest regions: %d, local peers started: %d\n", totalRegions, len(startedRegions))
		if missing := totalRegions - len(startedRegions); missing > 0 {
			fmt.Fprintf(w, "Store %d not present in %d region(s)\n", *storeID, missing)
		}
		if len(startedRegions) > 0 {
			fmt.Fprintln(w, "Sample regions:")
			for i, meta := range startedRegions {
				if i >= 5 {
					fmt.Fprintf(w, "  ... (%d more)\n", len(startedRegions)-i)
					break
				}
				fmt.Fprintf(w, "  - id=%d range=[%s,%s) peers=%s\n", meta.ID, formatKey(meta.StartKey, true), formatKey(meta.EndKey, false), formatPeers(meta.Peers))
			}
		}
	}

	fmt.Fprintf(w, "TinyKv service listening on %s (store=%d)\n", server.Addr(), *storeID)
	if len(peerFlags) > 0 {
		fmt.Fprintf(w, "Configured peers: %s\n", strings.Join(peerFlags, ", "))
	}
	fmt.Fprintln(w, "Press Ctrl+C to stop")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	fmt.Fprintln(w, "\nShutting down...")
	return nil
}

func startStorePeers(server *raftstore.Server, db *NoKV.DB, storeID uint64, electionTick, heartbeatTick, maxMsgBytes, maxInflight int) ([]manifest.RegionMeta, int, error) {
	if server == nil || db == nil {
		return nil, 0, fmt.Errorf("raftstore: server or db is nil")
	}
	mgr := db.Manifest()
	if mgr == nil {
		return nil, 0, fmt.Errorf("raftstore: manifest manager unavailable")
	}
	snapshot := mgr.RegionSnapshot()
	total := len(snapshot)
	if total == 0 {
		return nil, 0, nil
	}

	store := server.Store()
	transport := server.Transport()
	var started []manifest.RegionMeta
	for _, meta := range snapshot {
		peerID := peerIDForStore(meta, storeID)
		if peerID == 0 {
			continue
		}
		cfg := &peer.Config{
			RaftConfig: myraft.Config{
				ID:              peerID,
				ElectionTick:    electionTick,
				HeartbeatTick:   heartbeatTick,
				MaxSizePerMsg:   uint64(maxMsgBytes),
				MaxInflightMsgs: maxInflight,
				PreVote:         true,
			},
			Transport: transport,
			Apply:     kv.NewEntryApplier(db),
			WAL:       db.WAL(),
			Manifest:  mgr,
			GroupID:   meta.ID,
			Region:    manifest.CloneRegionMetaPtr(&meta),
		}
		var bootstrapPeers []myraft.Peer
		for _, p := range meta.Peers {
			bootstrapPeers = append(bootstrapPeers, myraft.Peer{ID: p.PeerID})
		}
		if _, err := store.StartPeer(cfg, bootstrapPeers); err != nil {
			return started, total, fmt.Errorf("raftstore: start peer for region %d: %w", meta.ID, err)
		}
		started = append(started, meta)
	}
	return started, total, nil
}

func formatKey(key []byte, isStart bool) string {
	if len(key) == 0 {
		if isStart {
			return "-inf"
		}
		return "+inf"
	}
	return fmt.Sprintf("%q", string(key))
}

func peerIDForStore(meta manifest.RegionMeta, storeID uint64) uint64 {
	for _, p := range meta.Peers {
		if p.StoreID == storeID {
			return p.PeerID
		}
	}
	return 0
}

func parseUint(value string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(value), 10, 64)
}
