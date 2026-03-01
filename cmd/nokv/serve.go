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
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	pdadapter "github.com/feichai0017/NoKV/pd/adapter"
	pdclient "github.com/feichai0017/NoKV/pd/client"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
)

var notifyContext = signal.NotifyContext

func runServeCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	listenAddr := fs.String("addr", "127.0.0.1:20160", "gRPC listen address for TinyKv + raft traffic")
	storeID := fs.Uint64("store-id", 0, "store ID assigned to this node")
	electionTick := fs.Int("election-tick", 10, "raft election tick")
	heartbeatTick := fs.Int("heartbeat-tick", 2, "raft heartbeat tick")
	maxMsgBytes := fs.Int("raft-max-msg-bytes", 1<<20, "raft max message bytes")
	maxInflight := fs.Int("raft-max-inflight", 256, "raft max inflight messages")
	raftTickInterval := fs.Duration("raft-tick-interval", 0, "interval between raft ticks (default 100ms)")
	raftDebugLog := fs.Bool("raft-debug-log", false, "enable verbose raft debug logging")
	pdAddr := fs.String("pd-addr", "", "PD-lite gRPC endpoint for cluster mode (required when --peer is set)")
	pdTimeout := fs.Duration("pd-timeout", 2*time.Second, "timeout for PD-lite heartbeat RPCs")
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
	if len(peerFlags) > 0 && strings.TrimSpace(*pdAddr) == "" {
		return fmt.Errorf("--pd-addr is required when --peer is configured")
	}

	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = *workDir
	db := NoKV.Open(opt)
	defer func() {
		_ = db.Close()
	}()

	var schedulerSink scheduler.RegionSink
	var pdSink *pdadapter.RegionSink
	if strings.TrimSpace(*pdAddr) != "" {
		// Cluster mode: route scheduler heartbeats and operations through PD.
		// This is the only runtime control-plane path for distributed mode.
		// In this mode, local in-process coordinator state is not authoritative.
		dialCtx, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
		pdCli, err := pdclient.NewGRPCClient(dialCtx, strings.TrimSpace(*pdAddr))
		cancelDial()
		if err != nil {
			return fmt.Errorf("dial pd %q: %w", *pdAddr, err)
		}
		pdSink = pdadapter.NewRegionSink(pdadapter.RegionSinkConfig{
			PD:      pdCli,
			Timeout: *pdTimeout,
		})
		schedulerSink = pdSink
	} else {
		// Standalone mode: keep an in-process coordinator strictly for local
		// observability/testing (`nokv scheduler`).
		// This local coordinator is process-scoped and should not be treated as
		// cluster-wide metadata truth.
		schedulerSink = scheduler.NewCoordinator()
	}
	if pdSink != nil {
		defer func() {
			_ = pdSink.Close()
		}()
	}
	runtimeMode := runtimeModeDevStandalone
	if pdSink != nil {
		runtimeMode = runtimeModeClusterPD
	}

	server, err := raftstore.NewServer(raftstore.ServerConfig{
		DB: db,
		Store: raftstore.StoreConfig{
			StoreID:   *storeID,
			Scheduler: schedulerSink,
		},
		EnableRaftDebugLog: *raftDebugLog,
		RaftTickInterval:   *raftTickInterval,
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
	registerRuntimeStoreWithMode(server.Store(), runtimeMode)
	defer unregisterRuntimeStore(server.Store())
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
		_, _ = fmt.Fprintln(w, "Manifest contains no regions; waiting for bootstrap")
	} else {
		_, _ = fmt.Fprintf(w, "Manifest regions: %d, local peers started: %d\n", totalRegions, len(startedRegions))
		if missing := totalRegions - len(startedRegions); missing > 0 {
			_, _ = fmt.Fprintf(w, "Store %d not present in %d region(s)\n", *storeID, missing)
		}
		if len(startedRegions) > 0 {
			_, _ = fmt.Fprintln(w, "Sample regions:")
			for i, meta := range startedRegions {
				if i >= 5 {
					_, _ = fmt.Fprintf(w, "  ... (%d more)\n", len(startedRegions)-i)
					break
				}
				_, _ = fmt.Fprintf(w, "  - id=%d range=[%s,%s) peers=%s\n", meta.ID, formatKey(meta.StartKey, true), formatKey(meta.EndKey, false), formatPeers(meta.Peers))
			}
		}
	}

	_, _ = fmt.Fprintf(w, "TinyKv service listening on %s (store=%d)\n", server.Addr(), *storeID)
	switch runtimeMode {
	case runtimeModeClusterPD:
		_, _ = fmt.Fprintf(w, "Serve mode: cluster (PD enabled, addr=%s)\n", strings.TrimSpace(*pdAddr))
	default:
		_, _ = fmt.Fprintln(w, "Serve mode: dev-standalone (PD disabled)")
	}
	if len(peerFlags) > 0 {
		_, _ = fmt.Fprintf(w, "Configured peers: %s\n", strings.Join(peerFlags, ", "))
	}
	if pdSink != nil {
		_, _ = fmt.Fprintf(w, "PD heartbeat sink enabled: %s\n", strings.TrimSpace(*pdAddr))
	}
	_, _ = fmt.Fprintln(w, "Press Ctrl+C to stop")

	ctx, cancel := notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	_, _ = fmt.Fprintln(w, "\nShutting down...")
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
		var peerID uint64
		for _, p := range meta.Peers {
			if p.StoreID == storeID {
				peerID = p.PeerID
				break
			}
		}
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

func parseUint(value string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(value), 10, 64)
}
