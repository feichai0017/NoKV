package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	pdadapter "github.com/feichai0017/NoKV/pd/adapter"
	pdclient "github.com/feichai0017/NoKV/pd/client"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/kv"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/peer"
	serverpkg "github.com/feichai0017/NoKV/raftstore/server"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

var notifyContext = signal.NotifyContext

func runServeCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	listenAddr := fs.String("addr", "127.0.0.1:20160", "gRPC listen address for NoKV + raft traffic")
	storeID := fs.Uint64("store-id", 0, "store ID assigned to this node")
	electionTick := fs.Int("election-tick", 10, "raft election tick")
	heartbeatTick := fs.Int("heartbeat-tick", 2, "raft heartbeat tick")
	maxMsgBytes := fs.Int("raft-max-msg-bytes", 1<<20, "raft max message bytes")
	maxInflight := fs.Int("raft-max-inflight", 256, "raft max inflight messages")
	raftTickInterval := fs.Duration("raft-tick-interval", 0, "interval between raft ticks (default 100ms)")
	raftDebugLog := fs.Bool("raft-debug-log", false, "enable verbose raft debug logging")
	pdAddr := fs.String("pd-addr", "", "PD-lite gRPC endpoint for cluster mode (required)")
	pdTimeout := fs.Duration("pd-timeout", 2*time.Second, "timeout for PD-lite heartbeat RPCs")
	metricsAddr := fs.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
	var peerFlags []string
	fs.Func("peer", "remote raft peer mapping in the form peerID=address (repeatable)", func(value string) error {
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
	if strings.TrimSpace(*pdAddr) == "" {
		return fmt.Errorf("--pd-addr is required (PD is the only scheduler/control-plane source)")
	}

	localMeta, err := localmeta.OpenLocalStore(*workDir, nil)
	if err != nil {
		return fmt.Errorf("open raftstore local metadata: %w", err)
	}
	defer func() {
		_ = localMeta.Close()
	}()

	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = *workDir
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
	opt.AllowedModes = []raftmode.Mode{
		raftmode.ModeStandalone,
		raftmode.ModeSeeded,
		raftmode.ModeCluster,
	}
	db, err := NoKV.Open(opt)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	// Cluster mode only: route scheduler heartbeats and operations through PD.
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	pdCli, err := pdclient.NewGRPCClient(dialCtx, strings.TrimSpace(*pdAddr))
	cancelDial()
	if err != nil {
		return fmt.Errorf("dial pd %q: %w", *pdAddr, err)
	}
	pdScheduler := pdadapter.NewSchedulerClient(pdadapter.SchedulerClientConfig{
		PD:      pdCli,
		Timeout: *pdTimeout,
	})

	server, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: db,
			Raft: db.RaftLog(),
		},
		Store: storepkg.Config{
			StoreID:   *storeID,
			LocalMeta: localMeta,
			WorkDir:   *workDir,
			Scheduler: pdScheduler,
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
	registerRuntimeStore(server.Store())
	defer unregisterRuntimeStore(server.Store())
	defer func() {
		_ = server.Close()
	}()
	metricsLn, err := startExpvarServer(*metricsAddr)
	if err != nil {
		return fmt.Errorf("start serve metrics endpoint: %w", err)
	}
	if metricsLn != nil {
		defer func() { _ = metricsLn.Close() }()
	}

	transport := server.Transport()
	for _, mapping := range peerFlags {
		parts := strings.SplitN(mapping, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid --peer value %q (want peerID=address)", mapping)
		}
		id, err := parseUint(parts[0])
		if err != nil {
			return fmt.Errorf("invalid peer id in --peer %q: %w", mapping, err)
		}
		transport.SetPeer(id, parts[1])
	}

	startedRegions, totalRegions, err := startStorePeers(server, serverpkg.Storage{MVCC: db, Raft: db.RaftLog()}, localMeta, *storeID, *electionTick, *heartbeatTick, *maxMsgBytes, *maxInflight)
	if err != nil {
		return err
	}
	if err := promoteClusterMode(*workDir, *storeID); err != nil {
		return fmt.Errorf("persist cluster mode: %w", err)
	}
	if totalRegions == 0 {
		_, _ = fmt.Fprintln(w, "Local peer catalog contains no regions; waiting for bootstrap")
	} else {
		_, _ = fmt.Fprintf(w, "Local peer catalog regions: %d, local peers started: %d\n", totalRegions, len(startedRegions))
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

	_, _ = fmt.Fprintf(w, "NoKV service listening on %s (store=%d)\n", server.Addr(), *storeID)
	if metricsLn != nil {
		_, _ = fmt.Fprintf(w, "Serve metrics endpoint listening on http://%s/debug/vars\n", metricsLn.Addr().String())
	}
	_, _ = fmt.Fprintf(w, "Serve mode: cluster (PD enabled, addr=%s)\n", strings.TrimSpace(*pdAddr))
	if len(peerFlags) > 0 {
		_, _ = fmt.Fprintf(w, "Configured peers: %s\n", strings.Join(peerFlags, ", "))
	}
	_, _ = fmt.Fprintf(w, "PD heartbeat sink enabled: %s\n", strings.TrimSpace(*pdAddr))
	_, _ = fmt.Fprintln(w, "Press Ctrl+C to stop")

	ctx, cancel := notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	_, _ = fmt.Fprintln(w, "\nShutting down...")
	return nil
}

func promoteClusterMode(workDir string, storeID uint64) error {
	state, err := raftmode.Read(workDir)
	if err != nil {
		return err
	}
	if state.Mode == raftmode.ModeCluster && state.StoreID == storeID {
		return nil
	}
	state.Mode = raftmode.ModeCluster
	if state.StoreID == 0 {
		state.StoreID = storeID
	}
	return raftmode.Write(workDir, state)
}

func startStorePeers(server *serverpkg.Node, storage serverpkg.Storage, localMeta *localmeta.Store, storeID uint64, electionTick, heartbeatTick, maxMsgBytes, maxInflight int) ([]localmeta.RegionMeta, int, error) {
	if server == nil || storage.MVCC == nil || storage.Raft == nil || localMeta == nil {
		return nil, 0, fmt.Errorf("raftstore: server, storage, or local metadata is nil")
	}
	snapshot := localMeta.Snapshot()
	total := len(snapshot)
	if total == 0 {
		return nil, 0, nil
	}

	store := server.Store()
	transport := server.Transport()
	ids := make([]uint64, 0, len(snapshot))
	for id := range snapshot {
		if id != 0 {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)

	var started []localmeta.RegionMeta
	for _, id := range ids {
		meta := snapshot[id]
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
		peerStorage, err := storage.Raft.Open(meta.ID, localMeta)
		if err != nil {
			return nil, total, fmt.Errorf("raftstore: open peer storage for region %d: %w", meta.ID, err)
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
			Apply:     kv.NewEntryApplier(storage.MVCC),
			Storage:   peerStorage,
			GroupID:   meta.ID,
			Region:    localmeta.CloneRegionMetaPtr(&meta),
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
