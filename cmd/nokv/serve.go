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
	"github.com/feichai0017/NoKV/config"
	coordadapter "github.com/feichai0017/NoKV/coordinator/adapter"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
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
	coordAddr := fs.String("coordinator-addr", "", "coordinator gRPC endpoint for cluster mode (required)")
	configPath := fs.String("config", "", "optional raft configuration file used to resolve listen/workdir/peer addresses")
	scope := fs.String("scope", "host", "scope for config-resolved addresses: host|docker")
	coordTimeout := fs.Duration("coordinator-timeout", 2*time.Second, "timeout for coordinator heartbeat RPCs")
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

	var cfg *config.File
	if strings.TrimSpace(*configPath) != "" {
		scopeNorm := strings.ToLower(strings.TrimSpace(*scope))
		if scopeNorm != "host" && scopeNorm != "docker" {
			return fmt.Errorf("invalid serve scope %q (expected host|docker)", *scope)
		}
		loaded, err := config.LoadFile(strings.TrimSpace(*configPath))
		if err != nil {
			return fmt.Errorf("serve load config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if err := loaded.Validate(); err != nil {
			return fmt.Errorf("serve validate config %q: %w", strings.TrimSpace(*configPath), err)
		}
		cfg = loaded
	}

	if *storeID == 0 {
		return fmt.Errorf("--store-id is required")
	}
	if cfg != nil {
		scopeNorm := strings.ToLower(strings.TrimSpace(*scope))
		if !flagPassed(fs, "workdir") {
			if resolved := cfg.ResolveStoreWorkDir(*storeID, scopeNorm); resolved != "" {
				*workDir = resolved
			}
		}
		if !flagPassed(fs, "addr") {
			if resolved := cfg.ResolveStoreListenAddr(*storeID, scopeNorm); resolved != "" {
				*listenAddr = resolved
			}
		}
		if !flagPassed(fs, "coordinator-addr") {
			if resolved := cfg.ResolveCoordinatorAddr(scopeNorm); resolved != "" {
				*coordAddr = resolved
			}
		}
	}
	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}
	if *electionTick <= 0 || *heartbeatTick <= 0 {
		return fmt.Errorf("heartbeat and election ticks must be > 0")
	}
	if strings.TrimSpace(*coordAddr) == "" {
		return fmt.Errorf("--coordinator-addr is required (coordinator is the only scheduler/control-plane source)")
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

	// Cluster mode only: route scheduler heartbeats and operations through the Coordinator.
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	coordCli, err := coordclient.NewGRPCClient(dialCtx, strings.TrimSpace(*coordAddr))
	cancelDial()
	if err != nil {
		return fmt.Errorf("dial coordinator %q: %w", *coordAddr, err)
	}
	coordScheduler := coordadapter.NewSchedulerClient(coordadapter.SchedulerClientConfig{
		Coordinator: coordCli,
		Timeout:     *coordTimeout,
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
			Scheduler: coordScheduler,
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
	explicitPeers := make(map[uint64]string, len(peerFlags))
	for _, mapping := range peerFlags {
		parts := strings.SplitN(mapping, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid --peer value %q (want peerID=address)", mapping)
		}
		id, err := parseUint(parts[0])
		if err != nil {
			return fmt.Errorf("invalid peer id in --peer %q: %w", mapping, err)
		}
		explicitPeers[id] = strings.TrimSpace(parts[1])
	}
	snapshot := localMeta.Snapshot()
	if cfg != nil {
		autoPeers, err := resolveTransportPeersFromConfig(cfg, strings.ToLower(strings.TrimSpace(*scope)), snapshot, *storeID, explicitPeers)
		if err != nil {
			return err
		}
		for peerID, addr := range autoPeers {
			if strings.TrimSpace(addr) == "" {
				continue
			}
			transport.SetPeer(peerID, addr)
		}
	} else if err := requireExplicitTransportPeers(snapshot, *storeID, explicitPeers); err != nil {
		return err
	}
	for peerID, addr := range explicitPeers {
		if strings.TrimSpace(addr) == "" {
			continue
		}
		transport.SetPeer(peerID, addr)
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
	_, _ = fmt.Fprintf(w, "Serve mode: cluster (coordinator enabled, addr=%s)\n", strings.TrimSpace(*coordAddr))
	if len(peerFlags) > 0 {
		_, _ = fmt.Fprintf(w, "Configured peers: %s\n", strings.Join(peerFlags, ", "))
	}
	_, _ = fmt.Fprintf(w, "coordinator heartbeat sink enabled: %s\n", strings.TrimSpace(*coordAddr))
	_, _ = fmt.Fprintln(w, "Press Ctrl+C to stop")

	ctx, cancel := notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	_, _ = fmt.Fprintln(w, "\nShutting down...")
	return nil
}

func resolveTransportPeersFromConfig(cfg *config.File, scope string, snapshot map[uint64]localmeta.RegionMeta, localStoreID uint64, explicit map[uint64]string) (map[uint64]string, error) {
	if cfg == nil || localStoreID == 0 {
		return nil, nil
	}
	needed := collectRemotePeers(snapshot, localStoreID)
	if len(needed) == 0 {
		return nil, nil
	}
	out := make(map[uint64]string, len(needed))
	for peerID, storeID := range needed {
		if _, ok := explicit[peerID]; ok {
			continue
		}
		addr := strings.TrimSpace(cfg.ResolveStoreAddr(storeID, scope))
		if addr == "" {
			return nil, fmt.Errorf("serve resolve peer %d on store %d: missing store address in config (use --peer to override)", peerID, storeID)
		}
		out[peerID] = addr
	}
	return out, nil
}

func requireExplicitTransportPeers(snapshot map[uint64]localmeta.RegionMeta, localStoreID uint64, explicit map[uint64]string) error {
	for peerID, storeID := range collectRemotePeers(snapshot, localStoreID) {
		if _, ok := explicit[peerID]; ok {
			continue
		}
		return fmt.Errorf("serve missing transport address for remote peer %d on store %d (provide --config or --peer)", peerID, storeID)
	}
	return nil
}

func collectRemotePeers(snapshot map[uint64]localmeta.RegionMeta, localStoreID uint64) map[uint64]uint64 {
	if len(snapshot) == 0 || localStoreID == 0 {
		return nil
	}
	out := make(map[uint64]uint64)
	for _, meta := range snapshot {
		for _, p := range meta.Peers {
			if p.StoreID == 0 || p.PeerID == 0 || p.StoreID == localStoreID {
				continue
			}
			out[p.PeerID] = p.StoreID
		}
	}
	return out
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
