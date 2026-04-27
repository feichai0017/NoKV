package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/config"
	coordadapter "github.com/feichai0017/NoKV/coordinator/adapter"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	enginekv "github.com/feichai0017/NoKV/engine/kv"
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
	configPath := fs.String("config", "", "optional raft configuration file used to resolve listen/workdir/store transport addresses")
	scope := fs.String("scope", "host", "scope for config-resolved addresses: host|docker")
	coordTimeout := fs.Duration("coordinator-timeout", 2*time.Second, "timeout for coordinator heartbeat RPCs")
	metricsAddr := fs.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
	var storeAddrFlags []string
	fs.Func("store-addr", "remote store transport mapping in the form storeID=address (repeatable)", func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("store address value cannot be empty")
		}
		storeAddrFlags = append(storeAddrFlags, value)
		return nil
	})
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	var cfg *config.File
	scopeNorm := strings.ToLower(strings.TrimSpace(*scope))
	if strings.TrimSpace(*configPath) != "" {
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
	explicitStoreAddrs := make(map[uint64]string, len(storeAddrFlags))
	for _, mapping := range storeAddrFlags {
		parts := strings.SplitN(mapping, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid --store-addr value %q (want storeID=address)", mapping)
		}
		id, err := parseUint(parts[0])
		if err != nil {
			return fmt.Errorf("invalid store id in --store-addr %q: %w", mapping, err)
		}
		if id == 0 {
			return fmt.Errorf("invalid --store-addr value %q (store id must be > 0)", mapping)
		}
		addr := strings.TrimSpace(parts[1])
		if addr == "" {
			return fmt.Errorf("invalid --store-addr value %q (empty address)", mapping)
		}
		explicitStoreAddrs[id] = addr
	}
	if strings.TrimSpace(*coordAddr) == "" {
		return fmt.Errorf("--coordinator-addr is required (coordinator is the only scheduler/control-plane source)")
	}
	if _, err := validateServeMode(*workDir, *storeID); err != nil {
		return err
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
	opt.MemTableEngine = NoKV.MemTableEngineART
	// Auto-detect existing vlog so a workdir written by an older build
	// (or by a future explicit-vlog deployment) is reopen-safe.
	if _, statErr := os.Stat(filepath.Join(*workDir, "vlog")); statErr == nil {
		opt.EnableValueLog = true
	}
	fsmetaInlinePolicy, err := enginekv.NewAlwaysInlinePolicy(enginekv.CFDefault, "fsm\x00")
	if err != nil {
		return fmt.Errorf("configure fsmeta value separation policy: %w", err)
	}
	opt.ValueSeparationPolicies = append(opt.ValueSeparationPolicies, fsmetaInlinePolicy)
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
			StoreID:    *storeID,
			ClientAddr: resolveStoreAdvertiseAddr(cfg, *storeID, scopeNorm),
			RaftAddr:   resolveStoreAdvertiseAddr(cfg, *storeID, scopeNorm),
			LocalMeta:  localMeta,
			WorkDir:    *workDir,
			Scheduler:  coordScheduler,
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
	snapshot := localMeta.Snapshot()
	transportPeers, err := resolveTransportPeers(snapshot, *storeID, cfg, strings.ToLower(strings.TrimSpace(*scope)), explicitStoreAddrs)
	if err != nil {
		return err
	}
	for peerID, addr := range transportPeers {
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
		_, _ = fmt.Fprintln(w, "Serve lifecycle: bootstrap-wait (runtime topology will come from local metadata once seeded)")
	} else {
		_, _ = fmt.Fprintf(w, "Local peer catalog regions: %d, local peers started: %d\n", totalRegions, len(startedRegions))
		_, _ = fmt.Fprintln(w, "Serve lifecycle: restart-recover (runtime topology sourced from local metadata)")
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
	if len(storeAddrFlags) > 0 {
		_, _ = fmt.Fprintf(w, "Configured store address overrides: %s\n", strings.Join(storeAddrFlags, ", "))
	}
	_, _ = fmt.Fprintf(w, "coordinator heartbeat sink enabled: %s\n", strings.TrimSpace(*coordAddr))
	_, _ = fmt.Fprintln(w, "Press Ctrl+C to stop")

	ctx, cancel := notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	_, _ = fmt.Fprintln(w, "\nShutting down...")
	return nil
}

func resolveTransportPeers(snapshot map[uint64]localmeta.RegionMeta, localStoreID uint64, cfg *config.File, scope string, explicitStoreAddrs map[uint64]string) (map[uint64]string, error) {
	needed := collectRemotePeers(snapshot, localStoreID)
	if len(needed) == 0 {
		if len(explicitStoreAddrs) > 0 {
			return nil, fmt.Errorf("serve received --store-addr overrides but local metadata has no remote stores to resolve")
		}
		return nil, nil
	}
	out := make(map[uint64]string, len(needed))
	usedOverrides := make(map[uint64]struct{}, len(explicitStoreAddrs))
	for peerID, storeID := range needed {
		addr := strings.TrimSpace(explicitStoreAddrs[storeID])
		if addr != "" {
			usedOverrides[storeID] = struct{}{}
		} else if cfg != nil {
			addr = strings.TrimSpace(cfg.ResolveStoreAddr(storeID, scope))
		}
		if addr == "" {
			return nil, fmt.Errorf("serve missing transport address for remote store %d (peer %d): provide --config store address or --store-addr override", storeID, peerID)
		}
		out[peerID] = addr
	}
	for storeID := range explicitStoreAddrs {
		if _, ok := usedOverrides[storeID]; ok {
			continue
		}
		return nil, fmt.Errorf("serve unused --store-addr override for store %d: local metadata does not reference that remote store", storeID)
	}
	return out, nil
}

func resolveStoreAdvertiseAddr(cfg *config.File, storeID uint64, scope string) string {
	if cfg == nil || storeID == 0 {
		return ""
	}
	return strings.TrimSpace(cfg.ResolveStoreAddr(storeID, scope))
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

func validateServeMode(workDir string, storeID uint64) (raftmode.State, error) {
	state, err := raftmode.Read(workDir)
	if err != nil {
		return raftmode.State{}, fmt.Errorf("read workdir mode: %w", err)
	}
	if state.StoreID != 0 && storeID != 0 && state.StoreID != storeID {
		return raftmode.State{}, fmt.Errorf("serve store-id mismatch: workdir %q is bound to store %d, not store %d", workDir, state.StoreID, storeID)
	}
	return state, nil
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
	state.RegionID = 0
	state.PeerID = 0
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
