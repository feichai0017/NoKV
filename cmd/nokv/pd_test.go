package main

import (
	"bytes"
	"context"
	"encoding/json"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/config"
	"github.com/feichai0017/NoKV/pd/core"
	pdstorage "github.com/feichai0017/NoKV/pd/storage"

	"github.com/stretchr/testify/require"
)

func TestRunPDCmdParseError(t *testing.T) {
	var buf bytes.Buffer
	err := runPDCmd(&buf, []string{"-bad-flag"})
	require.Error(t, err)
}

func TestRunPDCmdStartsAndStops(t *testing.T) {
	origNotify := pdNotifyContext
	pdNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { pdNotifyContext = origNotify })

	var buf bytes.Buffer
	require.NoError(t, runPDCmd(&buf, []string{"-addr", "127.0.0.1:0", "-metrics-addr", "127.0.0.1:0"}))
	require.Contains(t, buf.String(), "PD-lite service listening on")
	require.Contains(t, buf.String(), "PD metrics endpoint listening on http://")
}

func TestRunPDCmdStartsAndStopsWithReplicatedRoot(t *testing.T) {
	origNotify := pdNotifyContext
	pdNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { pdNotifyContext = origNotify })

	var buf bytes.Buffer
	require.NoError(t, runPDCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-mode", "replicated",
		"-root-node-id", "1",
		"-root-transport-addr", "127.0.0.1:0",
		"-root-peer", "1=127.0.0.1:7001",
		"-root-peer", "2=127.0.0.1:7002",
		"-root-peer", "3=127.0.0.1:7003",
	}))
	require.Contains(t, buf.String(), "PD-lite service listening on")
	require.Contains(t, buf.String(), "PD metadata root mode: replicated")
}

func TestRunPDCmdInvalidMetricsAddr(t *testing.T) {
	origNotify := pdNotifyContext
	pdNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { pdNotifyContext = origNotify })

	var buf bytes.Buffer
	err := runPDCmd(&buf, []string{"-addr", "127.0.0.1:0", "-metrics-addr", "bad"})
	require.ErrorContains(t, err, "start pd metrics endpoint")
}

func TestRunPDCmdRejectsInvalidRootMode(t *testing.T) {
	var buf bytes.Buffer
	err := runPDCmd(&buf, []string{"-addr", "127.0.0.1:0", "-root-mode", "bad"})
	require.ErrorContains(t, err, "invalid root mode")
}

func TestRunPDCmdReplicatedRootRequiresTransportAddress(t *testing.T) {
	var buf bytes.Buffer
	err := runPDCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-mode", "replicated",
		"-root-peer", "1=127.0.0.1:7001",
		"-root-peer", "2=127.0.0.1:7002",
		"-root-peer", "3=127.0.0.1:7003",
	})
	require.ErrorContains(t, err, "requires transport address")
}

func TestRunPDCmdReplicatedRootRequiresThreePeers(t *testing.T) {
	var buf bytes.Buffer
	err := runPDCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-mode", "replicated",
		"-root-transport-addr", "127.0.0.1:0",
		"-root-peer", "1=127.0.0.1:7001",
		"-root-peer", "2=127.0.0.1:7002",
	})
	require.ErrorContains(t, err, "requires exactly 3 peer addresses")
}

func TestMainPDCommand(t *testing.T) {
	origNotify := pdNotifyContext
	pdNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { pdNotifyContext = origNotify })

	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "pd", "-addr", "127.0.0.1:0"}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
}

func TestRestorePDRegionsFromLocalSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := pdstorage.OpenRootLocalStore(dir)
	require.NoError(t, err)
	require.NoError(t, store.PublishRegionDescriptor(testDescriptor(10, []byte("a"), []byte("m"), metaregion.Epoch{
		Version:     1,
		ConfVersion: 1,
	})))
	require.NoError(t, store.PublishRegionDescriptor(testDescriptor(20, []byte("m"), nil, metaregion.Epoch{
		Version:     1,
		ConfVersion: 1,
	})))
	snapshotState, err := store.Load()
	require.NoError(t, err)
	require.NoError(t, store.Close())

	cluster := core.NewCluster()
	loaded, err := pdstorage.RestoreDescriptors(cluster, snapshotState.Descriptors)
	require.NoError(t, err)
	require.Equal(t, 2, loaded)

	desc, ok := cluster.GetRegionDescriptorByKey([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(10), desc.RegionID)
	desc, ok = cluster.GetRegionDescriptorByKey([]byte("z"))
	require.True(t, ok)
	require.Equal(t, uint64(20), desc.RegionID)
}

func TestRunPDCmdReloadsPersistedRegionCatalog(t *testing.T) {
	origNotify := pdNotifyContext
	pdNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { pdNotifyContext = origNotify })

	dir := t.TempDir()
	store, err := pdstorage.OpenRootLocalStore(dir)
	require.NoError(t, err)
	require.NoError(t, store.PublishRegionDescriptor(testDescriptor(31, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1})))
	require.NoError(t, store.PublishRegionDescriptor(testDescriptor(32, []byte("m"), nil, metaregion.Epoch{Version: 3, ConfVersion: 2})))
	require.NoError(t, store.Close())

	var buf bytes.Buffer
	require.NoError(t, runPDCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-workdir", dir,
	}))
	require.Contains(t, buf.String(), "PD restored 2 region(s) from metadata root")
}

func TestRestorePDRegionsRejectsDivergentOverlap(t *testing.T) {
	cluster := core.NewCluster()
	snapshot := map[uint64]descriptor.Descriptor{
		10: {
			RegionID: 10,
			StartKey: []byte("a"),
			EndKey:   []byte("m"),
			Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		},
		20: {
			RegionID: 20,
			StartKey: []byte("l"),
			EndKey:   []byte("z"),
			Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		},
	}

	loaded, err := pdstorage.RestoreDescriptors(cluster, snapshot)
	require.Error(t, err)
	require.Equal(t, 1, loaded)
	desc, ok := cluster.GetRegionDescriptorByKey([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(10), desc.RegionID)
	_, ok = cluster.GetRegionDescriptorByKey([]byte("x"))
	require.False(t, ok)
}

func TestPDRootStoreSaveAndLoadAllocatorState(t *testing.T) {
	store, err := pdstorage.OpenRootLocalStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(0), snapshot.Allocator.IDCurrent)
	require.Equal(t, uint64(0), snapshot.Allocator.TSCurrent)

	require.NoError(t, store.SaveAllocatorState(123, 456))
	snapshot, err = store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(123), snapshot.Allocator.IDCurrent)
	require.Equal(t, uint64(456), snapshot.Allocator.TSCurrent)
}

func TestResolveAllocatorStarts(t *testing.T) {
	id, ts := pdstorage.ResolveAllocatorStarts(1, 100, pdstorage.AllocatorState{
		IDCurrent: 50,
		TSCurrent: 20,
	})
	require.Equal(t, uint64(51), id)
	require.Equal(t, uint64(100), ts)

	id, ts = pdstorage.ResolveAllocatorStarts(80, 30, pdstorage.AllocatorState{
		IDCurrent: 50,
		TSCurrent: 20,
	})
	require.Equal(t, uint64(80), id)
	require.Equal(t, uint64(30), ts)
}

func TestRunPDCmdResolvesWorkdirFromConfig(t *testing.T) {
	origNotify := pdNotifyContext
	pdNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { pdNotifyContext = origNotify })

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "raft_config.json")
	cfg := &config.File{
		MaxRetries: 3,
		PD: &config.PD{
			Addr:       "127.0.0.1:2379",
			WorkDir:    filepath.Join(cfgDir, "pd"),
			DockerAddr: "nokv-pd:2379",
		},
		Stores: []config.Store{{StoreID: 1, Addr: "127.0.0.1:20170", ListenAddr: "127.0.0.1:20170"}},
		Regions: []config.Region{{
			ID:            1,
			LeaderStoreID: 1,
			Epoch:         config.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:         []config.Peer{{StoreID: 1, PeerID: 101}},
		}},
	}
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))

	var buf bytes.Buffer
	require.NoError(t, runPDCmd(&buf, []string{"-addr", "127.0.0.1:0", "-config", cfgPath}))
	require.Contains(t, buf.String(), "PD restored 0 region(s) from metadata root")
	require.DirExists(t, cfg.PD.WorkDir)
	store, err := pdstorage.OpenRootLocalStore(cfg.PD.WorkDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
}

func TestRunPDCmdResolvesAddrFromConfig(t *testing.T) {
	origNotify := pdNotifyContext
	pdNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { pdNotifyContext = origNotify })

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "raft_config.json")
	cfg := &config.File{
		MaxRetries: 3,
		PD: &config.PD{
			Addr: "127.0.0.1:0",
		},
		Stores: []config.Store{{StoreID: 1, Addr: "127.0.0.1:20170", ListenAddr: "127.0.0.1:20170"}},
		Regions: []config.Region{{
			ID:            1,
			LeaderStoreID: 1,
			Epoch:         config.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:         []config.Peer{{StoreID: 1, PeerID: 101}},
		}},
	}
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))

	var buf bytes.Buffer
	require.NoError(t, runPDCmd(&buf, []string{"-config", cfgPath}))
	require.Contains(t, buf.String(), "PD-lite service listening on")
}

func TestRunPDCmdExplicitAddrOverridesConfig(t *testing.T) {
	origNotify := pdNotifyContext
	pdNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { pdNotifyContext = origNotify })

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "raft_config.json")
	cfg := &config.File{
		MaxRetries: 3,
		PD: &config.PD{
			Addr: "127.0.0.1:1",
		},
		Stores: []config.Store{{StoreID: 1, Addr: "127.0.0.1:20170", ListenAddr: "127.0.0.1:20170"}},
		Regions: []config.Region{{
			ID:            1,
			LeaderStoreID: 1,
			Epoch:         config.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:         []config.Peer{{StoreID: 1, PeerID: 101}},
		}},
	}
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, raw, 0o600))

	var buf bytes.Buffer
	require.NoError(t, runPDCmd(&buf, []string{"-addr", "127.0.0.1:0", "-config", cfgPath}))
	require.Contains(t, buf.String(), "PD-lite service listening on")
}

func testDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID:  id,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     epoch,
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}
