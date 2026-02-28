package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/config"
	"github.com/feichai0017/NoKV/manifest"
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
	require.NoError(t, runPDCmd(&buf, []string{"-addr", "127.0.0.1:0"}))
	require.Contains(t, buf.String(), "PD-lite service listening on")
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

func TestRestorePDRegionsFromManifestSnapshot(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	require.NoError(t, err)
	require.NoError(t, mgr.LogRegionUpdate(manifest.RegionMeta{
		ID:       10,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch: manifest.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
	}))
	require.NoError(t, mgr.LogRegionUpdate(manifest.RegionMeta{
		ID:       20,
		StartKey: []byte("m"),
		EndKey:   nil,
		Epoch: manifest.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
	}))
	snapshot := mgr.RegionSnapshot()
	require.NoError(t, mgr.Close())

	cluster := core.NewCluster()
	loaded, err := restorePDRegions(cluster, snapshot)
	require.NoError(t, err)
	require.Equal(t, 2, loaded)

	meta, ok := cluster.GetRegionByKey([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(10), meta.ID)
	meta, ok = cluster.GetRegionByKey([]byte("z"))
	require.True(t, ok)
	require.Equal(t, uint64(20), meta.ID)
}

func TestPDLocalStoreSaveAndLoadAllocatorState(t *testing.T) {
	store, err := pdstorage.OpenLocalStore(t.TempDir(), nil)
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
	require.Contains(t, buf.String(), "PD restored 0 region(s) from manifest")
	require.FileExists(t, filepath.Join(cfg.PD.WorkDir, "CURRENT"))
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
