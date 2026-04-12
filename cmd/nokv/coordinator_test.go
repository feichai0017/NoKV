package main

import (
	"bytes"
	"context"
	"encoding/json"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootremote "github.com/feichai0017/NoKV/meta/root/remote"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/config"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	pdstorage "github.com/feichai0017/NoKV/coordinator/storage"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestRunCoordinatorCmdParseError(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{"-bad-flag"})
	require.Error(t, err)
}

func TestRunCoordinatorCmdStartsAndStops(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	var buf bytes.Buffer
	require.NoError(t, runCoordinatorCmd(&buf, []string{"-addr", "127.0.0.1:0", "-metrics-addr", "127.0.0.1:0"}))
	require.Contains(t, buf.String(), "Coordinator service listening on")
	require.Contains(t, buf.String(), "Coordinator metrics endpoint listening on http://")
}

func TestRunCoordinatorCmdStartsAndStopsWithReplicatedRoot(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	var buf bytes.Buffer
	require.NoError(t, runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-mode", "replicated",
		"-workdir", t.TempDir(),
		"-root-node-id", "1",
		"-root-transport-addr", "127.0.0.1:0",
		"-root-peer", "1=127.0.0.1:7001",
		"-root-peer", "2=127.0.0.1:7002",
		"-root-peer", "3=127.0.0.1:7003",
	}))
	require.Contains(t, buf.String(), "Coordinator service listening on")
	require.Contains(t, buf.String(), "Coordinator metadata root mode: replicated")
}

func TestRunCoordinatorCmdStartsAndStopsWithRemoteRoot(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	_, err = backend.Append(rootevent.RegionBootstrapped(testDescriptor(41, []byte("a"), []byte("z"), metaregion.Epoch{
		Version:     1,
		ConfVersion: 1,
	})))
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	server := grpc.NewServer()
	metapb.RegisterMetadataRootServer(server, rootremote.NewService(backend))
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.GracefulStop)

	var buf bytes.Buffer
	require.NoError(t, runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-mode", "remote",
		"-root-peer", "1=" + lis.Addr().String(),
		"-coordinator-id", "c1",
		"-lease-ttl", "15s",
		"-lease-renew-before", "5s",
	}))
	require.Contains(t, buf.String(), "Coordinator service listening on")
	require.Contains(t, buf.String(), "Coordinator metadata root mode: remote")
	require.Contains(t, buf.String(), "Coordinator restored 1 region(s) from remote metadata root")
	require.Contains(t, buf.String(), "Coordinator lease owner: id=c1 ttl=15s renew_before=5s")
}

func TestRunCoordinatorCmdInvalidMetricsAddr(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{"-addr", "127.0.0.1:0", "-metrics-addr", "bad"})
	require.ErrorContains(t, err, "start coordinator metrics endpoint")
}

func TestRunCoordinatorCmdRejectsInvalidRootMode(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{"-addr", "127.0.0.1:0", "-root-mode", "bad"})
	require.ErrorContains(t, err, "invalid root mode")
}

func TestRunCoordinatorCmdReplicatedRootRequiresTransportAddress(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-mode", "replicated",
		"-workdir", t.TempDir(),
		"-root-peer", "1=127.0.0.1:7001",
		"-root-peer", "2=127.0.0.1:7002",
		"-root-peer", "3=127.0.0.1:7003",
	})
	require.ErrorContains(t, err, "requires transport address")
}

func TestRunCoordinatorCmdRemoteRootRequiresPeers(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-mode", "remote",
		"-coordinator-id", "c1",
	})
	require.ErrorContains(t, err, "requires at least one target")
}

func TestRunCoordinatorCmdRemoteRootRequiresCoordinatorID(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-mode", "remote",
		"-root-peer", "1=127.0.0.1:2380",
	})
	require.ErrorContains(t, err, "requires --coordinator-id")
}

func TestRunCoordinatorCmdLeaseFlagsRequireCoordinatorID(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-lease-ttl", "12s",
	})
	require.ErrorContains(t, err, "lease flags require --coordinator-id")
}

func TestRunCoordinatorCmdCoordinatorIDRequiresRootedStorage(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-coordinator-id", "c1",
	})
	require.ErrorContains(t, err, "requires rooted storage")
}

func TestRunCoordinatorCmdReplicatedRootRequiresThreePeers(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-mode", "replicated",
		"-workdir", t.TempDir(),
		"-root-transport-addr", "127.0.0.1:0",
		"-root-peer", "1=127.0.0.1:7001",
		"-root-peer", "2=127.0.0.1:7002",
	})
	require.ErrorContains(t, err, "requires exactly 3 peer addresses")
}

func TestRunCoordinatorCmdReplicatedRootRequiresWorkdir(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-mode", "replicated",
		"-root-transport-addr", "127.0.0.1:0",
		"-root-peer", "1=127.0.0.1:7001",
		"-root-peer", "2=127.0.0.1:7002",
		"-root-peer", "3=127.0.0.1:7003",
	})
	require.ErrorContains(t, err, "requires workdir")
}

func TestMainCoordinatorCommand(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "coordinator", "-addr", "127.0.0.1:0"}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
}

func TestRestoreCoordinatorRegionsFromLocalSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := pdstorage.OpenRootLocalStore(dir)
	require.NoError(t, err)
	require.NoError(t, store.AppendRootEvent(rootevent.RegionBootstrapped(testDescriptor(10, []byte("a"), []byte("m"), metaregion.Epoch{
		Version:     1,
		ConfVersion: 1,
	}))))
	require.NoError(t, store.AppendRootEvent(rootevent.RegionBootstrapped(testDescriptor(20, []byte("m"), nil, metaregion.Epoch{
		Version:     1,
		ConfVersion: 1,
	}))))
	snapshotState, err := store.Load()
	require.NoError(t, err)
	require.NoError(t, store.Close())

	cluster := catalog.NewCluster()
	loaded, err := pdstorage.RestoreDescriptors(cluster.PublishRegionDescriptor, snapshotState.Descriptors)
	require.NoError(t, err)
	require.Equal(t, 2, loaded)

	desc, ok := cluster.GetRegionDescriptorByKey([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(10), desc.RegionID)
	desc, ok = cluster.GetRegionDescriptorByKey([]byte("z"))
	require.True(t, ok)
	require.Equal(t, uint64(20), desc.RegionID)
}

func TestRunCoordinatorCmdReloadsPersistedRegionCatalog(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	dir := t.TempDir()
	store, err := pdstorage.OpenRootLocalStore(dir)
	require.NoError(t, err)
	require.NoError(t, store.AppendRootEvent(rootevent.RegionBootstrapped(testDescriptor(31, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}))))
	require.NoError(t, store.AppendRootEvent(rootevent.RegionBootstrapped(testDescriptor(32, []byte("m"), nil, metaregion.Epoch{Version: 3, ConfVersion: 2}))))
	require.NoError(t, store.Close())

	var buf bytes.Buffer
	require.NoError(t, runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-workdir", dir,
	}))
	require.Contains(t, buf.String(), "Coordinator restored 2 region(s) from metadata root")
}

func TestRestoreCoordinatorRegionsRejectsDivergentOverlap(t *testing.T) {
	cluster := catalog.NewCluster()
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

	loaded, err := pdstorage.RestoreDescriptors(cluster.PublishRegionDescriptor, snapshot)
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

func TestRunCoordinatorCmdResolvesWorkdirFromConfig(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "raft_config.json")
	cfg := &config.File{
		MaxRetries: 3,
		Coordinator: &config.Coordinator{
			Addr:       "127.0.0.1:2379",
			WorkDir:    filepath.Join(cfgDir, "coordinator"),
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
	require.NoError(t, runCoordinatorCmd(&buf, []string{"-addr", "127.0.0.1:0", "-config", cfgPath}))
	require.Contains(t, buf.String(), "Coordinator restored 0 region(s) from metadata root")
	require.DirExists(t, cfg.Coordinator.WorkDir)
	store, err := pdstorage.OpenRootLocalStore(cfg.Coordinator.WorkDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
}

func TestRunCoordinatorCmdResolvesAddrFromConfig(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "raft_config.json")
	cfg := &config.File{
		MaxRetries: 3,
		Coordinator: &config.Coordinator{
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
	require.NoError(t, runCoordinatorCmd(&buf, []string{"-config", cfgPath}))
	require.Contains(t, buf.String(), "Coordinator service listening on")
}

func TestRunCoordinatorCmdExplicitAddrOverridesConfig(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "raft_config.json")
	cfg := &config.File{
		MaxRetries: 3,
		Coordinator: &config.Coordinator{
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
	require.NoError(t, runCoordinatorCmd(&buf, []string{"-addr", "127.0.0.1:0", "-config", cfgPath}))
	require.Contains(t, buf.String(), "Coordinator service listening on")
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
