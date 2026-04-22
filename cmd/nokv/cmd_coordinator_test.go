package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/config"
	pdtestcluster "github.com/feichai0017/NoKV/coordinator/testcluster"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootserver "github.com/feichai0017/NoKV/meta/root/server"
	"github.com/feichai0017/NoKV/raftstore/descriptor"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// newReplicatedMetaRoot spins up the canonical 3-peer replicated meta-root
// via coordinator/testcluster and exposes each peer on a random TCP port via
// the meta/root/server gRPC service. The returned target map is what a
// coordinator CLI invocation expects to receive as --root-peer.
func newReplicatedMetaRoot(t *testing.T) (targets map[uint64]string, leaderRoot rootserver.Backend, cleanup func()) {
	t.Helper()
	cluster := pdtestcluster.OpenReplicated(t)
	leaderID := cluster.WaitLeader()

	servers := make([]*grpc.Server, 0, len(cluster.Roots))
	listeners := make([]net.Listener, 0, len(cluster.Roots))
	targets = make(map[uint64]string, len(cluster.Roots))
	for id, root := range cluster.Roots {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		server := grpc.NewServer()
		rootserver.Register(server, root)
		go func() { _ = server.Serve(lis) }()
		servers = append(servers, server)
		listeners = append(listeners, lis)
		targets[id] = lis.Addr().String()
	}

	cleanup = func() {
		for _, s := range servers {
			s.GracefulStop()
		}
		for _, l := range listeners {
			_ = l.Close()
		}
	}
	return targets, cluster.Roots[leaderID], cleanup
}

func rootPeerArgsFromTargets(targets map[uint64]string) []string {
	args := make([]string, 0, 2*len(targets))
	for id, addr := range targets {
		args = append(args, "-root-peer", formatRootPeer(id, addr))
	}
	return args
}

func formatRootPeer(id uint64, addr string) string {
	return itoaUint(id) + "=" + addr
}

func itoaUint(v uint64) string {
	const digits = "0123456789"
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	return string(buf[i:])
}

func TestRunCoordinatorCmdParseError(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{"-bad-flag"})
	require.Error(t, err)
}

func TestRunCoordinatorCmdRequiresCoordinatorID(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-root-peer", "1=127.0.0.1:2380",
		"-root-peer", "2=127.0.0.1:2381",
		"-root-peer", "3=127.0.0.1:2382",
	})
	require.ErrorContains(t, err, "requires --coordinator-id")
}

func TestRunCoordinatorCmdRequiresThreeRootPeers(t *testing.T) {
	var buf bytes.Buffer
	err := runCoordinatorCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-coordinator-id", "c1",
		"-root-peer", "1=127.0.0.1:2380",
		"-root-peer", "2=127.0.0.1:2381",
	})
	require.ErrorContains(t, err, "requires exactly 3 --root-peer")
}

func TestRunCoordinatorCmdStartsAndStops(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	targets, _, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

	var buf bytes.Buffer
	args := append([]string{
		"-addr", "127.0.0.1:0",
		"-metrics-addr", "127.0.0.1:0",
		"-coordinator-id", "c1",
	}, rootPeerArgsFromTargets(targets)...)
	require.NoError(t, runCoordinatorCmd(&buf, args))
	require.Contains(t, buf.String(), "Coordinator service listening on")
	require.Contains(t, buf.String(), "Coordinator metrics endpoint listening on http://")
	require.Contains(t, buf.String(), "Coordinator lease owner: id=c1")
}

func TestRunCoordinatorCmdRestoresRegionsFromRemoteRoot(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	targets, leaderRoot, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

	_, err := leaderRoot.Append(context.Background(), rootevent.RegionBootstrapped(testDescriptor(41, []byte("a"), []byte("z"), metaregion.Epoch{
		Version:     1,
		ConfVersion: 1,
	})))
	require.NoError(t, err)

	var buf bytes.Buffer
	args := append([]string{
		"-addr", "127.0.0.1:0",
		"-coordinator-id", "c1",
		"-lease-ttl", "15s",
		"-lease-renew-before", "5s",
	}, rootPeerArgsFromTargets(targets)...)
	require.NoError(t, runCoordinatorCmd(&buf, args))
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

	targets, _, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

	var buf bytes.Buffer
	args := append([]string{
		"-addr", "127.0.0.1:0",
		"-coordinator-id", "c1",
		"-metrics-addr", "bad",
	}, rootPeerArgsFromTargets(targets)...)
	err := runCoordinatorCmd(&buf, args)
	require.ErrorContains(t, err, "start coordinator metrics endpoint")
}

func TestRunCoordinatorCmdResolvesAddrFromConfig(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	targets, _, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

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
	args := append([]string{
		"-config", cfgPath,
		"-coordinator-id", "c1",
	}, rootPeerArgsFromTargets(targets)...)
	require.NoError(t, runCoordinatorCmd(&buf, args))
	require.Contains(t, buf.String(), "Coordinator service listening on")
}

func TestMainCoordinatorCommand(t *testing.T) {
	origNotify := coordinatorNotifyContext
	coordinatorNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { coordinatorNotifyContext = origNotify })

	targets, _, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = append([]string{
			"nokv", "coordinator",
			"-addr", "127.0.0.1:0",
			"-coordinator-id", "c1",
		}, rootPeerArgsFromTargets(targets)...)
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
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
