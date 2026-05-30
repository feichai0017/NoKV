// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build rust_raftstore

package client

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	adminclient "github.com/feichai0017/NoKV/raftstore/admin"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

func TestRustRaftstoreEndpointClientAtomicMutateGetAndWatch(t *testing.T) {
	addr := startRustRaftstoreEndpoint(t)
	meta := rustRaftstoreSingleRegion()
	cli, err := New(Config{
		RegionResolver: &mockRegionResolver{region: meta},
		StoreResolver: staticStoreResolver{{
			StoreID: 1,
			Addr:    addr,
			State:   coordpb.StoreState_STORE_STATE_UP,
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:       RetryPolicy{MaxAttempts: 1},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	watch, err := kvrpcpb.NewStoreKVClient(conn).WatchApply(ctx, &kvrpcpb.ApplyWatchRequest{
		KeyPrefix: []byte("agent/"),
		Buffer:    8,
	})
	require.NoError(t, err)

	handled, err := cli.TryAtomicMutate(ctx, []byte("agent/k"), []*kvrpcpb.AtomicPredicate{{
		Key:         []byte("agent/k"),
		Kind:        kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		ReadVersion: 9,
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("agent/k"),
		Value: []byte("v1"),
	}}, 8, 10)
	require.NoError(t, err)
	require.True(t, handled)

	got, err := cli.Get(ctx, []byte("agent/k"), 10)
	require.NoError(t, err)
	require.False(t, got.GetNotFound())
	require.Equal(t, []byte("v1"), got.GetValue())

	event, err := watch.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(1), event.GetEvent().GetRegionId())
	require.Equal(t, uint64(10), event.GetEvent().GetCommitVersion())
	require.Equal(t, [][]byte{[]byte("agent/k")}, event.GetEvent().GetKeys())

	admin, closeAdmin, err := adminclient.Dial(ctx, addr)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeAdmin()) })
	status, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.True(t, status.GetKnown())
	require.True(t, status.GetHosted())
	require.True(t, status.GetLeader())
	require.Equal(t, uint64(1), status.GetAppliedIndex())
}

func startRustRaftstoreEndpoint(t *testing.T) string {
	t.Helper()
	addr := reserveLocalAddr(t)
	root := findRepoRoot(t)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(
		ctx,
		"cargo",
		"run",
		"--quiet",
		"--manifest-path",
		filepath.Join(root, "raftstore-rs", "Cargo.toml"),
		"-p",
		"nokv-raftstore-server",
	)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "NOKV_RUST_RAFTSTORE_ADDR="+addr)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})
	go logPipe(t, "raftstore-rs stdout", stdout)
	go logPipe(t, "raftstore-rs stderr", stderr)
	waitForTCP(t, addr, 15*time.Second)
	return addr
}

func rustRaftstoreSingleRegion() *metapb.RegionDescriptor {
	return &metapb.RegionDescriptor{
		RegionId: 1,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 1}},
	}
}

func reserveLocalAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "raftstore-rs", "Cargo.toml")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root not found")
		}
		dir = parent
	}
}

func waitForTCP(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rust raftstore endpoint %s did not become ready: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func logPipe(t *testing.T, label string, pipe interface{ Read([]byte) (int, error) }) {
	t.Helper()
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		t.Logf("%s: %s", label, scanner.Text())
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Logf("%s read error: %v", label, err)
	}
}
