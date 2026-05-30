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
	"sync"
	"testing"
	"time"

	adminclient "github.com/feichai0017/NoKV/raftstore/admin"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

func TestRustRaftstoreEndpointClientAtomicMutateGetAndWatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		holt bool
	}{
		{name: "memory"},
		{name: "holt", holt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startRustRaftstoreEndpoint(t, tc.holt)
			testRustRaftstoreEndpointClientAtomicMutateGetAndWatch(t, addr)
		})
	}
}

func TestRustRaftstoreEndpointHoltApplyStatusSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	addr, stop := startRustRaftstoreProcess(t, dir)

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handled, err := cli.TryAtomicMutate(ctx, []byte("agent/restart"), []*kvrpcpb.AtomicPredicate{{
		Key:         []byte("agent/restart"),
		Kind:        kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		ReadVersion: 9,
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("agent/restart"),
		Value: []byte("v1"),
	}}, 8, 10)
	require.NoError(t, err)
	require.True(t, handled)
	require.NoError(t, cli.Close())

	admin, closeAdmin, err := adminclient.Dial(ctx, addr)
	require.NoError(t, err)
	statusBefore, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.GreaterOrEqual(t, statusBefore.GetAppliedIndex(), uint64(2))
	require.NoError(t, closeAdmin())
	stop()

	addr, _ = startRustRaftstoreProcess(t, dir)
	admin, closeAdmin, err = adminclient.Dial(ctx, addr)
	require.NoError(t, err)
	status, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.GreaterOrEqual(t, status.GetAppliedIndex(), statusBefore.GetAppliedIndex())
	require.NoError(t, closeAdmin())

	cli, err = New(Config{
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
	handled, err = cli.TryAtomicMutate(ctx, []byte("agent/restart2"), []*kvrpcpb.AtomicPredicate{{
		Key:         []byte("agent/restart2"),
		Kind:        kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		ReadVersion: 9,
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("agent/restart2"),
		Value: []byte("v2"),
	}}, 11, 12)
	require.NoError(t, err)
	require.True(t, handled)
	got, err := cli.Get(ctx, []byte("agent/restart2"), 12)
	require.NoError(t, err)
	require.False(t, got.GetNotFound())
	require.Equal(t, []byte("v2"), got.GetValue())
}

func TestRustRaftstoreEndpointClientTransactionSurface(t *testing.T) {
	for _, tc := range []struct {
		name string
		holt bool
	}{
		{name: "memory"},
		{name: "holt", holt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startRustRaftstoreEndpoint(t, tc.holt)
			testRustRaftstoreEndpointClientTransactionSurface(t, addr)
		})
	}
}

func testRustRaftstoreEndpointClientAtomicMutateGetAndWatch(t *testing.T, addr string) {
	t.Helper()
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

	admin, closeAdmin, err := adminclient.Dial(ctx, addr)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeAdmin()) })
	statusAfterWrite, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.GreaterOrEqual(t, statusAfterWrite.GetAppliedIndex(), uint64(2))

	got, err := cli.Get(ctx, []byte("agent/k"), 10)
	require.NoError(t, err)
	require.False(t, got.GetNotFound())
	require.Equal(t, []byte("v1"), got.GetValue())
	statusAfterRead, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.Equal(t, statusAfterWrite.GetAppliedIndex(), statusAfterRead.GetAppliedIndex())

	event, err := watch.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(1), event.GetEvent().GetRegionId())
	require.Equal(t, uint64(10), event.GetEvent().GetCommitVersion())
	require.Equal(t, [][]byte{[]byte("agent/k")}, event.GetEvent().GetKeys())

	handled, err = cli.TryAtomicMutate(ctx, []byte("agent/multi"), nil, []*kvrpcpb.Mutation{
		{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("agent/multi"),
			Value: []byte("v2"),
		},
		{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("other/multi"),
			Value: []byte("ignored"),
		},
	}, 11, 12)
	require.NoError(t, err)
	require.True(t, handled)
	event, err = watch.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(12), event.GetEvent().GetCommitVersion())
	require.Equal(t, [][]byte{[]byte("agent/multi")}, event.GetEvent().GetKeys())

	runtimeStatus, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.True(t, runtimeStatus.GetKnown())
	require.True(t, runtimeStatus.GetHosted())
	require.True(t, runtimeStatus.GetLeader())
	require.GreaterOrEqual(t, runtimeStatus.GetAppliedIndex(), uint64(2))

	_, err = admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	execution, err := admin.ExecutionStatus(ctx, &adminpb.ExecutionStatusRequest{})
	require.NoError(t, err)
	lastAdmission := execution.GetLastAdmission()
	require.NotNil(t, lastAdmission)
	require.True(t, lastAdmission.GetObserved())
	require.True(t, lastAdmission.GetAccepted())
	require.Equal(t, adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_WRITE, lastAdmission.GetClass())
	require.Equal(t, adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_ACCEPTED, lastAdmission.GetReason())
	require.Equal(t, uint64(1), lastAdmission.GetRegionId())
	require.Equal(t, uint64(1), lastAdmission.GetPeerId())
	require.Equal(t, adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_READY, execution.GetRestart().GetState())
	require.Equal(t, uint64(1), execution.GetRestart().GetRegionCount())
	require.Equal(t, uint64(1), execution.GetRestart().GetRaftGroupCount())
}

func testRustRaftstoreEndpointClientTransactionSurface(t *testing.T, addr string) {
	t.Helper()
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

	require.NoError(t, cli.TwoPhaseCommit(ctx, []byte("agent/txn-a"), []*kvrpcpb.Mutation{
		{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("agent/txn-a"),
			Value: []byte("va"),
		},
		{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("agent/txn-b"),
			Value: []byte("vb"),
		},
	}, 20, 30, 60_000))

	got, err := cli.BatchGet(ctx, [][]byte{
		[]byte("agent/txn-a"),
		[]byte("agent/txn-b"),
		[]byte("agent/txn-missing"),
	}, 30)
	require.NoError(t, err)
	require.Equal(t, []byte("va"), got["agent/txn-a"].GetValue())
	require.Equal(t, []byte("vb"), got["agent/txn-b"].GetValue())
	require.True(t, got["agent/txn-missing"].GetNotFound())

	scanned, err := cli.Scan(ctx, []byte("agent/txn-"), 10, 30)
	require.NoError(t, err)
	require.Len(t, scanned, 2)
	require.Equal(t, []byte("agent/txn-a"), scanned[0].GetKey())
	require.Equal(t, []byte("agent/txn-b"), scanned[1].GetKey())

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = kvrpcpb.NewStoreKVClient(conn).Scan(ctx, &kvrpcpb.KvScanRequest{
		Context: &kvrpcpb.Context{
			RegionId:    meta.GetRegionId(),
			RegionEpoch: meta.GetEpoch(),
			Peer:        meta.GetPeers()[0],
		},
		Request: &kvrpcpb.ScanRequest{
			StartKey: []byte("agent/txn-"),
			Limit:    1,
			Reverse:  true,
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))

	install, err := cli.InstallPreparedMVCCEntries(ctx, []byte("agent/prepared"), &kvrpcpb.InstallPreparedMVCCEntriesRequest{
		CommitVersion: 40,
		Entries: []*kvrpcpb.PreparedMVCCEntry{{
			ColumnFamily: kvrpcpb.PreparedMVCCEntry_DEFAULT,
			Key:          []byte("agent/prepared"),
			Version:      40,
			Value:        []byte("prepared"),
			HasValue:     true,
		}},
		WatchKeys: [][]byte{[]byte("agent/prepared")},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), install.GetAppliedEntries())
	require.Equal(t, uint64(40), install.GetCommitVersion())

	prepared, err := cli.Get(ctx, []byte("agent/prepared"), 40)
	require.NoError(t, err)
	require.False(t, prepared.GetNotFound())
	require.Equal(t, []byte("prepared"), prepared.GetValue())
}

func startRustRaftstoreEndpoint(t *testing.T, holt bool) string {
	t.Helper()
	holtDir := ""
	if holt {
		holtDir = t.TempDir()
	}
	addr, _ := startRustRaftstoreProcess(t, holtDir)
	return addr
}

func startRustRaftstoreProcess(t *testing.T, holtDir string) (string, func()) {
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
	if holtDir != "" {
		cmd.Env = append(cmd.Env, "NOKV_RUST_RAFTSTORE_HOLT_DIR="+holtDir)
	}
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			cancel()
			_ = cmd.Wait()
		})
	}
	t.Cleanup(func() {
		stop()
	})
	go logPipe(t, "raftstore-rs stdout", stdout)
	go logPipe(t, "raftstore-rs stderr", stderr)
	waitForTCP(t, addr, 15*time.Second)
	return addr, stop
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
