// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build rust_raftstore

package integration

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/model"
	fsmetaraftstore "github.com/feichai0017/NoKV/fsmeta/runtime/raftstore"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRustRaftstoreEndpointFSMetaContract(t *testing.T) {
	for _, tc := range []struct {
		name    string
		holtDir string
	}{
		{name: "memory"},
		{name: "holt", holtDir: t.TempDir()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			steps := envInt("NOKV_RUST_RAFTSTORE_FSMETA_STEPS", 32)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			addr := startRustRaftstoreEndpoint(t, tc.holtDir)
			region := rustRaftstoreSingleRegion()
			kv, err := client.New(client.Config{
				RegionResolver: &staticRegionResolver{regions: []*metapb.RegionDescriptor{region}},
				StoreResolver:  rustStoreResolver{addr: addr},
				DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
				Retry:          client.RetryPolicy{MaxAttempts: 1},
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, kv.Close()) })

			runner, err := fsmetaraftstore.NewRunner(kv, &rustTSO{next: 100})
			require.NoError(t, err)
			seedRootInode(t, ctx, runner, model.MountIdentity{MountID: "vol", MountKeyID: 1})
			contractModel := fsmetacontract.NewModel("vol")
			executor, err := fsmetaexec.New(
				runner,
				fsmetaexec.WithMountResolver(rustMountResolver{}),
				fsmetaexec.WithInodeAllocator(&rustInodeAllocator{next: 10}),
				fsmetaexec.WithClock(func() time.Time { return time.Unix(0, contractModel.NowUnixNs) }),
			)
			require.NoError(t, err)
			mapped, err := fsmetacontract.NewInodeMappingExecutor(executor)
			require.NoError(t, err)

			require.NoError(t, fsmetacontract.Run(ctx, mapped, contractModel, fsmetacontract.GenerateScript(1, steps)))
		})
	}
}

type rustStoreResolver struct {
	addr string
}

func (r rustStoreResolver) GetStore(_ context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error) {
	if req.GetStoreId() != 1 {
		return &coordpb.GetStoreResponse{NotFound: true}, nil
	}
	return &coordpb.GetStoreResponse{
		Store: &coordpb.StoreInfo{
			StoreId:    1,
			ClientAddr: r.addr,
			State:      coordpb.StoreState_STORE_STATE_UP,
		},
	}, nil
}

type rustTSO struct {
	next uint64
}

func (t *rustTSO) Tso(_ context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	end := atomic.AddUint64(&t.next, count)
	return &coordpb.TsoResponse{Timestamp: end - count + 1, Count: count}, nil
}

type rustMountResolver struct{}

func (rustMountResolver) ResolveMount(context.Context, model.MountID) (fsmetaexec.MountAdmission, error) {
	return fsmetaexec.MountAdmission{
		MountID:       "vol",
		MountKeyID:    1,
		RootInode:     model.RootInode,
		SchemaVersion: 1,
	}, nil
}

type rustInodeAllocator struct {
	mu   sync.Mutex
	next model.InodeID
}

func (a *rustInodeAllocator) AllocateCreateInode(context.Context, model.MountIdentity, model.InodeID, string) (model.InodeID, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	inode := a.next
	a.next++
	return inode, nil
}

func startRustRaftstoreEndpoint(t *testing.T, holtDir string) string {
	t.Helper()
	addr := reserveRustEndpointAddr(t)
	root := findRustEndpointRepoRoot(t)
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
	t.Cleanup(func() {
		stopOnce.Do(func() {
			cancel()
			_ = cmd.Wait()
		})
	})
	go logRustEndpointPipe(t, "raftstore-rs stdout", stdout)
	go logRustEndpointPipe(t, "raftstore-rs stderr", stderr)
	waitForRustEndpointTCP(t, addr, 15*time.Second)
	return addr
}

func rustRaftstoreSingleRegion() *metapb.RegionDescriptor {
	return &metapb.RegionDescriptor{
		RegionId: 1,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 1}},
	}
}

func reserveRustEndpointAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func findRustEndpointRepoRoot(t *testing.T) string {
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

func waitForRustEndpointTCP(t *testing.T, addr string, timeout time.Duration) {
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

func logRustEndpointPipe(t *testing.T, label string, pipe interface{ Read([]byte) (int, error) }) {
	t.Helper()
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		t.Logf("%s: %s", label, scanner.Text())
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Logf("%s read error: %v", label, err)
	}
}
