// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build rust_raftstore

package raftstore

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/tso"
	"github.com/feichai0017/NoKV/fsmeta/model"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	metadatapb "github.com/feichai0017/NoKV/pb/metadata"
)

func TestRustMetadataPlaneFsmetaRuntimeEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	repo := repoRootFromThisFile(t)
	binary := buildRustRaftstoreServer(t, ctx, repo)
	addr := freeTCPAddr(t)
	logs := startRustRaftstoreServer(t, ctx, binary, repo, addr)
	waitForRustMetadataPlane(t, ctx, addr)

	coordinator := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(100), tso.NewAllocator(1000))
	publishRootEvent(t, coordinator, rootevent.StoreJoined(1))
	publishRootEvent(t, coordinator, rootevent.MountRegistered("vol", 1, uint64(model.RootInode), 1))
	publishRootEvent(t, coordinator, rootevent.RegionBootstrapped(testMetadataPlaneDescriptor()))
	heartbeat, err := coordinator.StoreHeartbeat(ctx, &coordpb.StoreHeartbeatRequest{
		StoreId:         1,
		ClientAddr:      addr,
		RaftAddr:        addr,
		RegionNum:       1,
		LeaderNum:       1,
		LeaderRegionIds: []uint64{1},
		RegionStats: []*coordpb.RegionRuntimeStats{{
			RegionId:      1,
			LeaderStoreId: 1,
		}},
	})
	require.NoError(t, err)
	require.True(t, heartbeat.GetAccepted())

	runtime, err := Open(ctx, Options{
		Coordinator:    coordinator,
		DialTimeout:    5 * time.Second,
		BootstrapMount: "vol",
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	created, err := runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact.json",
		Attrs: model.CreateAttrs{
			Type:        model.InodeTypeFile,
			Size:        42,
			Mode:        0o644,
			OpaqueAttrs: []byte(`{"body":"sha256:abc"}`),
		},
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	require.Equal(t, model.InodeTypeFile, created.Inode.Type)
	require.Equal(t, uint64(42), created.Inode.Size)

	lookup, err := runtime.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact.json",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry, lookup.Dentry)
	require.Equal(t, created.Inode.Inode, lookup.Inode.Inode)

	entries, err := runtime.Executor.ReadDirPlus(ctx, model.ReadDirRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Limit:  16,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "artifact.json", entries[0].Dentry.Name)

	removed, err := runtime.Executor.Remove(ctx, model.RemoveRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact.json",
	})
	require.NoError(t, err)
	require.True(t, removed.InodeDeleted)
	require.Equal(t, created.Dentry.Inode, removed.RemovedDentry.Inode)
	require.Equal(t, created.Inode.Inode, removed.OldInode.Inode)

	_, err = runtime.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact.json",
	})
	require.ErrorIs(t, err, model.ErrNotFound)
}

func repoRootFromThisFile(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../.."))
}

func buildRustRaftstoreServer(t *testing.T, ctx context.Context, repo string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "cargo", "build", "--manifest-path", filepath.Join(repo, "raftstore", "Cargo.toml"), "-p", "nokv-raftstore-server")
	cmd.Dir = repo
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "cargo build failed:\n%s", string(output))
	return filepath.Join(repo, "raftstore", "target", "debug", "nokv-raftstore-server")
}

func startRustRaftstoreServer(t *testing.T, ctx context.Context, binary, repo, addr string) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	cmd := exec.CommandContext(ctx, binary)
	cmd.Dir = repo
	cmd.Env = append(os.Environ(),
		"NOKV_RAFTSTORE_ADDR="+addr,
		"NOKV_RAFTSTORE_ADVERTISE_ADDR="+addr,
		"NOKV_RAFTSTORE_REGION_ID=1",
		"NOKV_RAFTSTORE_STORE_ID=1",
		"NOKV_RAFTSTORE_PEER_ID=1",
		"NOKV_RAFTSTORE_BOOTSTRAP=true",
		"NOKV_RAFTSTORE_HOLT_DIR="+filepath.Join(t.TempDir(), "holt"),
		"NOKV_RAFTSTORE_LOG_DIR="+filepath.Join(t.TempDir(), "raftlog"),
	)
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("rust raftstore logs:\n%s", logs.String())
		}
	})
	return &logs
}

func waitForRustMetadataPlane(t *testing.T, parent context.Context, addr string) {
	t.Helper()
	conn, err := grpc.NewClient("dns:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	conn.Connect()
	client := metadatapb.NewMetadataPlaneClient(conn)
	ctx := &metadatapb.MetadataContext{
		RegionId:    1,
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peer:        &metapb.RegionPeer{StoreId: 1, PeerId: 1},
	}
	require.Eventually(t, func() bool {
		callCtx, cancel := context.WithTimeout(parent, time.Second)
		defer cancel()
		resp, err := client.Get(callCtx, &metadatapb.MetadataGetRequest{
			Context: ctx,
			Key:     []byte("__probe"),
			Version: 1,
		})
		return err == nil && resp.GetRegionError() == nil
	}, 20*time.Second, 100*time.Millisecond)
}

func publishRootEvent(t *testing.T, coordinator *coordserver.Service, event rootevent.Event) {
	t.Helper()
	resp, err := coordinator.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
}

func testMetadataPlaneDescriptor() topology.Descriptor {
	desc := topology.Descriptor{
		RegionID: 1,
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers: []metaregion.Peer{{
			StoreID: 1,
			PeerID:  1,
		}},
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, ln.Close()) }()
	return ln.Addr().String()
}
