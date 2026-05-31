// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build rust_raftstore

package raftstore

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
	"github.com/feichai0017/NoKV/fsmeta/contract"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	metadatapb "github.com/feichai0017/NoKV/pb/metadata"
)

func TestRustMetadataPlaneFsmetaRuntimeEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	runtime, logs, coordinator := openRustMetadataPlaneRuntime(t, ctx)
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

	watch, err := runtime.Watcher.Subscribe(ctx, observe.WatchRequest{
		Mount:              "vol",
		RootInode:          model.RootInode,
		BackPressureWindow: 16,
	})
	require.NoError(t, err)
	defer watch.Close()

	removed, err := runtime.Executor.Remove(ctx, model.RemoveRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact.json",
	})
	require.NoError(t, err)
	require.True(t, removed.InodeDeleted)
	require.Equal(t, created.Dentry.Inode, removed.RemovedDentry.Inode)
	require.Equal(t, created.Inode.Inode, removed.OldInode.Inode)
	removeEvent := requireWatchEvent(t, watch)
	require.Equal(t, observe.WatchEventSourceCommit, removeEvent.Source)

	_, err = runtime.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact.json",
	})
	require.ErrorIs(t, err, model.ErrNotFound)

	_, err = runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact2.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 7,
			Mode: 0o644,
		},
	})
	require.NoError(t, err)

	resume, err := runtime.Watcher.Subscribe(ctx, observe.WatchRequest{
		Mount:              "vol",
		RootInode:          model.RootInode,
		ResumeCursor:       removeEvent.Cursor,
		BackPressureWindow: 16,
	})
	require.NoError(t, err)
	defer resume.Close()
	replayed := requireWatchEvent(t, resume)
	require.Equal(t, observe.WatchEventSourceCommit, replayed.Source)
	require.Greater(t, replayed.Cursor.Index, removeEvent.Cursor.Index)

	stage, err := runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "stage.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 11,
			Mode: 0o644,
		},
	})
	require.NoError(t, err)
	final, err := runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "final.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 19,
			Mode: 0o644,
		},
	})
	require.NoError(t, err)
	replace, err := runtime.Executor.RenameReplace(ctx, model.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: model.RootInode,
		FromName:   "stage.json",
		ToParent:   model.RootInode,
		ToName:     "final.json",
	})
	require.NoError(t, err)
	require.True(t, replace.Replaced)
	require.True(t, replace.OldInodeDeleted)
	require.Equal(t, final.Dentry.Inode, replace.OldDentry.Inode)
	require.Equal(t, final.Inode.Inode, replace.OldInode.Inode)
	replacedLookup, err := runtime.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "final.json",
	})
	require.NoError(t, err)
	require.Equal(t, stage.Dentry.Inode, replacedLookup.Dentry.Inode)
	require.Equal(t, stage.Inode.Inode, replacedLookup.Inode.Inode)
	_, err = runtime.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "stage.json",
	})
	require.ErrorIs(t, err, model.ErrNotFound)

	_, err = runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "empty-dir",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeDirectory,
			Mode: 0o755,
		},
	})
	require.NoError(t, err)
	require.NoError(t, runtime.Executor.RemoveDirectory(ctx, model.RemoveDirectoryRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "empty-dir",
	}))
	_, err = runtime.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "empty-dir",
	})
	require.ErrorIs(t, err, model.ErrNotFound)

	_, err = runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "snapshot-before.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 31,
			Mode: 0o644,
		},
	})
	require.NoError(t, err)
	token, err := runtime.Executor.SnapshotSubtree(ctx, model.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
	})
	require.NoError(t, err)
	require.NoError(t, runtime.Snapshot.PublishSnapshotSubtree(ctx, token))
	_, err = runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "snapshot-after.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 37,
			Mode: 0o644,
		},
	})
	require.NoError(t, err)
	snapshotEntries, err := runtime.Executor.ReadDirPlus(ctx, model.ReadDirRequest{
		Mount:           "vol",
		Parent:          model.RootInode,
		Limit:           64,
		SnapshotVersion: token.ReadVersion,
	})
	require.NoError(t, err)
	require.True(t, containsDentryName(snapshotEntries, "snapshot-before.json"))
	require.False(t, containsDentryName(snapshotEntries, "snapshot-after.json"))
	require.NoError(t, runtime.Snapshot.RetireSnapshotSubtree(ctx, token))

	publishRootEvent(t, coordinator, rootevent.MountRetired("vol"))
	_, err = runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "after-retire.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 1,
			Mode: 0o644,
		},
	})
	require.ErrorIs(t, err, model.ErrMountRetired)
}

func TestRustMetadataPlanePassesFSMetaContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	runtime, logs, _ := openRustMetadataPlaneRuntime(t, ctx)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	mapped, err := contract.NewInodeMappingExecutor(runtime.Executor)
	require.NoError(t, err)
	state := contract.NewModel("vol")
	err = contract.Run(ctx, mapped, state, contract.GenerateScript(11, 70))
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
}

func TestRustMetadataPlaneRouteSurvivesCoordinatorRebuild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	repo := repoRootFromThisFile(t)
	binary := buildRustRaftstoreServer(t, ctx, repo)
	addr := freeTCPAddr(t)
	logs := startRustRaftstoreServer(t, ctx, binary, repo, addr)
	waitForRustMetadataPlane(t, ctx, addr)

	rootStore := newE2ERootStorage()
	firstCoordinator := newE2ERootedCoordinator(rootStore)
	publishRootEvent(t, firstCoordinator, rootevent.StoreJoined(1))
	publishRootEvent(t, firstCoordinator, rootevent.MountRegistered("vol", 1, uint64(model.RootInode), 1))
	publishRootEvent(t, firstCoordinator, rootevent.RegionBootstrapped(testMetadataPlaneDescriptor()))
	heartbeatRustStore(t, ctx, firstCoordinator, addr)

	rebuiltCoordinator := newE2ERootedCoordinator(rootStore)
	require.NoError(t, rebuiltCoordinator.ReloadFromStorage())
	heartbeatRustStore(t, ctx, rebuiltCoordinator, addr)

	runtime, err := Open(ctx, Options{
		Coordinator:    rebuiltCoordinator,
		DialTimeout:    5 * time.Second,
		BootstrapMount: "vol",
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	created, err := runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "after-coordinator-rebuild.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 23,
			Mode: 0o644,
		},
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	lookup, err := runtime.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "after-coordinator-rebuild.json",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry.Inode, lookup.Dentry.Inode)
	require.Equal(t, created.Inode.Inode, lookup.Inode.Inode)
}

func TestRustMetadataPlaneSurvivesRaftstoreRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo := repoRootFromThisFile(t)
	binary := buildRustRaftstoreServer(t, ctx, repo)
	addr := freeTCPAddr(t)
	holtDir := filepath.Join(t.TempDir(), "holt")
	raftLogDir := filepath.Join(t.TempDir(), "raftlog")

	first := startRustRaftstoreServerWithDirs(t, ctx, binary, repo, addr, holtDir, raftLogDir)
	waitForRustMetadataPlane(t, ctx, addr)

	rootStore := newE2ERootStorage()
	coordinator := newE2ERootedCoordinator(rootStore)
	publishRootEvent(t, coordinator, rootevent.StoreJoined(1))
	publishRootEvent(t, coordinator, rootevent.MountRegistered("vol", 1, uint64(model.RootInode), 1))
	publishRootEvent(t, coordinator, rootevent.RegionBootstrapped(testMetadataPlaneDescriptor()))
	heartbeatRustStore(t, ctx, coordinator, addr)

	runtime, err := Open(ctx, Options{
		Coordinator:    coordinator,
		DialTimeout:    5 * time.Second,
		BootstrapMount: "vol",
	})
	require.NoError(t, err, "raftstore logs:\n%s", first.logs.String())
	created, err := runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "before-restart.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 55,
			Mode: 0o644,
		},
	})
	require.NoError(t, err, "raftstore logs:\n%s", first.logs.String())
	require.NoError(t, runtime.Close())
	first.stop()

	second := startRustRaftstoreServerWithDirs(t, ctx, binary, repo, addr, holtDir, raftLogDir)
	waitForRustMetadataPlane(t, ctx, addr)
	heartbeatRustStore(t, ctx, coordinator, addr)

	reopened, err := Open(ctx, Options{
		Coordinator:    coordinator,
		DialTimeout:    5 * time.Second,
		BootstrapMount: "vol",
	})
	require.NoError(t, err, "first raftstore logs:\n%s\nsecond raftstore logs:\n%s", first.logs.String(), second.logs.String())
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	lookup, err := reopened.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "before-restart.json",
	})
	require.NoError(t, err, "first raftstore logs:\n%s\nsecond raftstore logs:\n%s", first.logs.String(), second.logs.String())
	require.Equal(t, created.Dentry.Inode, lookup.Dentry.Inode)
	require.Equal(t, created.Inode.Inode, lookup.Inode.Inode)

	after, err := reopened.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "after-restart.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 89,
			Mode: 0o644,
		},
	})
	require.NoError(t, err, "first raftstore logs:\n%s\nsecond raftstore logs:\n%s", first.logs.String(), second.logs.String())
	afterLookup, err := reopened.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "after-restart.json",
	})
	require.NoError(t, err)
	require.Equal(t, after.Dentry.Inode, afterLookup.Dentry.Inode)
}

func TestRustMetadataPlaneRoutesAfterLeaderTransfer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	repo := repoRootFromThisFile(t)
	binary := buildRustRaftstoreServer(t, ctx, repo)
	addr1 := freeTCPAddr(t)
	addr2 := freeTCPAddr(t)
	peerEndpoints := map[uint64]string{1: addr1, 2: addr2}
	_ = startRustRaftstoreServerWithConfig(t, ctx, binary, repo, rustRaftstoreStartConfig{
		addr:          addr1,
		storeID:       1,
		peerID:        1,
		bootstrap:     true,
		peerEndpoints: peerEndpoints,
	})
	peer2 := startRustRaftstoreServerWithConfig(t, ctx, binary, repo, rustRaftstoreStartConfig{
		addr:          addr2,
		storeID:       2,
		peerID:        2,
		bootstrap:     false,
		peerEndpoints: peerEndpoints,
	})
	waitForRustMetadataPlane(t, ctx, addr1)
	waitForRustAdmin(t, ctx, addr2)

	rootStore := newE2ERootStorage()
	coordinator := newE2ERootedCoordinator(rootStore)
	publishRootEvent(t, coordinator, rootevent.StoreJoined(1))
	publishRootEvent(t, coordinator, rootevent.StoreJoined(2))
	publishRootEvent(t, coordinator, rootevent.MountRegistered("vol", 1, uint64(model.RootInode), 1))
	base := testMetadataPlaneDescriptor()
	publishRootEvent(t, coordinator, rootevent.RegionBootstrapped(base))
	heartbeatRustStoreAs(t, ctx, coordinator, 1, addr1, true)
	heartbeatRustStoreAs(t, ctx, coordinator, 2, addr2, false)

	admin1, closeAdmin1 := rustAdminClient(t, addr1)
	defer closeAdmin1()
	addResp, err := admin1.AddPeer(ctx, &adminpb.AddPeerRequest{
		RegionId: 1,
		StoreId:  2,
		PeerId:   2,
	})
	require.NoError(t, err, "peer2 raftstore logs:\n%s", peer2.logs.String())
	require.Equal(t, uint64(2), addResp.GetRegion().GetEpoch().GetConfVersion())
	target := base.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 2})
	target.Epoch.ConfVersion = addResp.GetRegion().GetEpoch().GetConfVersion()
	target.Hash = nil
	target.EnsureHash()
	publishRootEvent(t, coordinator, rootevent.PeerAdded(1, 2, 2, target))
	waitForRustRegionStatus(t, ctx, addr2, func(status *adminpb.RegionRuntimeStatusResponse) bool {
		return status.GetHosted() && len(status.GetRegion().GetPeers()) == 2
	})

	_, err = admin1.TransferLeader(ctx, &adminpb.TransferLeaderRequest{
		RegionId: 1,
		PeerId:   2,
	})
	require.NoError(t, err, "peer2 raftstore logs:\n%s", peer2.logs.String())
	waitForRustRegionStatus(t, ctx, addr2, func(status *adminpb.RegionRuntimeStatusResponse) bool {
		return status.GetLeader() && status.GetLeaderPeerId() == 2
	})
	// Keep the coordinator leader cache stale on store 1. The first fsmeta
	// write must observe NotLeader from peer 1, learn the peer 2 hint, and
	// retry through the same fsmeta runtime.

	runtime, err := Open(ctx, Options{
		Coordinator:    coordinator,
		DialTimeout:    5 * time.Second,
		BootstrapMount: "vol",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	created, err := runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "after-transfer.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 64,
			Mode: 0o644,
		},
	})
	require.NoError(t, err, "peer2 raftstore logs:\n%s", peer2.logs.String())
	lookup, err := runtime.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "after-transfer.json",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry.Inode, lookup.Dentry.Inode)
	require.Equal(t, created.Inode.Inode, lookup.Inode.Inode)
}

func TestRustMetadataPlaneRejectsRemovedPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	repo := repoRootFromThisFile(t)
	binary := buildRustRaftstoreServer(t, ctx, repo)
	addr1 := freeTCPAddr(t)
	addr2 := freeTCPAddr(t)
	peerEndpoints := map[uint64]string{1: addr1, 2: addr2}
	_ = startRustRaftstoreServerWithConfig(t, ctx, binary, repo, rustRaftstoreStartConfig{
		addr:          addr1,
		storeID:       1,
		peerID:        1,
		bootstrap:     true,
		peerEndpoints: peerEndpoints,
	})
	peer2 := startRustRaftstoreServerWithConfig(t, ctx, binary, repo, rustRaftstoreStartConfig{
		addr:          addr2,
		storeID:       2,
		peerID:        2,
		bootstrap:     false,
		peerEndpoints: peerEndpoints,
	})
	waitForRustMetadataPlane(t, ctx, addr1)
	waitForRustAdmin(t, ctx, addr2)

	rootStore := newE2ERootStorage()
	coordinator := newE2ERootedCoordinator(rootStore)
	publishRootEvent(t, coordinator, rootevent.StoreJoined(1))
	publishRootEvent(t, coordinator, rootevent.StoreJoined(2))
	publishRootEvent(t, coordinator, rootevent.MountRegistered("vol", 1, uint64(model.RootInode), 1))
	base := testMetadataPlaneDescriptor()
	publishRootEvent(t, coordinator, rootevent.RegionBootstrapped(base))
	heartbeatRustStoreAs(t, ctx, coordinator, 1, addr1, true)
	heartbeatRustStoreAs(t, ctx, coordinator, 2, addr2, false)

	admin1, closeAdmin1 := rustAdminClient(t, addr1)
	defer closeAdmin1()
	addResp, err := admin1.AddPeer(ctx, &adminpb.AddPeerRequest{
		RegionId: 1,
		StoreId:  2,
		PeerId:   2,
	})
	require.NoError(t, err, "peer2 raftstore logs:\n%s", peer2.logs.String())
	added := base.Clone()
	added.Peers = append(added.Peers, metaregion.Peer{StoreID: 2, PeerID: 2})
	added.Epoch.ConfVersion = addResp.GetRegion().GetEpoch().GetConfVersion()
	added.Hash = nil
	added.EnsureHash()
	publishRootEvent(t, coordinator, rootevent.PeerAdded(1, 2, 2, added))
	waitForRustRegionStatus(t, ctx, addr2, func(status *adminpb.RegionRuntimeStatusResponse) bool {
		return status.GetHosted() && len(status.GetRegion().GetPeers()) == 2
	})

	removeResp, err := admin1.RemovePeer(ctx, &adminpb.RemovePeerRequest{
		RegionId: 1,
		PeerId:   2,
	})
	require.NoError(t, err, "peer2 raftstore logs:\n%s", peer2.logs.String())
	removed := base.Clone()
	removed.Epoch.ConfVersion = removeResp.GetRegion().GetEpoch().GetConfVersion()
	removed.Hash = nil
	removed.EnsureHash()
	publishRootEvent(t, coordinator, rootevent.PeerRemoved(1, 2, 2, removed))
	waitForRustRegionStatus(t, ctx, addr2, func(status *adminpb.RegionRuntimeStatusResponse) bool {
		return !status.GetHosted()
	})
	heartbeatRustStoreAs(t, ctx, coordinator, 1, addr1, true)

	metadata2, closeMetadata2 := rustMetadataClient(t, addr2)
	defer closeMetadata2()
	staleResp, err := metadata2.CommitMetadata(ctx, &metadatapb.MetadataCommitRequest{
		Context: &metadatapb.MetadataContext{
			RegionId:    1,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: addResp.GetRegion().GetEpoch().GetConfVersion()},
			Peer:        &metapb.RegionPeer{StoreId: 2, PeerId: 2},
		},
		Command: &metadatapb.MetadataCommand{
			RequestId:     []byte("removed-peer-write"),
			PrimaryKey:    []byte("removed-peer-write"),
			ReadVersion:   10,
			CommitVersion: 11,
			Mutations: []*metadatapb.MetadataMutation{{
				Op:    metadatapb.MetadataMutation_PUT,
				Key:   []byte("removed-peer-write"),
				Value: []byte("must-not-apply"),
			}},
			WatchKeys: [][]byte{[]byte("removed-peer-write")},
		},
	})
	require.NoError(t, err, "peer2 raftstore logs:\n%s", peer2.logs.String())
	require.NotNil(t, staleResp.GetRegionError(), "removed peer accepted stale write")
	require.Nil(t, staleResp.GetResult(), "removed peer returned a successful commit result")

	runtime, err := Open(ctx, Options{
		Coordinator:    coordinator,
		DialTimeout:    5 * time.Second,
		BootstrapMount: "vol",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	created, err := runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "after-remove-peer.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 71,
			Mode: 0o644,
		},
	})
	require.NoError(t, err, "peer2 raftstore logs:\n%s", peer2.logs.String())
	lookup, err := runtime.Executor.LookupPlus(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "after-remove-peer.json",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry.Inode, lookup.Dentry.Inode)
}

func TestRustMetadataPlaneSnapshotRetentionPrunesViaCoordinator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo := repoRootFromThisFile(t)
	binary := buildRustRaftstoreServer(t, ctx, repo)
	addr := freeTCPAddr(t)
	rootStore := newE2ERootStorage()
	coordinator := newE2ERootedCoordinator(rootStore)
	coordinatorAddr := startCoordinatorGRPCServer(t, coordinator)
	publishRootEvent(t, coordinator, rootevent.MountRegistered("vol", 1, uint64(model.RootInode), 1))

	logs := startRustRaftstoreServerWithConfig(t, ctx, binary, repo, rustRaftstoreStartConfig{
		addr:                 addr,
		storeID:              1,
		peerID:               1,
		bootstrap:            true,
		coordinatorAddr:      coordinatorAddr,
		coordinatorHeartbeat: 25 * time.Millisecond,
	}).logs
	waitForRustMetadataPlane(t, ctx, addr)
	waitForCoordinatorRoute(t, ctx, coordinator, addr)

	runtime, err := Open(ctx, Options{
		Coordinator:    coordinator,
		DialTimeout:    5 * time.Second,
		BootstrapMount: "vol",
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	_, err = runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "retained.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 1,
			Mode: 0o644,
		},
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	oldVersion, err := runtime.Executor.GetReadVersion(ctx, model.ReadVersionRequest{Mount: "vol"})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	oldEntries, err := runtime.Executor.ReadDirPlus(ctx, model.ReadDirRequest{
		Mount:           "vol",
		Parent:          model.RootInode,
		Limit:           64,
		SnapshotVersion: oldVersion,
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	require.True(t, containsDentryName(oldEntries, "retained.json"))
	_, err = runtime.Executor.Remove(ctx, model.RemoveRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "retained.json",
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	_, err = runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "retained.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 2,
			Mode: 0o644,
		},
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())

	token, err := runtime.Executor.SnapshotSubtree(ctx, model.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	require.NoError(t, runtime.Snapshot.PublishSnapshotSubtree(ctx, token))

	_, err = runtime.Executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "after-retention-floor.json",
		Attrs: model.CreateAttrs{
			Type: model.InodeTypeFile,
			Size: 3,
			Mode: 0o644,
		},
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())

	require.Eventually(t, func() bool {
		prunedEntries, err := runtime.Executor.ReadDirPlus(ctx, model.ReadDirRequest{
			Mount:           "vol",
			Parent:          model.RootInode,
			Limit:           64,
			SnapshotVersion: oldVersion,
		})
		return err == nil && !containsDentryName(prunedEntries, "retained.json")
	}, 10*time.Second, 100*time.Millisecond, "coordinator-driven retention prune did not remove hidden versions; raftstore logs:\n%s", logs.String())

	snapshotEntries, err := runtime.Executor.ReadDirPlus(ctx, model.ReadDirRequest{
		Mount:           "vol",
		Parent:          model.RootInode,
		Limit:           64,
		SnapshotVersion: token.ReadVersion,
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	require.True(t, containsDentryName(snapshotEntries, "retained.json"))
	require.False(t, containsDentryName(snapshotEntries, "after-retention-floor.json"))

	latestEntries, err := runtime.Executor.ReadDirPlus(ctx, model.ReadDirRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Limit:  64,
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	require.True(t, containsDentryName(latestEntries, "retained.json"))
	require.True(t, containsDentryName(latestEntries, "after-retention-floor.json"))
}

func startCoordinatorGRPCServer(t *testing.T, coordinator *coordserver.Service) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(server, coordinator)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func openRustMetadataPlaneRuntime(t *testing.T, ctx context.Context) (*Runtime, *bytes.Buffer, *coordserver.Service) {
	t.Helper()
	repo := repoRootFromThisFile(t)
	binary := buildRustRaftstoreServer(t, ctx, repo)
	addr := freeTCPAddr(t)
	logs := startRustRaftstoreServer(t, ctx, binary, repo, addr)
	waitForRustMetadataPlane(t, ctx, addr)

	coordinator := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(100), tso.NewAllocator(1000))
	publishRootEvent(t, coordinator, rootevent.StoreJoined(1))
	publishRootEvent(t, coordinator, rootevent.MountRegistered("vol", 1, uint64(model.RootInode), 1))
	publishRootEvent(t, coordinator, rootevent.RegionBootstrapped(testMetadataPlaneDescriptor()))
	heartbeatRustStore(t, ctx, coordinator, addr)

	runtime, err := Open(ctx, Options{
		Coordinator:    coordinator,
		DialTimeout:    5 * time.Second,
		BootstrapMount: "vol",
	})
	require.NoError(t, err, "raftstore logs:\n%s", logs.String())
	return runtime, logs, coordinator
}

func requireWatchEvent(t *testing.T, sub observe.WatchSubscription) observe.WatchEvent {
	t.Helper()
	select {
	case event := <-sub.Events():
		return event
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for fsmeta watch event")
	}
	return observe.WatchEvent{}
}

func containsDentryName(entries []model.DentryAttrPair, name string) bool {
	for _, entry := range entries {
		if entry.Dentry.Name == name {
			return true
		}
	}
	return false
}

func newE2ERootedCoordinator(rootStore rootview.RootStorage) *coordserver.Service {
	return coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(100), tso.NewAllocator(1000), rootStore)
}

func heartbeatRustStore(t *testing.T, ctx context.Context, coordinator *coordserver.Service, addr string) *coordpb.StoreHeartbeatResponse {
	t.Helper()
	return heartbeatRustStoreAs(t, ctx, coordinator, 1, addr, true)
}

func heartbeatRustStoreAs(t *testing.T, ctx context.Context, coordinator *coordserver.Service, storeID uint64, addr string, leader bool) *coordpb.StoreHeartbeatResponse {
	t.Helper()
	var leaderRegionIDs []uint64
	var leaderNum uint64
	var leaderStoreID uint64
	if leader {
		leaderRegionIDs = []uint64{1}
		leaderNum = 1
		leaderStoreID = storeID
	}
	heartbeat, err := coordinator.StoreHeartbeat(ctx, &coordpb.StoreHeartbeatRequest{
		StoreId:         storeID,
		ClientAddr:      addr,
		RaftAddr:        addr,
		RegionNum:       1,
		LeaderNum:       leaderNum,
		LeaderRegionIds: leaderRegionIDs,
		RegionStats: []*coordpb.RegionRuntimeStats{{
			RegionId:      1,
			LeaderStoreId: leaderStoreID,
		}},
	})
	require.NoError(t, err)
	require.True(t, heartbeat.GetAccepted())
	return heartbeat
}

func waitForCoordinatorRoute(t *testing.T, parent context.Context, coordinator *coordserver.Service, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		callCtx, cancel := context.WithTimeout(parent, time.Second)
		defer cancel()
		store, err := coordinator.GetStore(callCtx, &coordpb.GetStoreRequest{StoreId: 1})
		if err != nil || store.GetNotFound() || store.GetStore().GetClientAddr() != addr {
			return false
		}
		region, err := coordinator.GetRegionByKey(callCtx, &coordpb.GetRegionByKeyRequest{
			Key:       []byte("__probe"),
			Freshness: coordpb.Freshness_FRESHNESS_STRONG,
		})
		return err == nil && !region.GetNotFound() && region.GetRegionDescriptor().GetRegionId() == 1
	}, 20*time.Second, 100*time.Millisecond)
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
	return startRustRaftstoreServerWithDirs(t, ctx, binary, repo, addr, filepath.Join(t.TempDir(), "holt"), filepath.Join(t.TempDir(), "raftlog")).logs
}

type rustRaftstoreProcess struct {
	cmd      *exec.Cmd
	logs     *bytes.Buffer
	stopOnce sync.Once
}

type rustRaftstoreStartConfig struct {
	addr                 string
	holtDir              string
	raftLogDir           string
	storeID              uint64
	peerID               uint64
	bootstrap            bool
	peerEndpoints        map[uint64]string
	coordinatorAddr      string
	coordinatorHeartbeat time.Duration
}

func startRustRaftstoreServerWithDirs(t *testing.T, ctx context.Context, binary, repo, addr, holtDir, raftLogDir string) *rustRaftstoreProcess {
	t.Helper()
	return startRustRaftstoreServerWithConfig(t, ctx, binary, repo, rustRaftstoreStartConfig{
		addr:       addr,
		holtDir:    holtDir,
		raftLogDir: raftLogDir,
		storeID:    1,
		peerID:     1,
		bootstrap:  true,
	})
}

func startRustRaftstoreServerWithConfig(t *testing.T, ctx context.Context, binary, repo string, cfg rustRaftstoreStartConfig) *rustRaftstoreProcess {
	t.Helper()
	if cfg.holtDir == "" {
		cfg.holtDir = filepath.Join(t.TempDir(), "holt")
	}
	if cfg.raftLogDir == "" {
		cfg.raftLogDir = filepath.Join(t.TempDir(), "raftlog")
	}
	if cfg.storeID == 0 {
		cfg.storeID = 1
	}
	if cfg.peerID == 0 {
		cfg.peerID = cfg.storeID
	}
	var logs bytes.Buffer
	cmd := exec.CommandContext(ctx, binary)
	cmd.Dir = repo
	cmd.Env = append(os.Environ(),
		"NOKV_RAFTSTORE_ADDR="+cfg.addr,
		"NOKV_RAFTSTORE_ADVERTISE_ADDR="+cfg.addr,
		"NOKV_RAFTSTORE_REGION_ID=1",
		"NOKV_RAFTSTORE_STORE_ID="+strconv.FormatUint(cfg.storeID, 10),
		"NOKV_RAFTSTORE_PEER_ID="+strconv.FormatUint(cfg.peerID, 10),
		"NOKV_RAFTSTORE_BOOTSTRAP="+strconv.FormatBool(cfg.bootstrap),
		"NOKV_RAFTSTORE_HOLT_DIR="+cfg.holtDir,
		"NOKV_RAFTSTORE_LOG_DIR="+cfg.raftLogDir,
	)
	if len(cfg.peerEndpoints) != 0 {
		cmd.Env = append(cmd.Env, "NOKV_RAFTSTORE_PEER_ENDPOINTS="+peerEndpointsEnv(cfg.peerEndpoints))
	}
	if cfg.coordinatorAddr != "" {
		cmd.Env = append(cmd.Env, "NOKV_RAFTSTORE_COORDINATOR_ADDR="+cfg.coordinatorAddr)
	}
	if cfg.coordinatorHeartbeat > 0 {
		cmd.Env = append(cmd.Env, "NOKV_RAFTSTORE_COORDINATOR_HEARTBEAT_MS="+strconv.FormatInt(cfg.coordinatorHeartbeat.Milliseconds(), 10))
	}
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	require.NoError(t, cmd.Start())
	proc := &rustRaftstoreProcess{
		cmd:  cmd,
		logs: &logs,
	}
	t.Cleanup(func() {
		proc.stop()
		if t.Failed() {
			t.Logf("rust raftstore logs:\n%s", logs.String())
		}
	})
	return proc
}

func peerEndpointsEnv(peers map[uint64]string) string {
	ids := make([]uint64, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, strconv.FormatUint(id, 10)+"="+peers[id])
	}
	return strings.Join(parts, ",")
}

func (p *rustRaftstoreProcess) stop() {
	p.stopOnce.Do(func() {
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		_ = p.cmd.Wait()
	})
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

func rustAdminClient(t *testing.T, addr string) (adminpb.RaftAdminClient, func()) {
	t.Helper()
	conn, err := grpc.NewClient("dns:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	conn.Connect()
	return adminpb.NewRaftAdminClient(conn), func() {
		require.NoError(t, conn.Close())
	}
}

func rustMetadataClient(t *testing.T, addr string) (metadatapb.MetadataPlaneClient, func()) {
	t.Helper()
	conn, err := grpc.NewClient("dns:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	conn.Connect()
	return metadatapb.NewMetadataPlaneClient(conn), func() {
		require.NoError(t, conn.Close())
	}
}

func waitForRustAdmin(t *testing.T, parent context.Context, addr string) {
	t.Helper()
	client, closeClient := rustAdminClient(t, addr)
	defer closeClient()
	require.Eventually(t, func() bool {
		callCtx, cancel := context.WithTimeout(parent, time.Second)
		defer cancel()
		_, err := client.ExecutionStatus(callCtx, &adminpb.ExecutionStatusRequest{})
		return err == nil
	}, 20*time.Second, 100*time.Millisecond)
}

func waitForRustRegionStatus(t *testing.T, parent context.Context, addr string, accept func(*adminpb.RegionRuntimeStatusResponse) bool) {
	t.Helper()
	client, closeClient := rustAdminClient(t, addr)
	defer closeClient()
	require.Eventually(t, func() bool {
		callCtx, cancel := context.WithTimeout(parent, time.Second)
		defer cancel()
		status, err := client.RegionRuntimeStatus(callCtx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
		return err == nil && accept(status)
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

type e2eRootStorage struct {
	mu       sync.Mutex
	snapshot rootview.Snapshot
}

func newE2ERootStorage() *e2eRootStorage {
	return &e2eRootStorage{
		snapshot: rootview.SnapshotFromRoot(rootstate.Snapshot{}),
	}
}

func (s *e2eRootStorage) Load() (rootview.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return rootview.CloneSnapshot(s.snapshot), nil
}

func (s *e2eRootStorage) AppendRootEvent(_ context.Context, event rootevent.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.Kind == rootevent.KindUnknown {
		return errors.New("invalid root event")
	}
	rooted := s.snapshot.RootSnapshot()
	cursor := rootstate.NextCursor(rooted.State.LastCommitted)
	rootstate.ApplyEventToSnapshot(&rooted, cursor, event)
	next := rootview.SnapshotFromRoot(rooted)
	next.RootToken.Revision = s.snapshot.RootToken.Revision + 1
	s.snapshot = next
	return nil
}

func (s *e2eRootStorage) SaveAllocatorState(_ context.Context, idCurrent, tsCurrent uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idCurrent > s.snapshot.Allocator.IDCurrent {
		s.snapshot.Allocator.IDCurrent = idCurrent
	}
	if tsCurrent > s.snapshot.Allocator.TSCurrent {
		s.snapshot.Allocator.TSCurrent = tsCurrent
	}
	return nil
}

func (s *e2eRootStorage) ApplyGrant(context.Context, rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, errors.New("grant protocol is not used by fsmeta metadata-plane e2e tests")
}

func (s *e2eRootStorage) ApplyVisibleAuthority(context.Context, rootproto.VisibleAuthorityCommand) (rootstate.State, rootproto.VisibleAuthorityGrant, error) {
	return rootstate.State{}, rootproto.VisibleAuthorityGrant{}, errors.New("visible authority protocol is not used by fsmeta metadata-plane e2e tests")
}

func (s *e2eRootStorage) Refresh() error {
	return nil
}

func (s *e2eRootStorage) CanSubmitRootWrites() bool {
	return true
}

func (s *e2eRootStorage) LeaderID() uint64 {
	return 1
}

func (s *e2eRootStorage) Close() error {
	return nil
}
