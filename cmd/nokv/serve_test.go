package main

import (
	"bytes"
	"context"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"net"
	"os"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	pdserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	serverpkg "github.com/feichai0017/NoKV/raftstore/server"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func testStorage(db *NoKV.DB) serverpkg.Storage {
	if db == nil {
		return serverpkg.Storage{}
	}
	return serverpkg.Storage{
		MVCC: db,
		Raft: db.RaftLog(),
	}
}

func TestRunServeCmdErrors(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, runServeCmd(&buf, nil))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir()}))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-election-tick", "0"}))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-peer", "bad"}))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-peer", "x=addr"}))
	require.ErrorContains(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-peer", "2=127.0.0.1:20160"}), "--coordinator-addr is required")
}

func TestRunServeCmdInvalidMetricsAddr(t *testing.T) {
	withNotifyContext(t, true, func() {
		dir := t.TempDir()
		coordAddr, stopCoordinator := startTestCoordinatorServer(t)
		defer stopCoordinator()
		var buf bytes.Buffer
		err := runServeCmd(&buf, []string{
			"-workdir", dir,
			"-store-id", "1",
			"-addr", "127.0.0.1:0",
			"-coordinator-addr", coordAddr,
			"-metrics-addr", "bad",
		})
		require.ErrorContains(t, err, "start serve metrics endpoint")
	})
}

func TestStartStorePeersNil(t *testing.T) {
	_, _, err := startStorePeers(nil, serverpkg.Storage{}, nil, 1, 1, 1, 1, 1)
	require.Error(t, err)
}

func TestStartStorePeersManifestMissing(t *testing.T) {
	realDB := newTestDB(t)
	server := newTestServer(t, realDB, 1)
	defer func() {
		_ = server.Close()
		_ = realDB.Close()
	}()

	_, _, err := startStorePeers(server, testStorage(&NoKV.DB{}), nil, 1, 10, 1, 1, 1)
	require.Error(t, err)
}

func TestStartStorePeersEmpty(t *testing.T) {
	db := newTestDB(t)
	server := newTestServer(t, db, 1)
	defer func() {
		_ = server.Close()
		_ = db.Close()
	}()

	localMeta := openLocalMetaStore(t, db.WorkDir())
	started, total, err := startStorePeers(server, testStorage(db), localMeta, 1, 10, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 0, total)
	require.Empty(t, started)
}

func TestStartStorePeersSkipsMissing(t *testing.T) {
	db := newTestDB(t)
	localMeta := openLocalMetaStore(t, db.WorkDir())
	server := newTestServerWithMeta(t, db, 1, localMeta)
	defer func() {
		_ = server.Close()
		_ = db.Close()
	}()

	meta := localmeta.RegionMeta{
		ID:    10,
		State: metaregion.ReplicaStateRunning,
		Epoch: metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers: []metaregion.Peer{{StoreID: 2, PeerID: 200}},
	}
	require.NoError(t, localMeta.SaveRegion(meta))

	started, total, err := startStorePeers(server, testStorage(db), localMeta, 1, 10, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Empty(t, started)
}

func TestStartStorePeersStartsPeer(t *testing.T) {
	db := newTestDB(t)
	localMeta := openLocalMetaStore(t, db.WorkDir())
	server := newTestServerWithMeta(t, db, 1, localMeta)
	defer func() {
		_ = server.Close()
		_ = db.Close()
	}()

	meta := localmeta.RegionMeta{
		ID:       11,
		State:    metaregion.ReplicaStateRunning,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
	}
	require.NoError(t, localMeta.SaveRegion(meta))
	require.NoError(t, server.Close())
	server = newTestServerWithMeta(t, db, 1, localMeta)

	started, total, err := startStorePeers(server, testStorage(db), localMeta, 1, 10, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, started, 1)
}

func TestRunServeCmdNoRegions(t *testing.T) {
	withNotifyContext(t, true, func() {
		dir := t.TempDir()
		coordAddr, stopCoordinator := startTestCoordinatorServer(t)
		defer stopCoordinator()
		var buf bytes.Buffer
		err := runServeCmd(&buf, []string{
			"-workdir", dir,
			"-store-id", "1",
			"-addr", "127.0.0.1:0",
			"-coordinator-addr", coordAddr,
			"-metrics-addr", "127.0.0.1:0",
		})
		require.NoError(t, err)
		require.Contains(t, buf.String(), "Local peer catalog contains no regions")
		require.Contains(t, buf.String(), "Serve metrics endpoint listening on http://")
		require.Contains(t, buf.String(), "Serve mode: cluster (coordinator enabled, addr="+coordAddr+")")
		state, err := raftmode.Read(dir)
		require.NoError(t, err)
		require.Equal(t, raftmode.ModeCluster, state.Mode)
		require.Equal(t, uint64(1), state.StoreID)
	})
}

func TestRunServeCmdWithRegions(t *testing.T) {
	withNotifyContext(t, true, func() {
		dir := t.TempDir()
		coordAddr, stopCoordinator := startTestCoordinatorServer(t)
		defer stopCoordinator()
		localMeta := openLocalMetaStore(t, dir)
		require.NoError(t, localMeta.SaveRegion(localmeta.RegionMeta{
			ID:       1,
			State:    metaregion.ReplicaStateRunning,
			StartKey: nil,
			EndKey:   nil,
			Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
			Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		}))
		require.NoError(t, localMeta.SaveRegion(localmeta.RegionMeta{
			ID:       2,
			State:    metaregion.ReplicaStateRunning,
			StartKey: []byte("b"),
			EndKey:   []byte("c"),
			Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
			Peers:    []metaregion.Peer{{StoreID: 2, PeerID: 201}},
		}))

		var buf bytes.Buffer
		err := runServeCmd(&buf, []string{
			"-workdir", dir,
			"-store-id", "1",
			"-addr", "127.0.0.1:0",
			"-peer", "2=127.0.0.1:20160",
			"-coordinator-addr", coordAddr,
		})
		require.NoError(t, err)
		out := buf.String()
		require.Contains(t, out, "Local peer catalog regions: 2, local peers started: 1")
		require.Contains(t, out, "Store 1 not present in 1 region(s)")
		require.Contains(t, out, "Sample regions:")
		require.Contains(t, out, "Configured peers:")
		require.Contains(t, out, "Serve mode: cluster (coordinator enabled, addr="+coordAddr+")")
		require.Contains(t, out, "coordinator heartbeat sink enabled: "+coordAddr)
		state, err := raftmode.Read(dir)
		require.NoError(t, err)
		require.Equal(t, raftmode.ModeCluster, state.Mode)
		require.Equal(t, uint64(1), state.StoreID)
	})
}

func TestFormatKeyNonEmpty(t *testing.T) {
	require.Equal(t, "\"a\"", formatKey([]byte("a"), true))
	require.Equal(t, "\"b\"", formatKey([]byte("b"), false))
}

func withNotifyContext(t *testing.T, cancelImmediately bool, fn func()) {
	t.Helper()
	origNotify := notifyContext
	defer func() { notifyContext = origNotify }()
	notifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		if cancelImmediately {
			cancel()
		}
		return ctx, cancel
	}
	fn()
}

func newTestDB(t *testing.T) *NoKV.DB {
	t.Helper()
	return newTestDBWithDir(t, t.TempDir())
}

func newTestDBWithDir(t *testing.T, dir string) *NoKV.DB {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	return db
}

func openLocalMetaStore(t *testing.T, dir string) *localmeta.Store {
	t.Helper()
	store, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func newTestServer(t *testing.T, db *NoKV.DB, storeID uint64) *serverpkg.Node {
	return newTestServerWithMeta(t, db, storeID, nil)
}

func newTestServerWithMeta(t *testing.T, db *NoKV.DB, storeID uint64, localMeta *localmeta.Store) *serverpkg.Node {
	t.Helper()
	server, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: db,
			Raft: db.RaftLog(),
		},
		Store: storepkg.Config{
			StoreID:   storeID,
			LocalMeta: localMeta,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	return server
}

func startTestCoordinatorServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svc := pdserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	srv := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(srv, svc)

	go func() {
		_ = srv.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}
