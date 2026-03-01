package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/pd/core"
	pdserver "github.com/feichai0017/NoKV/pd/server"
	"github.com/feichai0017/NoKV/pd/tso"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestRunServeCmdErrors(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, runServeCmd(&buf, nil))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir()}))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-election-tick", "0"}))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-peer", "bad"}))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-peer", "x=addr"}))
	require.ErrorContains(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-peer", "2=127.0.0.1:20160"}), "--pd-addr is required")
}

func TestStartStorePeersNil(t *testing.T) {
	_, _, err := startStorePeers(nil, nil, 1, 1, 1, 1, 1)
	require.Error(t, err)
}

func TestStartStorePeersManifestMissing(t *testing.T) {
	realDB := newTestDB(t)
	server := newTestServer(t, realDB, 1)
	defer func() {
		_ = server.Close()
		_ = realDB.Close()
	}()

	_, _, err := startStorePeers(server, &NoKV.DB{}, 1, 10, 1, 1, 1)
	require.Error(t, err)
}

func TestStartStorePeersEmpty(t *testing.T) {
	db := newTestDB(t)
	server := newTestServer(t, db, 1)
	defer func() {
		_ = server.Close()
		_ = db.Close()
	}()

	started, total, err := startStorePeers(server, db, 1, 10, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 0, total)
	require.Empty(t, started)
}

func TestStartStorePeersSkipsMissing(t *testing.T) {
	db := newTestDB(t)
	server := newTestServer(t, db, 1)
	defer func() {
		_ = server.Close()
		_ = db.Close()
	}()

	mgr := db.Manifest()
	require.NoError(t, mgr.LogRegionUpdate(manifest.RegionMeta{
		ID:    10,
		State: manifest.RegionStateRunning,
		Epoch: manifest.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers: []manifest.PeerMeta{{StoreID: 2, PeerID: 200}},
	}))

	started, total, err := startStorePeers(server, db, 1, 10, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Empty(t, started)
}

func TestStartStorePeersStartsPeer(t *testing.T) {
	db := newTestDB(t)
	server := newTestServer(t, db, 1)
	defer func() {
		_ = server.Close()
		_ = db.Close()
	}()

	mgr := db.Manifest()
	require.NoError(t, mgr.LogRegionUpdate(manifest.RegionMeta{
		ID:       11,
		State:    manifest.RegionStateRunning,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []manifest.PeerMeta{{StoreID: 1, PeerID: 101}},
	}))

	started, total, err := startStorePeers(server, db, 1, 10, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, started, 1)
}

func TestRunServeCmdNoRegions(t *testing.T) {
	withNotifyContext(t, true, func() {
		dir := t.TempDir()
		var buf bytes.Buffer
		err := runServeCmd(&buf, []string{
			"-workdir", dir,
			"-store-id", "1",
			"-addr", "127.0.0.1:0",
		})
		require.NoError(t, err)
		require.Contains(t, buf.String(), "Manifest contains no regions")
		require.Contains(t, buf.String(), "Serve mode: dev-standalone")
	})
}

func TestRunServeCmdWithRegions(t *testing.T) {
	withNotifyContext(t, true, func() {
		dir := t.TempDir()
		pdAddr, stopPD := startTestPDServer(t)
		defer stopPD()
		db := newTestDBWithDir(t, dir)
		mgr := db.Manifest()
		require.NoError(t, mgr.LogRegionUpdate(manifest.RegionMeta{
			ID:       1,
			State:    manifest.RegionStateRunning,
			StartKey: nil,
			EndKey:   nil,
			Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:    []manifest.PeerMeta{{StoreID: 1, PeerID: 101}},
		}))
		require.NoError(t, mgr.LogRegionUpdate(manifest.RegionMeta{
			ID:       2,
			State:    manifest.RegionStateRunning,
			StartKey: []byte("b"),
			EndKey:   []byte("c"),
			Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:    []manifest.PeerMeta{{StoreID: 2, PeerID: 201}},
		}))
		require.NoError(t, db.Close())

		var buf bytes.Buffer
		err := runServeCmd(&buf, []string{
			"-workdir", dir,
			"-store-id", "1",
			"-addr", "127.0.0.1:0",
			"-peer", "2=127.0.0.1:20160",
			"-pd-addr", pdAddr,
		})
		require.NoError(t, err)
		out := buf.String()
		require.Contains(t, out, "Manifest regions: 2, local peers started: 1")
		require.Contains(t, out, "Store 1 not present in 1 region(s)")
		require.Contains(t, out, "Sample regions:")
		require.Contains(t, out, "Configured peers:")
		require.Contains(t, out, "Serve mode: cluster (PD enabled, addr="+pdAddr+")")
		require.Contains(t, out, "PD heartbeat sink enabled: "+pdAddr)
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
	db := NoKV.Open(opt)
	return db
}

func newTestServer(t *testing.T, db *NoKV.DB, storeID uint64) *raftstore.Server {
	t.Helper()
	server, err := raftstore.NewServer(raftstore.ServerConfig{
		DB: db,
		Store: raftstore.StoreConfig{
			StoreID: storeID,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	return server
}

func startTestPDServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svc := pdserver.NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	srv := grpc.NewServer()
	pb.RegisterPDServer(srv, svc)

	go func() {
		_ = srv.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}
