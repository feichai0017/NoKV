package transport_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/percolator"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/raftstore/kv"
	transportpkg "github.com/feichai0017/NoKV/raftstore/transport"
	"github.com/feichai0017/NoKV/utils"

	"google.golang.org/grpc/credentials"

	"github.com/feichai0017/NoKV/pb"
)

func mustEncodePutCommand(t *testing.T, key, value []byte, startVersion uint64) []byte {
	t.Helper()
	req := &pb.RaftCmdRequest{
		Requests: []*pb.Request{
			{
				CmdType: pb.CmdType_CMD_PREWRITE,
				Cmd: &pb.Request_Prewrite{Prewrite: &pb.PrewriteRequest{
					Mutations: []*pb.Mutation{{
						Op:    pb.Mutation_Put,
						Key:   append([]byte(nil), key...),
						Value: append([]byte(nil), value...),
					}},
					PrimaryLock:  append([]byte(nil), key...),
					StartVersion: startVersion,
					LockTtl:      3000,
				}},
			},
			{
				CmdType: pb.CmdType_CMD_COMMIT,
				Cmd: &pb.Request_Commit{Commit: &pb.CommitRequest{
					Keys:          [][]byte{append([]byte(nil), key...)},
					StartVersion:  startVersion,
					CommitVersion: startVersion + 1,
				}},
			},
		},
	}
	payload, err := command.Encode(req)
	require.NoError(t, err)
	return payload
}

func requireVisibleValue(t *testing.T, db *NoKV.DB, key, value []byte) {
	t.Helper()
	reader := percolator.NewReader(db)
	val, err := reader.GetValue(key, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, value, val)
}

func requireMissingValue(t *testing.T, db *NoKV.DB, key []byte) {
	t.Helper()
	reader := percolator.NewReader(db)
	_, err := reader.GetValue(key, math.MaxUint64)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestGRPCTransportReplicatesProposals(t *testing.T) {
	transportpkg.ResetGRPCMetricsForTesting()
	cluster := newGRPCTestCluster(t, []uint64{1, 2, 3}, raftstore.Config{})
	require.NoError(t, cluster.campaign(1))
	cluster.tickMany(3)
	cluster.flush()

	leader, ok := cluster.leader()
	require.True(t, ok)

	payload := mustEncodePutCommand(t, []byte("grpc-propose"), []byte("grpc-value"), 10)
	require.NoError(t, cluster.propose(leader, payload))
	cluster.tickMany(6)
	cluster.flush()

	for id := range cluster.nodes {
		requireVisibleValue(t, cluster.db(id), []byte("grpc-propose"), []byte("grpc-value"))
	}
}

func TestGRPCTransportManualTicksDriveElection(t *testing.T) {
	transportpkg.ResetGRPCMetricsForTesting()
	cluster := newGRPCTestCluster(t, []uint64{1, 2, 3}, raftstore.Config{})
	cluster.flush()

	if _, ok := cluster.leader(); ok {
		t.Fatalf("expected no leader before ticking")
	}

	cluster.tickMany(4)
	cluster.flush()
	if _, ok := cluster.leader(); ok {
		t.Fatalf("unexpected leader before election timeout")
	}

	cluster.tickMany(6)
	cluster.flush()
	leader, ok := cluster.leader()
	if !ok || leader == 0 {
		t.Fatalf("expected leader after manual ticks")
	}
}

func TestGRPCTransportSupportsTLS(t *testing.T) {
	transportpkg.ResetGRPCMetricsForTesting()
	serverCreds, clientCreds := buildTestCredentials(t)
	cluster := newGRPCTestCluster(
		t,
		[]uint64{1, 2},
		raftstore.Config{},
		raftstore.WithGRPCServerCredentials(serverCreds),
		raftstore.WithGRPCClientCredentials(clientCreds),
		raftstore.WithGRPCRetry(1, 25*time.Millisecond),
		raftstore.WithGRPCSendTimeout(750*time.Millisecond),
	)
	require.NoError(t, cluster.campaign(1))
	cluster.tickMany(2)
	cluster.flush()

	leader, ok := cluster.leader()
	require.True(t, ok)

	payload := mustEncodePutCommand(t, []byte("tls-propose"), []byte("secure-value"), 20)
	require.NoError(t, cluster.propose(leader, payload))
	cluster.tickMany(6)
	cluster.flush()

	for id := range cluster.nodes {
		requireVisibleValue(t, cluster.db(id), []byte("tls-propose"), []byte("secure-value"))
	}
}

func TestGRPCTransportHandlesPartition(t *testing.T) {
	transportpkg.ResetGRPCMetricsForTesting()
	cluster := newGRPCTestCluster(
		t,
		[]uint64{1, 2, 3},
		raftstore.Config{},
		raftstore.WithGRPCRetry(2, 25*time.Millisecond),
		raftstore.WithGRPCSendTimeout(750*time.Millisecond),
	)
	require.NoError(t, cluster.campaign(1))
	cluster.tickMany(3)
	cluster.flush()

	leader, ok := cluster.leader()
	require.True(t, ok)
	require.NotEqual(t, uint64(0), leader)

	followerID := uint64(2)
	cluster.blockLink(leader, followerID)
	cluster.blockLink(followerID, leader)

	payload := mustEncodePutCommand(t, []byte("grpc-partition"), []byte("stale"), 30)
	require.NoError(t, cluster.propose(leader, payload))

	cluster.tickMany(6)
	cluster.flush()

	requireMissingValue(t, cluster.db(followerID), []byte("grpc-partition"))

	cluster.unblockLink(leader, followerID)
	cluster.unblockLink(followerID, leader)

	cluster.tickMany(10)
	cluster.flush()

	requireVisibleValue(t, cluster.db(followerID), []byte("grpc-partition"), []byte("stale"))

	leader, ok = cluster.leader()
	require.True(t, ok)

	payload = mustEncodePutCommand(t, []byte("grpc-reconnect"), []byte("recovered"), 32)
	require.NoError(t, cluster.propose(leader, payload))
	cluster.tickMany(6)
	cluster.flush()

	requireVisibleValue(t, cluster.db(followerID), []byte("grpc-reconnect"), []byte("recovered"))

	ptr, ok := cluster.manifest(followerID).RaftPointer(cluster.groupID)
	require.True(t, ok)
	require.GreaterOrEqual(t, ptr.AppliedIndex, uint64(2))
}

func TestGRPCTransportMetricsWatchdog(t *testing.T) {
	transportpkg.ResetGRPCMetricsForTesting()

	transport, err := raftstore.NewGRPCTransport(
		1,
		"127.0.0.1:0",
		raftstore.WithGRPCDialTimeout(50*time.Millisecond),
		raftstore.WithGRPCSendTimeout(50*time.Millisecond),
		raftstore.WithGRPCRetry(0, 0),
	)
	require.NoError(t, err)
	defer func() { _ = transport.Close() }()

	transport.SetPeer(2, "127.0.0.1:65535")
	msg := myraft.Message{To: 2}
	for i := 0; i < 3; i++ {
		transport.Send(msg)
	}

	snap := transportpkg.GRPCMetricsSnapshot()
	require.GreaterOrEqual(t, snap.DialFailures, int64(3))
	require.True(t, snap.WatchdogActive)
	require.GreaterOrEqual(t, snap.WatchdogConsecutiveFails, snap.WatchdogThreshold)
	require.True(t, strings.Contains(snap.WatchdogReason, "dial failure"))
	logTransportMetric(t, "watchdog_after_failures", snap)

	peer, err := raftstore.NewGRPCTransport(
		2,
		"127.0.0.1:0",
		raftstore.WithGRPCDialTimeout(50*time.Millisecond),
		raftstore.WithGRPCSendTimeout(50*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = peer.Close() }()

	transport.SetPeer(2, peer.Addr())
	transport.Send(msg)

	snap = transportpkg.GRPCMetricsSnapshot()
	require.False(t, snap.WatchdogActive)
	require.Equal(t, int64(0), snap.WatchdogConsecutiveFails)
	require.Equal(t, "", snap.WatchdogReason)
	logTransportMetric(t, "watchdog_after_recovery", snap)
}

func TestGRPCTransportMetricsBlockedPeers(t *testing.T) {
	transportpkg.ResetGRPCMetricsForTesting()

	tr, err := raftstore.NewGRPCTransport(1, "127.0.0.1:0")
	require.NoError(t, err)
	trRef := tr
	t.Cleanup(func() {
		if trRef != nil {
			_ = trRef.Close()
		}
	})

	tr.BlockPeer(2)
	tr.BlockPeer(2)
	snap := transportpkg.GRPCMetricsSnapshot()
	logTransportMetric(t, "blocked_after_block", snap)
	require.Equal(t, int64(1), snap.BlockedPeers)

	tr.UnblockPeer(2)
	snap = transportpkg.GRPCMetricsSnapshot()
	logTransportMetric(t, "blocked_after_unblock", snap)
	require.Equal(t, int64(0), snap.BlockedPeers)

	tr.BlockPeer(3)
	snap = transportpkg.GRPCMetricsSnapshot()
	logTransportMetric(t, "blocked_after_block_again", snap)
	require.Equal(t, int64(1), snap.BlockedPeers)

	tr.SetPeer(3, "127.0.0.1:9000")
	snap = transportpkg.GRPCMetricsSnapshot()
	logTransportMetric(t, "blocked_after_set_peer", snap)
	require.Equal(t, int64(0), snap.BlockedPeers)

	tr.BlockPeer(4)
	snap = transportpkg.GRPCMetricsSnapshot()
	logTransportMetric(t, "blocked_before_close", snap)
	require.Equal(t, int64(1), snap.BlockedPeers)

	require.NoError(t, tr.Close())
	trRef = nil
	snap = transportpkg.GRPCMetricsSnapshot()
	logTransportMetric(t, "blocked_after_close", snap)
	require.Equal(t, int64(0), snap.BlockedPeers)
}

func logTransportMetric(t *testing.T, label string, snap transportpkg.GRPCTransportMetrics) {
	if os.Getenv("CHAOS_TRACE_METRICS") == "" {
		return
	}
	t.Helper()
	t.Logf("TRANSPORT_METRIC %s %+v", label, snap)
}

type grpcTestCluster struct {
	t          *testing.T
	groupID    uint64
	nodes      map[uint64]*grpcTestNode
	transports map[uint64]*raftstore.GRPCTransport
}

type grpcTestNode struct {
	id        uint64
	db        *NoKV.DB
	peer      *raftstore.Peer
	transport *raftstore.GRPCTransport
}

func newGRPCTestCluster(t *testing.T, ids []uint64, cfg raftstore.Config, opts ...raftstore.GRPCOption) *grpcTestCluster {
	t.Helper()
	cluster := &grpcTestCluster{
		t:          t,
		groupID:    cfg.GroupID,
		nodes:      make(map[uint64]*grpcTestNode),
		transports: make(map[uint64]*raftstore.GRPCTransport),
	}
	if cluster.groupID == 0 {
		cluster.groupID = 1
	}
	baseDir := t.TempDir()

	addresses := make(map[uint64]string)
	for _, id := range ids {
		transport, err := raftstore.NewGRPCTransport(id, "127.0.0.1:0", opts...)
		require.NoError(t, err)
		cluster.transports[id] = transport
		addresses[id] = transport.Addr()
	}
	peers := make([]myraft.Peer, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, myraft.Peer{ID: id})
	}
	for id, transport := range cluster.transports {
		for peerID, addr := range addresses {
			if peerID == id {
				continue
			}
			transport.SetPeer(peerID, addr)
		}
	}

	for _, id := range ids {
		dbPath := filepath.Join(baseDir, fmt.Sprintf("node-%d", id))
		db := openDBAt(t, dbPath)
		transport := cluster.transports[id]
		config := cfg
		config.Transport = transport
		config.WAL = db.WAL()
		config.Manifest = db.Manifest()
		config.Apply = applyToDB(db)
		config.GroupID = cluster.groupID
		config.RaftConfig.ID = id
		if config.RaftConfig.ElectionTick == 0 {
			config.RaftConfig = myraft.Config{
				ID:              id,
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
				PreVote:         true,
			}
		}
		peer, err := raftstore.NewPeer(&config)
		require.NoError(t, err)
		transport.SetHandler(peer.Step)
		require.NoError(t, peer.Bootstrap(peers))
		t.Cleanup(func() { _ = peer.Close() })
		cluster.nodes[id] = &grpcTestNode{
			id:        id,
			db:        db,
			peer:      peer,
			transport: transport,
		}
	}

	t.Cleanup(func() {
		for _, node := range cluster.nodes {
			_ = node.db.Close()
		}
		for _, transport := range cluster.transports {
			_ = transport.Close()
		}
	})
	return cluster
}

func (c *grpcTestCluster) campaign(id uint64) error {
	if node, ok := c.nodes[id]; ok {
		return node.peer.Campaign()
	}
	return errors.New("node not found")
}

func (c *grpcTestCluster) leader() (uint64, bool) {
	for id, node := range c.nodes {
		if node.peer.Status().RaftState == myraft.StateLeader {
			return id, true
		}
	}
	return 0, false
}

func (c *grpcTestCluster) propose(id uint64, data []byte) error {
	if node, ok := c.nodes[id]; ok {
		return node.peer.Propose(data)
	}
	return errors.New("node not found")
}

func (c *grpcTestCluster) tickMany(n int) {
	for i := 0; i < n; i++ {
		for _, node := range c.nodes {
			_ = node.peer.Tick()
		}
	}
}

func (c *grpcTestCluster) flush() {
	for _, node := range c.nodes {
		_ = node.peer.Flush()
	}
}

func (c *grpcTestCluster) blockLink(from, to uint64) {
	if transport, ok := c.transports[from]; ok {
		transport.BlockPeer(to)
	}
}

func (c *grpcTestCluster) unblockLink(from, to uint64) {
	if transport, ok := c.transports[from]; ok {
		transport.UnblockPeer(to)
	}
}

func (c *grpcTestCluster) db(id uint64) *NoKV.DB {
	return c.nodes[id].db
}

func (c *grpcTestCluster) manifest(id uint64) *manifest.Manager {
	return c.nodes[id].db.Manifest()
}

func buildTestCredentials(t *testing.T) (credentials.TransportCredentials, credentials.TransportCredentials) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "NoKV-test"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)

	cert := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}

	caCert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	cert.Leaf = caCert

	serverCreds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	clientCreds := credentials.NewTLS(&tls.Config{
		RootCAs:    pool,
		ServerName: "127.0.0.1",
	})
	return serverCreds, clientCreds
}

func openDBAt(t *testing.T, dir string) *NoKV.DB {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	opt.ValueLogFileSize = 1 << 20
	opt.ValueThreshold = utils.DefaultValueThreshold
	opt.RaftLagWarnSegments = 1
	return NoKV.Open(opt)
}

func applyToDB(db *NoKV.DB) raftstore.ApplyFunc {
	return func(entries []myraft.Entry) error {
		for _, entry := range entries {
			if entry.Type != myraft.EntryNormal || len(entry.Data) == 0 {
				continue
			}
			req, ok, err := command.Decode(entry.Data)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("raftstore transport test: unsupported legacy raft payload")
			}
			if _, err := kv.Apply(db, req); err != nil {
				return err
			}
		}
		return nil
	}
}
