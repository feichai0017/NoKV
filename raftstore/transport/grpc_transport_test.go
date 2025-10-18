package transport_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/utils"
	proto "google.golang.org/protobuf/proto"

	"google.golang.org/grpc/credentials"

	"github.com/feichai0017/NoKV/pb"
)

func TestGRPCTransportReplicatesProposals(t *testing.T) {
	cluster := newGRPCTestCluster(t, []uint64{1, 2, 3}, raftstore.Config{})
	require.NoError(t, cluster.campaign(1))
	cluster.tickMany(3)
	cluster.flush()

	leader, ok := cluster.leader()
	require.True(t, ok)

	payload, err := proto.Marshal(&pb.KV{
		Key:   []byte("grpc-propose"),
		Value: []byte("grpc-value"),
	})
	require.NoError(t, err)
	require.NoError(t, cluster.propose(leader, payload))
	cluster.tickMany(6)
	cluster.flush()

	for id := range cluster.nodes {
		entry, err := cluster.db(id).GetCF(utils.CFDefault, []byte("grpc-propose"))
		require.NoError(t, err, "db %d", id)
		require.Equal(t, []byte("grpc-value"), entry.Value, "db %d", id)
		entry.DecrRef()
	}
}

func TestGRPCTransportSupportsTLS(t *testing.T) {
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

	payload, err := proto.Marshal(&pb.KV{
		Key:   []byte("tls-propose"),
		Value: []byte("secure-value"),
	})
	require.NoError(t, err)
	require.NoError(t, cluster.propose(leader, payload))
	cluster.tickMany(6)
	cluster.flush()

	for id := range cluster.nodes {
		entry, err := cluster.db(id).GetCF(utils.CFDefault, []byte("tls-propose"))
		require.NoError(t, err)
		require.Equal(t, []byte("secure-value"), entry.Value, "db %d", id)
		entry.DecrRef()
	}
}

func TestGRPCTransportHandlesPartition(t *testing.T) {
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

	payload, err := proto.Marshal(&pb.KV{
		Key:   []byte("grpc-partition"),
		Value: []byte("stale"),
	})
	require.NoError(t, err)
	require.NoError(t, cluster.propose(leader, payload))

	cluster.tickMany(6)
	cluster.flush()

	_, err = cluster.db(followerID).GetCF(utils.CFDefault, []byte("grpc-partition"))
	require.ErrorIs(t, err, utils.ErrKeyNotFound, "follower should not have applied entry while partitioned")

	cluster.unblockLink(leader, followerID)
	cluster.unblockLink(followerID, leader)

	cluster.tickMany(10)
	cluster.flush()

	entry, err := cluster.db(followerID).GetCF(utils.CFDefault, []byte("grpc-partition"))
	require.NoError(t, err)
	require.Equal(t, []byte("stale"), entry.Value)
	entry.DecrRef()

	leader, ok = cluster.leader()
	require.True(t, ok)

	payload, err = proto.Marshal(&pb.KV{
		Key:   []byte("grpc-reconnect"),
		Value: []byte("recovered"),
	})
	require.NoError(t, err)
	require.NoError(t, cluster.propose(leader, payload))
	cluster.tickMany(6)
	cluster.flush()

	entry, err = cluster.db(followerID).GetCF(utils.CFDefault, []byte("grpc-reconnect"))
	require.NoError(t, err)
	require.Equal(t, []byte("recovered"), entry.Value)
	entry.DecrRef()

	ptr, ok := cluster.manifest(followerID).RaftPointer(cluster.groupID)
	require.True(t, ok)
	require.GreaterOrEqual(t, ptr.AppliedIndex, uint64(2))
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
			var kv pb.KV
			if err := proto.Unmarshal(entry.Data, &kv); err != nil {
				return err
			}
			if len(kv.GetValue()) == 0 {
				if err := db.DelCF(utils.CFDefault, kv.GetKey()); err != nil {
					return err
				}
				continue
			}
			if err := db.SetCF(utils.CFDefault, kv.GetKey(), kv.GetValue()); err != nil {
				return err
			}
		}
		return nil
	}
}
