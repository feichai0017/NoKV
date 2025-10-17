package raftstore_test

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
	proto "google.golang.org/protobuf/proto"

	"github.com/feichai0017/NoKV/pb"
)

type memoryNetwork struct {
	peers   map[uint64]*raftstore.Peer
	blocked map[uint64]bool
}

func newMemoryNetwork() *memoryNetwork {
	return &memoryNetwork{
		peers:   make(map[uint64]*raftstore.Peer),
		blocked: make(map[uint64]bool),
	}
}

func (n *memoryNetwork) Register(peer *raftstore.Peer) {
	n.peers[peer.ID()] = peer
}

func (n *memoryNetwork) Send(msg myraft.Message) {
	if msg.To == 0 {
		return
	}
	if n.blocked[msg.To] {
		return
	}
	if peer, ok := n.peers[msg.To]; ok {
		_ = peer.Step(msg)
	}
}

func (n *memoryNetwork) Tick() {
	for _, p := range n.peers {
		_ = p.Tick()
	}
}

func (n *memoryNetwork) Campaign(id uint64) error {
	if peer, ok := n.peers[id]; ok {
		return peer.Campaign()
	}
	return nil
}

func (n *memoryNetwork) Propose(id uint64, data []byte) error {
	if peer, ok := n.peers[id]; ok {
		return peer.Propose(data)
	}
	return nil
}

func (n *memoryNetwork) Leader() (uint64, bool) {
	for id, p := range n.peers {
		if p.Status().RaftState == myraft.StateLeader {
			return id, true
		}
	}
	return 0, false
}

func (n *memoryNetwork) Flush() {
	for _, p := range n.peers {
		_ = p.Flush()
	}
}

func (n *memoryNetwork) Block(id uint64) {
	n.blocked[id] = true
}

func (n *memoryNetwork) Unblock(id uint64) {
	delete(n.blocked, id)
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

func TestRaftStoreReplicatesProposals(t *testing.T) {
	net := newMemoryNetwork()
	var peers []*raftstore.Peer
	var dbs []*NoKV.DB
	peerList := []myraft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}

	const raftGroupID = uint64(1)

	for id := uint64(1); id <= 3; id++ {
		dbDir := filepath.Join(t.TempDir(), fmt.Sprintf("db-%d", id))
		db := openDBAt(t, dbDir)
		t.Cleanup(func(db *NoKV.DB) func() {
			return func() { _ = db.Close() }
		}(db))
		rc := myraft.Config{
			ID:              id,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   math.MaxUint64,
			MaxInflightMsgs: 256,
			PreVote:         true,
		}
		peer, err := raftstore.NewPeer(&raftstore.Config{
			RaftConfig: rc,
			Transport:  net,
			Apply:      applyToDB(db),
			WAL:        db.WAL(),
			Manifest:   db.Manifest(),
			GroupID:    raftGroupID,
		})
		require.NoError(t, err)
		net.Register(peer)
		peers = append(peers, peer)
		dbs = append(dbs, db)
	}

	for _, peer := range peers {
		require.NoError(t, peer.Bootstrap(peerList))
	}

	require.NoError(t, net.Campaign(1))
	net.Flush()

	leader, ok := net.Leader()
	require.True(t, ok)
	require.Equal(t, uint64(1), leader)

	payload, err := proto.Marshal(&pb.KV{
		Key:   []byte("raft-key"),
		Value: []byte("raft-value"),
	})
	require.NoError(t, err)

	require.NoError(t, net.Propose(leader, payload))
	for i := 0; i < 10; i++ {
		net.Tick()
	}
	net.Flush()

	for idx, db := range dbs {
		entry, err := db.GetCF(utils.CFDefault, []byte("raft-key"))
		require.NoError(t, err, "db %d", idx+1)
		require.Equal(t, []byte("raft-value"), entry.Value, "db %d", idx+1)
		entry.DecrRef()
	}
}

func TestRaftStoreRecoverFromDisk(t *testing.T) {
	baseDir := t.TempDir()
	net := newMemoryNetwork()
	var peers []*raftstore.Peer
	type node struct {
		id    uint64
		dbDir string
		db    *NoKV.DB
	}
	var nodes []node
	peerList := []myraft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}
	const raftGroupID = uint64(1)

	for id := uint64(1); id <= 3; id++ {
		dbDir := filepath.Join(baseDir, fmt.Sprintf("db-%d", id))
		db := openDBAt(t, dbDir)
		rc := myraft.Config{
			ID:              id,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   math.MaxUint64,
			MaxInflightMsgs: 256,
			PreVote:         true,
		}
		peer, err := raftstore.NewPeer(&raftstore.Config{
			RaftConfig: rc,
			Transport:  net,
			Apply:      applyToDB(db),
			WAL:        db.WAL(),
			Manifest:   db.Manifest(),
			GroupID:    raftGroupID,
		})
		require.NoError(t, err)
		net.Register(peer)
		peers = append(peers, peer)
		nodes = append(nodes, node{id: id, dbDir: dbDir, db: db})
	}

	for _, peer := range peers {
		require.NoError(t, peer.Bootstrap(peerList))
	}

	require.NoError(t, net.Campaign(1))
	net.Flush()

	payload, err := proto.Marshal(&pb.KV{
		Key:   []byte("raft-persist-key"),
		Value: []byte("persist-before"),
	})
	require.NoError(t, err)

	leader, ok := net.Leader()
	require.True(t, ok)
	require.NoError(t, net.Propose(leader, payload))
	for i := 0; i < 10; i++ {
		net.Tick()
	}
	net.Flush()

	for _, n := range nodes {
		entry, err := n.db.GetCF(utils.CFDefault, []byte("raft-persist-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("persist-before"), entry.Value)
		entry.DecrRef()
		require.NoError(t, n.db.Close())
	}

	// Restart peers with persistent storage.
	net2 := newMemoryNetwork()
	var peers2 []*raftstore.Peer
	for i, n := range nodes {
		db := openDBAt(t, n.dbDir)
		rc := myraft.Config{
			ID:              n.id,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   math.MaxUint64,
			MaxInflightMsgs: 256,
			PreVote:         true,
		}
		peer, err := raftstore.NewPeer(&raftstore.Config{
			RaftConfig: rc,
			Transport:  net2,
			Apply:      applyToDB(db),
			WAL:        db.WAL(),
			Manifest:   db.Manifest(),
			GroupID:    raftGroupID,
		})
		require.NoError(t, err)
		net2.Register(peer)
		peers2 = append(peers2, peer)
		nodes[i].db = db
	}

	net2.Flush()

	for _, n := range nodes {
		entry, err := n.db.GetCF(utils.CFDefault, []byte("raft-persist-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("persist-before"), entry.Value)
		entry.DecrRef()
	}

	var (
		leader2 uint64
		found   bool
	)
	_ = net2.Campaign(1)
	for range 20 {
		net2.Tick()
		net2.Flush()
		leader2, found = net2.Leader()
		if found {
			break
		}
	}
	if !found {
		for id, peer := range net2.peers {
			status := peer.Status()
			t.Logf("peer %d state=%v term=%d", id, status.RaftState, status.Term)
		}
	}
	require.True(t, found)

	payload2, err := proto.Marshal(&pb.KV{
		Key:   []byte("raft-persist-key2"),
		Value: []byte("persist-after"),
	})
	require.NoError(t, err)
	require.NoError(t, net2.Propose(leader2, payload2))
	for range 10 {
		net2.Tick()
	}
	net2.Flush()

	for _, n := range nodes {
		entry, err := n.db.GetCF(utils.CFDefault, []byte("raft-persist-key2"))
		require.NoError(t, err)
		require.Equal(t, []byte("persist-after"), entry.Value)
		entry.DecrRef()
		require.NoError(t, n.db.Close())
	}
}

func TestRaftStoreSlowFollowerRetention(t *testing.T) {
	net := newMemoryNetwork()
	const (
		raftGroupID = uint64(1)
		followerID  = uint64(2)
	)

	dbs := make(map[uint64]*NoKV.DB)
	var peers []*raftstore.Peer
	peerList := []myraft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}

	for id := uint64(1); id <= 3; id++ {
		dbDir := filepath.Join(t.TempDir(), fmt.Sprintf("slow-follower-db-%d", id))
		db := openDBAt(t, dbDir)
		dbs[id] = db
		rc := myraft.Config{
			ID:              id,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   math.MaxUint64,
			MaxInflightMsgs: 256,
			PreVote:         true,
		}
		peer, err := raftstore.NewPeer(&raftstore.Config{
			RaftConfig: rc,
			Transport:  net,
			Apply:      applyToDB(db),
			WAL:        db.WAL(),
			Manifest:   db.Manifest(),
			GroupID:    raftGroupID,
		})
		require.NoError(t, err)
		net.Register(peer)
		peers = append(peers, peer)
	}

	for _, peer := range peers {
		require.NoError(t, peer.Bootstrap(peerList))
	}

	require.NoError(t, net.Campaign(1))
	net.Flush()

	leader, ok := net.Leader()
	require.True(t, ok)
	require.Equal(t, uint64(1), leader)

	payload, err := proto.Marshal(&pb.KV{
		Key:   []byte("slow-follower-key"),
		Value: []byte("slow-follower-value"),
	})
	require.NoError(t, err)

	followerDB := dbs[followerID]
	ptrBaseline, ok := followerDB.Manifest().RaftPointer(raftGroupID)
	if !ok {
		ptrBaseline = manifest.RaftLogPointer{}
	}

	net.Block(followerID)

	require.NoError(t, net.Propose(leader, payload))

	for i := 0; i < 8; i++ {
		net.Tick()
		net.Flush()
	}

	_, err = followerDB.GetCF(utils.CFDefault, []byte("slow-follower-key"))
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	ptrDuring, ok := followerDB.Manifest().RaftPointer(raftGroupID)
	if ok {
		require.Equal(t, ptrBaseline.AppliedIndex, ptrDuring.AppliedIndex, "follower pointer should remain unchanged while blocked")
	}

	net.Unblock(followerID)

	for i := 0; i < 12; i++ {
		net.Tick()
		net.Flush()
	}

	entry, err := followerDB.GetCF(utils.CFDefault, []byte("slow-follower-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("slow-follower-value"), entry.Value)
	entry.DecrRef()

	ptrAfter, ok := followerDB.Manifest().RaftPointer(raftGroupID)
	require.True(t, ok)
	require.Greater(t, ptrAfter.AppliedIndex, ptrBaseline.AppliedIndex)
}
