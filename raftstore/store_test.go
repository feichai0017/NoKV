package raftstore_test

import (
	"math"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
	proto "google.golang.org/protobuf/proto"

	"github.com/feichai0017/NoKV/pb"
)

type memoryNetwork struct {
	peers map[uint64]*raftstore.Peer
}

func newMemoryNetwork() *memoryNetwork {
	return &memoryNetwork{
		peers: make(map[uint64]*raftstore.Peer),
	}
}

func (n *memoryNetwork) Register(peer *raftstore.Peer) {
	n.peers[peer.ID()] = peer
}

func (n *memoryNetwork) Send(msg myraft.Message) {
	if msg.To == 0 {
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

func newTestDB(t *testing.T) *NoKV.DB {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	opt.ValueLogFileSize = 1 << 20
	opt.ValueThreshold = utils.DefaultValueThreshold
	db := NoKV.Open(opt)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
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

	for id := uint64(1); id <= 3; id++ {
		db := newTestDB(t)
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
