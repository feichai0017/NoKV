package peer_test

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/percolator"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
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
	for _, peer := range n.peers {
		if peer.Status().RaftState == myraft.StateLeader {
			_ = peer.ResendSnapshot(id)
		}
	}
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
				return fmt.Errorf("raftstore peer test: unsupported legacy raft payload")
			}
			if _, err := kv.Apply(db, req); err != nil {
				return err
			}
		}
		return nil
	}
}

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
		t.Cleanup(func() { _ = peer.Close() })
		t.Cleanup(func(peer *raftstore.Peer) func() {
			return func() { _ = peer.Close() }
		}(peer))
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

	payload := mustEncodePutCommand(t, []byte("raft-key"), []byte("raft-value"), 10)

	require.NoError(t, net.Propose(leader, payload))
	for range 10 {
		net.Tick()
	}
	net.Flush()

	for idx, db := range dbs {
		reader := percolator.NewReader(db)
		val, err := reader.GetValue([]byte("raft-key"), math.MaxUint64)
		require.NoError(t, err, "db %d", idx+1)
		require.Equal(t, []byte("raft-value"), val, "db %d", idx+1)
	}
}

func TestPeerPrewriteCommit(t *testing.T) {
	net := newMemoryNetwork()
	var peers []*raftstore.Peer
	var dbs []*NoKV.DB
	peerList := []myraft.Peer{{ID: 1}, {ID: 2}}

	for id := uint64(1); id <= 2; id++ {
		dbDir := filepath.Join(t.TempDir(), fmt.Sprintf("db-%d", id))
		db := openDBAt(t, dbDir)
		dbs = append(dbs, db)
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
			GroupID:    11,
			Region:     &manifest.RegionMeta{ID: 11},
		})
		require.NoError(t, err)
		net.Register(peer)
		t.Cleanup(func() { _ = peer.Close() })
		t.Cleanup(func(db *NoKV.DB) func() { return func() { _ = db.Close() } }(db))
		peers = append(peers, peer)
	}

	for _, peer := range peers {
		require.NoError(t, peer.Bootstrap(peerList))
	}
	require.NoError(t, net.Campaign(1))
	net.Flush()

	prewrite := &pb.RaftCmdRequest{
		Header: &pb.CmdHeader{RegionId: 11},
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_PREWRITE,
			Cmd: &pb.Request_Prewrite{Prewrite: &pb.PrewriteRequest{
				Mutations: []*pb.Mutation{{
					Op:    pb.Mutation_Put,
					Key:   []byte("txn-key"),
					Value: []byte("txn-value"),
				}},
				PrimaryLock:  []byte("txn-key"),
				StartVersion: 5,
				LockTtl:      3000,
			}},
		}},
	}
	require.NoError(t, peers[0].ProposeCommand(prewrite))
	for range 5 {
		net.Tick()
	}
	net.Flush()

	commit := &pb.RaftCmdRequest{
		Header: &pb.CmdHeader{RegionId: 11},
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_COMMIT,
			Cmd: &pb.Request_Commit{Commit: &pb.CommitRequest{
				Keys:          [][]byte{[]byte("txn-key")},
				StartVersion:  5,
				CommitVersion: 10,
			}},
		}},
	}
	require.NoError(t, peers[0].ProposeCommand(commit))
	for range 5 {
		net.Tick()
	}
	net.Flush()

	reader := percolator.NewReader(dbs[0])
	val, err := reader.GetValue([]byte("txn-key"), 10)
	require.NoError(t, err)
	require.Equal(t, []byte("txn-value"), val)
	lock, err := reader.GetLock([]byte("txn-key"))
	require.NoError(t, err)
	require.Nil(t, lock)
}

func TestPeerAutoCompactionUpdatesManifest(t *testing.T) {
	net := newMemoryNetwork()
	dbDir := filepath.Join(t.TempDir(), "auto-compact")
	db := openDBAt(t, dbDir)
	t.Cleanup(func() { _ = db.Close() })

	rc := myraft.Config{
		ID:              1,
		ElectionTick:    3,
		HeartbeatTick:   1,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
		PreVote:         true,
	}
	peer, err := raftstore.NewPeer(&raftstore.Config{
		RaftConfig:       rc,
		Transport:        net,
		Apply:            applyToDB(db),
		WAL:              db.WAL(),
		Manifest:         db.Manifest(),
		GroupID:          1,
		LogRetainEntries: 1,
	})
	require.NoError(t, err)
	net.Register(peer)
	t.Cleanup(func() { _ = peer.Close() })
	require.NoError(t, peer.Bootstrap([]myraft.Peer{{ID: 1}}))

	require.NoError(t, net.Campaign(1))
	net.Flush()

	for i := range 6 {
		payload := mustEncodePutCommand(
			t,
			fmt.Appendf(nil, "compact-key-%d", i),
			fmt.Appendf(nil, "val-%d", i),
			uint64(20+i*2),
		)
		require.NoError(t, net.Propose(1, payload))
		for range 3 {
			net.Tick()
		}
		net.Flush()
	}

	for range 4 {
		net.Tick()
		net.Flush()
	}

	ptr, ok := db.Manifest().RaftPointer(1)
	require.True(t, ok)
	require.GreaterOrEqual(t, ptr.TruncatedIndex, uint64(5))
	require.Equal(t, uint64(ptr.Segment), ptr.SegmentIndex)
}

func TestPeerTransferLeader(t *testing.T) {
	net := newMemoryNetwork()
	peerList := []myraft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}

	for id := uint64(1); id <= 3; id++ {
		dbDir := filepath.Join(t.TempDir(), fmt.Sprintf("transfer-%d", id))
		db := openDBAt(t, dbDir)
		t.Cleanup(func(db *NoKV.DB) func() { return func() { _ = db.Close() } }(db))
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
			GroupID:    1,
		})
		require.NoError(t, err)
		net.Register(peer)
		t.Cleanup(func() { _ = peer.Close() })
		require.NoError(t, peer.Bootstrap(peerList))
	}

	require.NoError(t, net.Campaign(1))
	net.Flush()

	leader, ok := net.Leader()
	require.True(t, ok)
	require.NotZero(t, leader)

	var target uint64
	for _, id := range []uint64{1, 2, 3} {
		if id != leader {
			target = id
			break
		}
	}
	require.NotZero(t, target)

	require.NoError(t, net.peers[leader].TransferLeader(target))

	require.Eventually(t, func() bool {
		for range 3 {
			net.Tick()
		}
		net.Flush()
		newLeader, ok := net.Leader()
		return ok && newLeader == target
	}, 2*time.Second, 10*time.Millisecond)
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
		t.Cleanup(func(peer *raftstore.Peer) func() {
			return func() { _ = peer.Close() }
		}(peer))
		peers = append(peers, peer)
		nodes = append(nodes, node{id: id, dbDir: dbDir, db: db})
	}

	for _, peer := range peers {
		require.NoError(t, peer.Bootstrap(peerList))
	}

	require.NoError(t, net.Campaign(1))
	net.Flush()

	payload := mustEncodePutCommand(t, []byte("raft-persist-key"), []byte("persist-before"), 100)

	leader, ok := net.Leader()
	require.True(t, ok)
	require.NoError(t, net.Propose(leader, payload))
	for range 10 {
		net.Tick()
	}
	net.Flush()

	for _, n := range nodes {
		requireVisibleValue(t, n.db, []byte("raft-persist-key"), []byte("persist-before"))
		require.NoError(t, n.db.Close())
	}

	// Restart peers with persistent storage.
	net2 := newMemoryNetwork()
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
		t.Cleanup(func(peer *raftstore.Peer) func() {
			return func() { _ = peer.Close() }
		}(peer))
		nodes[i].db = db
	}

	net2.Flush()

	for _, n := range nodes {
		requireVisibleValue(t, n.db, []byte("raft-persist-key"), []byte("persist-before"))
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

	payload2 := mustEncodePutCommand(t, []byte("raft-persist-key2"), []byte("persist-after"), 120)
	require.NoError(t, net2.Propose(leader2, payload2))
	for range 10 {
		net2.Tick()
	}
	net2.Flush()

	for _, n := range nodes {
		requireVisibleValue(t, n.db, []byte("raft-persist-key2"), []byte("persist-after"))
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
		t.Cleanup(func(peer *raftstore.Peer) func() {
			return func() { _ = peer.Close() }
		}(peer))
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

	payload := mustEncodePutCommand(t, []byte("slow-follower-key"), []byte("slow-follower-value"), 140)

	followerDB := dbs[followerID]
	ptrBaseline, ok := followerDB.Manifest().RaftPointer(raftGroupID)
	if !ok {
		ptrBaseline = manifest.RaftLogPointer{}
	}

	net.Block(followerID)

	require.NoError(t, net.Propose(leader, payload))

	for range 8 {
		net.Tick()
		net.Flush()
	}

	requireMissingValue(t, followerDB, []byte("slow-follower-key"))

	ptrDuring, ok := followerDB.Manifest().RaftPointer(raftGroupID)
	if ok {
		require.Equal(t, ptrBaseline.AppliedIndex, ptrDuring.AppliedIndex, "follower pointer should remain unchanged while blocked")
	}

	net.Unblock(followerID)

	for range 12 {
		net.Tick()
		net.Flush()
	}

	requireVisibleValue(t, followerDB, []byte("slow-follower-key"), []byte("slow-follower-value"))

	ptrAfter, ok := followerDB.Manifest().RaftPointer(raftGroupID)
	require.True(t, ok)
	require.Greater(t, ptrAfter.AppliedIndex, ptrBaseline.AppliedIndex)
}

func TestRaftStoreReadyFailpointRecovery(t *testing.T) {
	const raftGroupID = uint64(1)
	raftstore.SetReadyFailpoint(raftstore.ReadyFailpointNone)
	defer raftstore.SetReadyFailpoint(raftstore.ReadyFailpointNone)

	net := newMemoryNetwork()
	dbDir := filepath.Join(t.TempDir(), "ready-fail-db")
	db := openDBAt(t, dbDir)

	rc := myraft.Config{
		ID:              1,
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

	require.NoError(t, peer.Bootstrap([]myraft.Peer{{ID: 1}}))

	require.NoError(t, net.Campaign(1))
	net.Flush()

	leader, ok := net.Leader()
	require.True(t, ok)
	require.Equal(t, uint64(1), leader)

	ptrBefore, ptrPresent := db.Manifest().RaftPointer(raftGroupID)

	raftstore.SetReadyFailpoint(raftstore.ReadyFailpointSkipManifest)
	payload := mustEncodePutCommand(t, []byte("ready-fail-key"), []byte("ready-fail-value"), 160)

	require.NoError(t, net.Propose(leader, payload))
	for range 8 {
		net.Tick()
		net.Flush()
	}

	ptrAfterFail, ptrAfterPresent := db.Manifest().RaftPointer(raftGroupID)
	if ptrPresent {
		require.True(t, ptrAfterPresent, "manifest pointer should exist when it existed before failpoint")
		require.Equal(t, ptrBefore, ptrAfterFail, "manifest pointer must not advance under failpoint")
	} else {
		require.False(t, ptrAfterPresent, "manifest pointer should remain absent under failpoint")
	}

	require.NoError(t, db.WAL().Rotate())
	snapBeforeCrash := db.Info().Snapshot()
	require.Equal(t, int64(1), snapBeforeCrash.Raft.LagWarnThreshold)
	payloadLag := mustEncodePutCommand(t, []byte("ready-fail-key-lag"), []byte("ready-fail-value-lag"), 162)
	require.NoError(t, net.Propose(leader, payloadLag))
	for range 6 {
		net.Tick()
		net.Flush()
	}
	snapBeforeCrash = db.Info().Snapshot()
	t.Logf("pre-crash snapshot: warning=%v maxLag=%d lagging=%d activeSeg=%d activeSize=%d", snapBeforeCrash.Raft.LagWarning, snapBeforeCrash.Raft.MaxLagSegments, snapBeforeCrash.Raft.LaggingGroups, snapBeforeCrash.WAL.ActiveSegment, snapBeforeCrash.WAL.ActiveSize)
	require.True(t, snapBeforeCrash.Raft.LagWarning, "stats snapshot should flag raft lag while manifest lags")
	require.GreaterOrEqual(t, snapBeforeCrash.Raft.MaxLagSegments, snapBeforeCrash.Raft.LagWarnThreshold)
	require.Greater(t, snapBeforeCrash.Raft.LaggingGroups, 0)

	raftstore.SetReadyFailpoint(raftstore.ReadyFailpointNone)

	requireVisibleValue(t, db, []byte("ready-fail-key"), []byte("ready-fail-value"))

	require.NoError(t, db.Close())

	dbRestart := openDBAt(t, dbDir)
	t.Cleanup(func() { _ = dbRestart.Close() })

	netRestart := newMemoryNetwork()
	rc2 := rc
	peerRestart, err := raftstore.NewPeer(&raftstore.Config{
		RaftConfig: rc2,
		Transport:  netRestart,
		Apply:      applyToDB(dbRestart),
		WAL:        dbRestart.WAL(),
		Manifest:   dbRestart.Manifest(),
		GroupID:    raftGroupID,
	})
	require.NoError(t, err)
	netRestart.Register(peerRestart)
	t.Cleanup(func() { _ = peerRestart.Close() })
	require.NoError(t, peerRestart.Bootstrap([]myraft.Peer{{ID: 1}}))

	ptrRecovered, recovered := dbRestart.Manifest().RaftPointer(raftGroupID)
	require.True(t, recovered, "manifest pointer should be recorded after restart")
	if ptrPresent {
		require.GreaterOrEqual(t, ptrRecovered.AppliedIndex, ptrBefore.AppliedIndex)
		require.NotEqual(t, ptrBefore, ptrRecovered, "recovery should advance manifest pointer beyond failpoint snapshot")
	} else {
		require.Greater(t, ptrRecovered.AppliedIndex, uint64(0))
	}
	require.Greater(t, ptrRecovered.Segment, uint32(0))

	requireVisibleValue(t, dbRestart, []byte("ready-fail-key"), []byte("ready-fail-value"))

	recoveredSnap := dbRestart.Info().Snapshot()
	t.Logf("recovered snapshot: warning=%v maxLag=%d lagging=%d activeSeg=%d activeSize=%d ptr=%+v", recoveredSnap.Raft.LagWarning, recoveredSnap.Raft.MaxLagSegments, recoveredSnap.Raft.LaggingGroups, recoveredSnap.WAL.ActiveSegment, recoveredSnap.WAL.ActiveSize, ptrRecovered)
	require.False(t, recoveredSnap.Raft.LagWarning, "lag warning should clear after manifest catches up")
}

func TestPeerWaitAppliedTracksCommittedIndex(t *testing.T) {
	net := newMemoryNetwork()
	dbDir := filepath.Join(t.TempDir(), "wait-applied-db")
	db := openDBAt(t, dbDir)
	t.Cleanup(func() { _ = db.Close() })

	appliedCh := make(chan uint64, 4)
	applyFn := func(entries []myraft.Entry) error {
		if err := applyToDB(db)(entries); err != nil {
			return err
		}
		if len(entries) > 0 {
			appliedCh <- entries[len(entries)-1].Index
		}
		return nil
	}

	rc := myraft.Config{
		ID:              1,
		ElectionTick:    5,
		HeartbeatTick:   1,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
		PreVote:         true,
	}
	peer, err := raftstore.NewPeer(&raftstore.Config{
		RaftConfig: rc,
		Transport:  net,
		Apply:      applyFn,
		WAL:        db.WAL(),
		Manifest:   db.Manifest(),
		GroupID:    1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })
	net.Register(peer)
	require.NoError(t, peer.Bootstrap([]myraft.Peer{{ID: 1}}))

	require.NoError(t, net.Campaign(1))
	net.Flush()

	leader, ok := net.Leader()
	require.True(t, ok)
	require.Equal(t, uint64(1), leader)

	payload := mustEncodePutCommand(t, []byte("wait-applied"), []byte("value"), 180)
	require.NoError(t, net.Propose(leader, payload))

	for range 5 {
		net.Tick()
	}
	net.Flush()

	var appliedIdx uint64
	select {
	case appliedIdx = <-appliedCh:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for apply index")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, peer.WaitApplied(ctx, appliedIdx))
}
