package replicated

import (
	"fmt"
	"math"
	"slices"
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

// FixedCluster is one single-process, fixed-membership replicated log cluster.
// It is the first multi-node landing point for the replicated metadata root:
// no transport, no dynamic membership, but real raft message exchange across
// three in-process nodes.
type FixedCluster struct {
	mu    sync.Mutex
	ids   []uint64
	nodes map[uint64]*clusterNode
}

type clusterNode struct {
	id         uint64
	storage    *myraft.MemoryStorage
	raw        *myraft.RawNode
	checkpoint rootstorage.Checkpoint
	records    []rootstorage.CommittedEvent
}

// ClusterDriver binds one replicated root backend instance to one node in the
// fixed in-process cluster.
type ClusterDriver struct {
	cluster *FixedCluster
	id      uint64
}

// NewFixedCluster creates one fixed-membership in-process raft cluster.
func NewFixedCluster(ids ...uint64) (*FixedCluster, error) {
	if len(ids) == 0 {
		ids = []uint64{1, 2, 3}
	}
	seen := make(map[uint64]struct{}, len(ids))
	peers := make([]myraft.Peer, 0, len(ids))
	nodes := make(map[uint64]*clusterNode, len(ids))
	for _, id := range ids {
		if id == 0 {
			return nil, fmt.Errorf("meta/root/backend/replicated: cluster node id must be non-zero")
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("meta/root/backend/replicated: duplicate cluster node id %d", id)
		}
		seen[id] = struct{}{}
		peers = append(peers, myraft.Peer{ID: id})
	}
	cluster := &FixedCluster{
		ids:   slices.Clone(ids),
		nodes: nodes,
	}
	for _, id := range ids {
		node, err := newClusterNode(id, peers)
		if err != nil {
			return nil, err
		}
		cluster.nodes[id] = node
	}
	if err := cluster.drainAllLocked(); err != nil {
		return nil, err
	}
	leader := ids[0]
	if err := cluster.nodes[leader].raw.Campaign(); err != nil {
		return nil, err
	}
	if err := cluster.drainAllLocked(); err != nil {
		return nil, err
	}
	return cluster, nil
}

// Driver returns one node-local driver view for the fixed cluster.
func (c *FixedCluster) Driver(id uint64) (*ClusterDriver, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.nodes[id]; !ok {
		return nil, fmt.Errorf("meta/root/backend/replicated: unknown cluster node %d", id)
	}
	return &ClusterDriver{cluster: c, id: id}, nil
}

// LeaderID returns the current raft leader, if one is known.
func (c *FixedCluster) LeaderID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaderIDLocked()
}

// OpenFixedCluster opens one replicated metadata store per fixed-cluster node.
func OpenFixedCluster(maxRetainedRecords int, ids ...uint64) (map[uint64]*Store, *FixedCluster, error) {
	cluster, err := NewFixedCluster(ids...)
	if err != nil {
		return nil, nil, err
	}
	out := make(map[uint64]*Store, len(cluster.ids))
	for _, id := range cluster.ids {
		driver, err := cluster.Driver(id)
		if err != nil {
			return nil, nil, err
		}
		store, err := Open(Config{Driver: driver, MaxRetainedRecords: maxRetainedRecords})
		if err != nil {
			return nil, nil, err
		}
		out[id] = store
	}
	return out, cluster, nil
}

func (d *ClusterDriver) Config(maxRetainedRecords int) Config {
	return ConfigFromDriver(d, maxRetainedRecords)
}

func (d *ClusterDriver) Log() rootstorage.EventLog { return clusterEventLog{driver: d} }

func (d *ClusterDriver) CheckpointStore() rootstorage.CheckpointStore {
	return clusterCheckpointStore{driver: d}
}

func (d *ClusterDriver) BootstrapInstaller() rootstorage.BootstrapInstaller { return d }

func (d *ClusterDriver) State() DriverState {
	d.cluster.mu.Lock()
	defer d.cluster.mu.Unlock()
	node := d.cluster.nodes[d.id]
	return DriverState{
		Checkpoint: rootstorage.CloneCheckpoint(node.checkpoint),
		Records:    rootstorage.CloneCommittedEvents(node.records),
	}
}

type clusterEventLog struct{ driver *ClusterDriver }

func (l clusterEventLog) Load(offset int64) ([]rootstorage.CommittedEvent, error) {
	l.driver.cluster.mu.Lock()
	defer l.driver.cluster.mu.Unlock()
	node := l.driver.cluster.nodes[l.driver.id]
	if offset <= 0 || int(offset) > len(node.records) {
		return rootstorage.CloneCommittedEvents(node.records), nil
	}
	return rootstorage.CloneCommittedEvents(node.records[int(offset):]), nil
}

func (l clusterEventLog) Append(records ...rootstorage.CommittedEvent) (int64, error) {
	return l.driver.cluster.appendCommitted(l.driver.id, records)
}

func (l clusterEventLog) Compact(records []rootstorage.CommittedEvent) error {
	return l.driver.cluster.compactTail(records)
}

func (l clusterEventLog) Size() (int64, error) {
	l.driver.cluster.mu.Lock()
	defer l.driver.cluster.mu.Unlock()
	return int64(len(l.driver.cluster.nodes[l.driver.id].records)), nil
}

type clusterCheckpointStore struct{ driver *ClusterDriver }

func (s clusterCheckpointStore) Load() (rootstorage.Checkpoint, error) {
	s.driver.cluster.mu.Lock()
	defer s.driver.cluster.mu.Unlock()
	return rootstorage.CloneCheckpoint(s.driver.cluster.nodes[s.driver.id].checkpoint), nil
}

func (s clusterCheckpointStore) Save(checkpoint rootstorage.Checkpoint) error {
	s.driver.cluster.mu.Lock()
	defer s.driver.cluster.mu.Unlock()
	s.driver.cluster.nodes[s.driver.id].checkpoint = rootstorage.CloneCheckpoint(checkpoint)
	return nil
}

func (d *ClusterDriver) InstallBootstrap(checkpoint rootstorage.Checkpoint, records []rootstorage.CommittedEvent) error {
	return d.cluster.installBootstrap(checkpoint, records)
}

func (c *FixedCluster) appendCommitted(from uint64, records []rootstorage.CommittedEvent) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(records) == 0 {
		return int64(len(c.nodes[from].records)), nil
	}
	if from != c.leaderIDLocked() {
		return 0, fmt.Errorf("meta/root/backend/replicated: node %d is not leader", from)
	}
	leader := c.nodes[from]
	for _, rec := range records {
		payload, err := marshalCommittedEvent(rec)
		if err != nil {
			return 0, err
		}
		if err := leader.raw.Propose(payload); err != nil {
			return 0, err
		}
		if err := c.drainAllLocked(); err != nil {
			return 0, err
		}
	}
	return int64(len(leader.records)), nil
}

func (c *FixedCluster) compactTail(records []rootstorage.CommittedEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	checkpoints := make(map[uint64]rootstorage.Checkpoint, len(c.nodes))
	for id, node := range c.nodes {
		checkpoints[id] = rootstorage.CloneCheckpoint(node.checkpoint)
	}
	return c.rebuildLocked(checkpoints, records)
}

func (c *FixedCluster) installBootstrap(checkpoint rootstorage.Checkpoint, records []rootstorage.CommittedEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	checkpoints := make(map[uint64]rootstorage.Checkpoint, len(c.nodes))
	for _, id := range c.ids {
		checkpoints[id] = rootstorage.CloneCheckpoint(checkpoint)
	}
	return c.rebuildLocked(checkpoints, records)
}

func (c *FixedCluster) rebuildLocked(checkpoints map[uint64]rootstorage.Checkpoint, records []rootstorage.CommittedEvent) error {
	peers := make([]myraft.Peer, 0, len(c.ids))
	for _, id := range c.ids {
		peers = append(peers, myraft.Peer{ID: id})
	}
	nodes := make(map[uint64]*clusterNode, len(c.ids))
	for _, id := range c.ids {
		node, err := newClusterNode(id, peers)
		if err != nil {
			return err
		}
		node.checkpoint = rootstorage.CloneCheckpoint(checkpoints[id])
		nodes[id] = node
	}
	c.nodes = nodes
	if err := c.drainAllLocked(); err != nil {
		return err
	}
	leader := c.ids[0]
	if err := c.nodes[leader].raw.Campaign(); err != nil {
		return err
	}
	if err := c.drainAllLocked(); err != nil {
		return err
	}
	for _, rec := range records {
		payload, err := marshalCommittedEvent(rec)
		if err != nil {
			return err
		}
		if err := c.nodes[leader].raw.Propose(payload); err != nil {
			return err
		}
		if err := c.drainAllLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (c *FixedCluster) leaderIDLocked() uint64 {
	for _, id := range c.ids {
		if c.nodes[id].raw.Status().RaftState == myraft.StateLeader {
			return id
		}
	}
	return 0
}

func (c *FixedCluster) drainAllLocked() error {
	for {
		progress := false
		var outbound []myraft.Message
		for _, id := range c.ids {
			node := c.nodes[id]
			for node.raw.HasReady() {
				progress = true
				rd := node.raw.Ready()
				if !myraft.IsEmptyHardState(rd.HardState) {
					if err := node.storage.SetHardState(rd.HardState); err != nil {
						return err
					}
				}
				if !myraft.IsEmptySnap(rd.Snapshot) {
					if err := node.storage.ApplySnapshot(rd.Snapshot); err != nil {
						return err
					}
				}
				if len(rd.Entries) > 0 {
					if err := node.storage.Append(rd.Entries); err != nil {
						return err
					}
				}
				for _, entry := range rd.CommittedEntries {
					if entry.Type != myraft.EntryNormal || len(entry.Data) == 0 {
						continue
					}
					rec, err := unmarshalCommittedEvent(entry.Data)
					if err != nil {
						return err
					}
					node.records = append(node.records, rec)
				}
				outbound = append(outbound, rd.Messages...)
				node.raw.Advance(rd)
			}
		}
		for _, msg := range outbound {
			target, ok := c.nodes[msg.To]
			if !ok {
				continue
			}
			progress = true
			if err := target.raw.Step(msg); err != nil {
				return err
			}
		}
		if !progress {
			return nil
		}
	}
}

func newClusterNode(id uint64, peers []myraft.Peer) (*clusterNode, error) {
	storage := myraft.NewMemoryStorage()
	cfg := &myraft.Config{
		ID:              id,
		ElectionTick:    5,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
		PreVote:         true,
	}
	raw, err := myraft.NewRawNode(cfg)
	if err != nil {
		return nil, err
	}
	if err := raw.Bootstrap(peers); err != nil {
		return nil, err
	}
	return &clusterNode{
		id:         id,
		storage:    storage,
		raw:        raw,
		checkpoint: rootstorage.Checkpoint{},
	}, nil
}
