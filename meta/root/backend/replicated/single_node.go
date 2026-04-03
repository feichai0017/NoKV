package replicated

import (
	"math"
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

// SingleNodeDriver is one single-process ordered-log adapter backed by the
// repo's raft wrapper. It is still single-node, but it validates that the
// replicated metadata backend can sit on top of a real proposal/commit path.
type SingleNodeDriver struct {
	mu         sync.Mutex
	id         uint64
	checkpoint rootstorage.Checkpoint
	records    []rootstorage.CommittedEvent
	node       *singleNode
}

// NewSingleNodeDriver creates one single-node raft-backed driver.
func NewSingleNodeDriver(id uint64) (*SingleNodeDriver, error) {
	if id == 0 {
		id = 1
	}
	node, err := newSingleNode(id)
	if err != nil {
		return nil, err
	}
	return &SingleNodeDriver{id: id, node: node}, nil
}

// OpenSingleNode opens one replicated root store backed by a single-node
// raft committed-log driver.
func OpenSingleNode(id uint64, maxRetainedRecords int) (*Store, *SingleNodeDriver, error) {
	driver, err := NewSingleNodeDriver(id)
	if err != nil {
		return nil, nil, err
	}
	store, err := Open(Config{Driver: driver, MaxRetainedRecords: maxRetainedRecords})
	if err != nil {
		return nil, nil, err
	}
	return store, driver, nil
}

// Log returns the ordered committed-log view of the driver.
func (d *SingleNodeDriver) Log() rootstorage.EventLog { return singleNodeEventLog{driver: d} }

// CheckpointStore returns the compact rooted-checkpoint view of the driver.
func (d *SingleNodeDriver) CheckpointStore() rootstorage.CheckpointStore {
	return singleNodeCheckpointStore{driver: d}
}

// BootstrapInstaller returns the bootstrap-install view of the driver.
func (d *SingleNodeDriver) BootstrapInstaller() rootstorage.BootstrapInstaller { return d }

// State returns one detached view of the driver's checkpoint and retained tail.
func (d *SingleNodeDriver) State() DriverState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return DriverState{
		Checkpoint: rootstorage.CloneCheckpoint(d.checkpoint),
		Records:    rootstorage.CloneCommittedEvents(d.records),
	}
}

type singleNodeEventLog struct{ driver *SingleNodeDriver }

func (l singleNodeEventLog) Load(offset int64) ([]rootstorage.CommittedEvent, error) {
	l.driver.mu.Lock()
	defer l.driver.mu.Unlock()
	if offset <= 0 || int(offset) > len(l.driver.records) {
		return rootstorage.CloneCommittedEvents(l.driver.records), nil
	}
	return rootstorage.CloneCommittedEvents(l.driver.records[int(offset):]), nil
}

func (l singleNodeEventLog) Append(records ...rootstorage.CommittedEvent) (int64, error) {
	l.driver.mu.Lock()
	defer l.driver.mu.Unlock()
	for _, rec := range records {
		payload, err := marshalCommittedEvent(rec)
		if err != nil {
			return 0, err
		}
		if err := l.driver.node.raw.Propose(payload); err != nil {
			return 0, err
		}
		committed, err := l.driver.node.drain()
		if err != nil {
			return 0, err
		}
		for _, entry := range committed {
			next, err := unmarshalCommittedEvent(entry.Data)
			if err != nil {
				return 0, err
			}
			l.driver.records = append(l.driver.records, next)
		}
	}
	return int64(len(l.driver.records)), nil
}

func (l singleNodeEventLog) Compact(records []rootstorage.CommittedEvent) error {
	l.driver.mu.Lock()
	defer l.driver.mu.Unlock()
	if err := l.driver.rebuildLocked(records); err != nil {
		return err
	}
	return nil
}

func (l singleNodeEventLog) Size() (int64, error) {
	l.driver.mu.Lock()
	defer l.driver.mu.Unlock()
	return int64(len(l.driver.records)), nil
}

type singleNodeCheckpointStore struct{ driver *SingleNodeDriver }

func (s singleNodeCheckpointStore) Load() (rootstorage.Checkpoint, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	return rootstorage.CloneCheckpoint(s.driver.checkpoint), nil
}

func (s singleNodeCheckpointStore) Save(checkpoint rootstorage.Checkpoint) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	s.driver.checkpoint = rootstorage.CloneCheckpoint(checkpoint)
	return nil
}

func (d *SingleNodeDriver) InstallBootstrap(checkpoint rootstorage.Checkpoint, records []rootstorage.CommittedEvent) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checkpoint = rootstorage.CloneCheckpoint(checkpoint)
	return d.rebuildLocked(records)
}

func (d *SingleNodeDriver) rebuildLocked(records []rootstorage.CommittedEvent) error {
	node, err := newSingleNode(d.id)
	if err != nil {
		return err
	}
	d.node = node
	d.records = nil
	for _, rec := range records {
		payload, err := marshalCommittedEvent(rec)
		if err != nil {
			return err
		}
		if err := d.node.raw.Propose(payload); err != nil {
			return err
		}
		committed, err := d.node.drain()
		if err != nil {
			return err
		}
		for _, entry := range committed {
			next, err := unmarshalCommittedEvent(entry.Data)
			if err != nil {
				return err
			}
			d.records = append(d.records, next)
		}
	}
	return nil
}

type singleNode struct {
	id      uint64
	storage *myraft.MemoryStorage
	raw     *myraft.RawNode
}

func newSingleNode(id uint64) (*singleNode, error) {
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
	if err := raw.Bootstrap([]myraft.Peer{{ID: id}}); err != nil {
		return nil, err
	}
	node := &singleNode{id: id, storage: storage, raw: raw}
	if _, err := node.drain(); err != nil {
		return nil, err
	}
	if err := raw.Campaign(); err != nil {
		return nil, err
	}
	if _, err := node.drain(); err != nil {
		return nil, err
	}
	return node, nil
}

func (n *singleNode) drain() ([]myraft.Entry, error) {
	var committed []myraft.Entry
	for n.raw.HasReady() {
		rd := n.raw.Ready()
		if !myraft.IsEmptyHardState(rd.HardState) {
			if err := n.storage.SetHardState(rd.HardState); err != nil {
				return nil, err
			}
		}
		if !myraft.IsEmptySnap(rd.Snapshot) {
			if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
				return nil, err
			}
		}
		if len(rd.Entries) > 0 {
			if err := n.storage.Append(rd.Entries); err != nil {
				return nil, err
			}
		}
		for _, entry := range rd.CommittedEntries {
			if entry.Type == myraft.EntryNormal && len(entry.Data) > 0 {
				committed = append(committed, entry)
			}
		}
		for _, msg := range rd.Messages {
			if msg.To == n.id {
				if err := n.raw.Step(msg); err != nil {
					return nil, err
				}
			}
		}
		n.raw.Advance(rd)
	}
	return committed, nil
}
