package replicated

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	myraft "github.com/feichai0017/NoKV/raft"
)

const defaultNetworkTickInterval = 100 * time.Millisecond

// NetworkConfig wires one local raft node to a transport and a fixed peer set.
type NetworkConfig struct {
	ID           uint64
	PeerIDs      []uint64
	Transport    Transport
	TickInterval time.Duration
}

// NetworkDriver hosts one local raft rawnode and exchanges messages through a
// transport, which is the first real landing point for multi-process metadata
// replication.
type NetworkDriver struct {
	mu         sync.Mutex
	closeOnce  sync.Once
	id         uint64
	checkpoint rootstorage.Checkpoint
	records    []rootstorage.CommittedEvent
	node       *networkNode
	transport  Transport
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// NewNetworkDriver creates one transport-backed local metadata replication node.
func NewNetworkDriver(cfg NetworkConfig) (*NetworkDriver, error) {
	if cfg.ID == 0 {
		return nil, fmt.Errorf("meta/root/backend/replicated: network driver id must be non-zero")
	}
	if cfg.Transport == nil {
		return nil, fmt.Errorf("meta/root/backend/replicated: network driver transport is required")
	}
	if len(cfg.PeerIDs) == 0 {
		cfg.PeerIDs = []uint64{cfg.ID}
	}
	if !slices.Contains(cfg.PeerIDs, cfg.ID) {
		return nil, fmt.Errorf("meta/root/backend/replicated: local node %d missing from peer set %v", cfg.ID, cfg.PeerIDs)
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultNetworkTickInterval
	}
	driver := &NetworkDriver{
		id:        cfg.ID,
		transport: cfg.Transport,
		stopCh:    make(chan struct{}),
	}
	node, err := newNetworkNode(cfg, driver.handleTransportMessage)
	if err != nil {
		return nil, err
	}
	driver.node = node
	driver.wg.Add(1)
	go driver.tickLoop(cfg.TickInterval)
	return driver, nil
}

func (d *NetworkDriver) Log() rootstorage.EventLog { return networkEventLog{driver: d} }

func (d *NetworkDriver) CheckpointStore() rootstorage.CheckpointStore {
	return networkCheckpointStore{driver: d}
}

func (d *NetworkDriver) BootstrapInstaller() rootstorage.BootstrapInstaller { return d }

func (d *NetworkDriver) IsLeader() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.node == nil {
		return false
	}
	return d.node.raw.Status().RaftState == myraft.StateLeader
}

func (d *NetworkDriver) LeaderID() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.node == nil {
		return 0
	}
	return d.node.raw.Status().Lead
}

func (d *NetworkDriver) State() DriverState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return DriverState{
		Checkpoint: rootstorage.CloneCheckpoint(d.checkpoint),
		Records:    rootstorage.CloneCommittedEvents(d.records),
	}
}

func (d *NetworkDriver) Campaign() error {
	d.mu.Lock()
	if d.node == nil {
		d.mu.Unlock()
		return fmt.Errorf("meta/root/backend/replicated: network driver is closed")
	}
	if err := d.node.raw.Campaign(); err != nil {
		d.mu.Unlock()
		return err
	}
	_, outbound, err := d.drainLocked()
	d.mu.Unlock()
	if err != nil {
		return err
	}
	return d.sendMessages(outbound)
}

func (d *NetworkDriver) InstallBootstrap(checkpoint rootstorage.Checkpoint, records []rootstorage.CommittedEvent) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checkpoint = rootstorage.CloneCheckpoint(checkpoint)
	return d.rebuildLocked(records)
}

func (d *NetworkDriver) Close() error {
	var err error
	d.closeOnce.Do(func() {
		close(d.stopCh)
		d.wg.Wait()
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.transport != nil {
			err = d.transport.Close()
		}
		d.node = nil
	})
	return err
}

func (d *NetworkDriver) tickLoop(interval time.Duration) {
	defer d.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.mu.Lock()
			var outbound []myraft.Message
			if d.node != nil {
				d.node.raw.Tick()
				_, outbound, _ = d.drainLocked()
			}
			d.mu.Unlock()
			_ = d.sendMessages(outbound)
		}
	}
}

func (d *NetworkDriver) handleTransportMessage(msg myraft.Message) error {
	d.mu.Lock()
	if d.node == nil {
		d.mu.Unlock()
		return fmt.Errorf("meta/root/backend/replicated: network driver is closed")
	}
	if err := d.node.raw.Step(msg); err != nil {
		d.mu.Unlock()
		return err
	}
	_, outbound, err := d.drainLocked()
	d.mu.Unlock()
	if err != nil {
		return err
	}
	return d.sendMessages(outbound)
}

func (d *NetworkDriver) rebuildLocked(records []rootstorage.CommittedEvent) error {
	peers := d.node.peerIDs
	node, err := newNetworkNode(NetworkConfig{
		ID:        d.id,
		PeerIDs:   peers,
		Transport: d.transport,
	}, d.handleTransportMessage)
	if err != nil {
		return err
	}
	d.node = node
	d.records = nil
	for _, rec := range records {
		if d.node.raw.Status().RaftState != myraft.StateLeader {
			if err := d.node.raw.Campaign(); err != nil {
				return err
			}
			_, outbound, err := d.drainLocked()
			if err != nil {
				return err
			}
			d.mu.Unlock()
			if err := d.sendMessages(outbound); err != nil {
				d.mu.Lock()
				return err
			}
			d.mu.Lock()
		}
		payload, err := marshalCommittedEvent(rec)
		if err != nil {
			return err
		}
		if err := d.node.raw.Propose(payload); err != nil {
			return err
		}
		_, outbound, err := d.drainLocked()
		if err != nil {
			return err
		}
		d.mu.Unlock()
		if err := d.sendMessages(outbound); err != nil {
			d.mu.Lock()
			return err
		}
		d.mu.Lock()
	}
	return nil
}

func (d *NetworkDriver) drainLocked() ([]rootstorage.CommittedEvent, []myraft.Message, error) {
	if d.node == nil {
		return nil, nil, nil
	}
	var committed []rootstorage.CommittedEvent
	var outbound []myraft.Message
	for d.node.raw.HasReady() {
		rd := d.node.raw.Ready()
		if !myraft.IsEmptyHardState(rd.HardState) {
			if err := d.node.storage.SetHardState(rd.HardState); err != nil {
				return nil, nil, err
			}
		}
		if !myraft.IsEmptySnap(rd.Snapshot) {
			if err := d.node.storage.ApplySnapshot(rd.Snapshot); err != nil {
				return nil, nil, err
			}
		}
		if len(rd.Entries) > 0 {
			if err := d.node.storage.Append(rd.Entries); err != nil {
				return nil, nil, err
			}
		}
		for _, entry := range rd.CommittedEntries {
			if entry.Type != myraft.EntryNormal || len(entry.Data) == 0 {
				continue
			}
			rec, err := unmarshalCommittedEvent(entry.Data)
			if err != nil {
				return nil, nil, err
			}
			d.records = append(d.records, rec)
			committed = append(committed, rec)
		}
		outbound = append(outbound, rd.Messages...)
		d.node.raw.Advance(rd)
	}
	return committed, outbound, nil
}

type networkEventLog struct{ driver *NetworkDriver }

func (l networkEventLog) Load(offset int64) ([]rootstorage.CommittedEvent, error) {
	l.driver.mu.Lock()
	defer l.driver.mu.Unlock()
	if offset <= 0 || int(offset) > len(l.driver.records) {
		return rootstorage.CloneCommittedEvents(l.driver.records), nil
	}
	return rootstorage.CloneCommittedEvents(l.driver.records[int(offset):]), nil
}

func (l networkEventLog) Append(records ...rootstorage.CommittedEvent) (int64, error) {
	l.driver.mu.Lock()
	if l.driver.node == nil {
		l.driver.mu.Unlock()
		return 0, fmt.Errorf("meta/root/backend/replicated: network driver is closed")
	}
	if l.driver.node.raw.Status().RaftState != myraft.StateLeader {
		l.driver.mu.Unlock()
		return 0, fmt.Errorf("meta/root/backend/replicated: node %d is not leader", l.driver.id)
	}
	for _, rec := range records {
		payload, err := marshalCommittedEvent(rec)
		if err != nil {
			l.driver.mu.Unlock()
			return 0, err
		}
		if err := l.driver.node.raw.Propose(payload); err != nil {
			l.driver.mu.Unlock()
			return 0, err
		}
		_, outbound, err := l.driver.drainLocked()
		if err != nil {
			l.driver.mu.Unlock()
			return 0, err
		}
		l.driver.mu.Unlock()
		if err := l.driver.sendMessages(outbound); err != nil {
			return 0, err
		}
		l.driver.mu.Lock()
	}
	size := int64(len(l.driver.records))
	l.driver.mu.Unlock()
	return size, nil
}

func (l networkEventLog) Compact(records []rootstorage.CommittedEvent) error {
	l.driver.mu.Lock()
	defer l.driver.mu.Unlock()
	return l.driver.rebuildLocked(records)
}

func (l networkEventLog) Size() (int64, error) {
	l.driver.mu.Lock()
	defer l.driver.mu.Unlock()
	return int64(len(l.driver.records)), nil
}

func (l networkEventLog) Close() error {
	return l.driver.Close()
}

type networkCheckpointStore struct{ driver *NetworkDriver }

func (s networkCheckpointStore) Load() (rootstorage.Checkpoint, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	return rootstorage.CloneCheckpoint(s.driver.checkpoint), nil
}

func (s networkCheckpointStore) Save(checkpoint rootstorage.Checkpoint) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	s.driver.checkpoint = rootstorage.CloneCheckpoint(checkpoint)
	return nil
}

func (s networkCheckpointStore) Close() error {
	return s.driver.Close()
}

type networkNode struct {
	id      uint64
	peerIDs []uint64
	storage *myraft.MemoryStorage
	raw     *myraft.RawNode
}

func newNetworkNode(cfg NetworkConfig, handler MessageHandler) (*networkNode, error) {
	storage := myraft.NewMemoryStorage()
	rcfg := &myraft.Config{
		ID:              cfg.ID,
		ElectionTick:    5,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
		PreVote:         true,
	}
	raw, err := myraft.NewRawNode(rcfg)
	if err != nil {
		return nil, err
	}
	peers := make([]myraft.Peer, 0, len(cfg.PeerIDs))
	for _, id := range cfg.PeerIDs {
		peers = append(peers, myraft.Peer{ID: id})
	}
	if err := raw.Bootstrap(peers); err != nil {
		return nil, err
	}
	cfg.Transport.SetHandler(handler)
	return &networkNode{
		id:      cfg.ID,
		peerIDs: slices.Clone(cfg.PeerIDs),
		storage: storage,
		raw:     raw,
	}, nil
}

func (d *NetworkDriver) sendMessages(msgs []myraft.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	return d.transport.Send(msgs...)
}
