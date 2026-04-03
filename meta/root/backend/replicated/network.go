package replicated

import (
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"time"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	rootfile "github.com/feichai0017/NoKV/meta/root/storage/file"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/vfs"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const defaultNetworkTickInterval = 100 * time.Millisecond

// NetworkConfig wires one local raft node to a transport and a fixed peer set.
type NetworkConfig struct {
	ID           uint64
	WorkDir      string
	PeerIDs      []uint64
	Transport    Transport
	TickInterval time.Duration
}

// NetworkDriver hosts one local raft rawnode and exchanges messages through a
// transport, which is the first real landing point for multi-process metadata
// replication.
type NetworkDriver struct {
	mu        sync.Mutex
	closeOnce sync.Once
	id        uint64
	workdir   string
	storage   rootstorage.Substrate
	node      *networkNode
	transport Transport
	stopCh    chan struct{}
	wg        sync.WaitGroup
	notifyCh  chan struct{}
	latest    rootstorage.TailToken
}

// NewNetworkDriver creates one transport-backed local metadata replication node.
func NewNetworkDriver(cfg NetworkConfig) (*NetworkDriver, error) {
	if cfg.ID == 0 {
		return nil, fmt.Errorf("meta/root/backend/replicated: network driver id must be non-zero")
	}
	if cfg.WorkDir == "" {
		return nil, fmt.Errorf("meta/root/backend/replicated: network driver workdir is required")
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
		workdir:   cfg.WorkDir,
		storage:   rootfile.NewStore(vfs.Ensure(nil), cfg.WorkDir),
		transport: cfg.Transport,
		stopCh:    make(chan struct{}),
		notifyCh:  make(chan struct{}, 1),
	}
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		return nil, err
	}
	if observed, err := rootstorage.ObserveCommitted(driver.storage, 0); err == nil {
		driver.latest.Cursor = observed.LastCursor()
		if driver.latest.Cursor != (rootstate.Cursor{}) || observed.Checkpoint.TailOffset != 0 || len(observed.Tail.Records) > 0 {
			driver.latest.Revision = 1
		}
	}
	node, err := newNetworkNode(cfg, driver.handleTransportMessage)
	if err != nil {
		return nil, err
	}
	driver.node = node
	driver.mu.Lock()
	_, outbound, err := driver.drainLocked()
	driver.mu.Unlock()
	if err != nil {
		_ = driver.Close()
		return nil, err
	}
	if err := driver.sendMessages(outbound); err != nil {
		_ = driver.Close()
		return nil, err
	}
	driver.wg.Add(1)
	go driver.tickLoop(cfg.TickInterval)
	return driver, nil
}

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
	observed, _ := rootstorage.ObserveCommitted(d.storage, 0)
	return DriverState{
		Checkpoint: rootstorage.CloneCheckpoint(observed.Checkpoint),
		Records:    rootstorage.CloneCommittedEvents(observed.Tail.Records),
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

func (d *NetworkDriver) WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
	if timeout <= 0 {
		timeout = 200 * time.Millisecond
	}
	d.mu.Lock()
	current := d.latest
	notify := d.notifyCh
	advance, err := d.currentTailLocked()
	d.mu.Unlock()
	if err != nil {
		return rootstorage.TailAdvance{}, err
	}
	if current.AdvancedSince(after) {
		return advance, nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-d.stopCh:
		d.mu.Lock()
		defer d.mu.Unlock()
		return rootstorage.TailAdvance{Token: d.latest}, fmt.Errorf("meta/root/backend/replicated: network driver is closed")
	case <-notify:
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.currentTailLocked()
	case <-timer.C:
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.currentTailLocked()
	}
}

func (d *NetworkDriver) InstallBootstrap(checkpoint rootstorage.Checkpoint, stream rootstorage.CommittedTail) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.storage.InstallBootstrap(checkpoint, stream); err != nil {
		return err
	}
	d.bumpLatestLocked(stream.TailCursor(checkpoint.Snapshot.State.LastCommitted))
	d.signalLocked()
	return nil
}

func (d *NetworkDriver) Close() error {
	var err error
	d.closeOnce.Do(func() {
		close(d.stopCh)
		d.wg.Wait()
		d.mu.Lock()
		transport := d.transport
		if d.transport != nil {
			d.transport = nil
		}
		d.node = nil
		d.mu.Unlock()
		if transport != nil {
			err = transport.Close()
		}
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

func (d *NetworkDriver) drainLocked() ([]rootstorage.CommittedEvent, []myraft.Message, error) {
	if d.node == nil {
		return nil, nil, nil
	}
	var committed []rootstorage.CommittedEvent
	var outbound []myraft.Message
	for d.node.raw.HasReady() {
		rd := d.node.raw.Ready()
		persistProtocol := false
		if !myraft.IsEmptyHardState(rd.HardState) {
			if err := d.node.storage.SetHardState(rd.HardState); err != nil {
				return nil, nil, err
			}
			persistProtocol = true
		}
		if !myraft.IsEmptySnap(rd.Snapshot) {
			if err := d.node.storage.ApplySnapshot(rd.Snapshot); err != nil {
				return nil, nil, err
			}
			persistProtocol = true
		}
		if len(rd.Entries) > 0 {
			if err := d.node.storage.Append(rd.Entries); err != nil {
				return nil, nil, err
			}
			persistProtocol = true
		}
		for _, entry := range rd.CommittedEntries {
			if entry.Type != myraft.EntryNormal || len(entry.Data) == 0 {
				continue
			}
			rec, err := unmarshalCommittedEvent(entry.Data)
			if err != nil {
				return nil, nil, err
			}
			committed = append(committed, rec)
		}
		if persistProtocol {
			state, err := captureProtocolState(d.node.storage)
			if err != nil {
				return nil, nil, err
			}
			if err := saveProtocolState(d.workdir, state); err != nil {
				return nil, nil, err
			}
		}
		outbound = append(outbound, rd.Messages...)
		d.node.raw.Advance(rd)
	}
	if len(committed) > 0 {
		if _, err := d.storage.AppendCommitted(committed...); err != nil {
			return nil, nil, err
		}
		d.bumpLatestLocked(committed[len(committed)-1].Cursor)
		d.signalLocked()
	}
	return committed, outbound, nil
}

func (d *NetworkDriver) LoadCheckpoint() (rootstorage.Checkpoint, error) {
	return d.storage.LoadCheckpoint()
}

func (d *NetworkDriver) SaveCheckpoint(checkpoint rootstorage.Checkpoint) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.storage.SaveCheckpoint(checkpoint); err != nil {
		return err
	}
	observed, err := rootstorage.ObserveCommitted(d.storage, 0)
	if err != nil {
		return err
	}
	d.bumpLatestLocked(observed.LastCursor())
	d.signalLocked()
	return nil
}

func (d *NetworkDriver) ReadCommitted(offset int64) (rootstorage.CommittedTail, error) {
	return d.storage.ReadCommitted(offset)
}

func (d *NetworkDriver) AppendCommitted(records ...rootstorage.CommittedEvent) (int64, error) {
	d.mu.Lock()
	if d.node == nil {
		d.mu.Unlock()
		return 0, fmt.Errorf("meta/root/backend/replicated: network driver is closed")
	}
	if d.node.raw.Status().RaftState != myraft.StateLeader {
		d.mu.Unlock()
		return 0, fmt.Errorf("meta/root/backend/replicated: node %d is not leader", d.id)
	}
	for _, rec := range records {
		payload, err := marshalCommittedEvent(rec)
		if err != nil {
			d.mu.Unlock()
			return 0, err
		}
		if err := d.node.raw.Propose(payload); err != nil {
			d.mu.Unlock()
			return 0, err
		}
		_, outbound, err := d.drainLocked()
		if err != nil {
			d.mu.Unlock()
			return 0, err
		}
		d.mu.Unlock()
		if err := d.sendMessages(outbound); err != nil {
			return 0, err
		}
		d.mu.Lock()
	}
	size, err := d.storage.Size()
	d.mu.Unlock()
	return size, err
}

func (d *NetworkDriver) CompactCommitted(stream rootstorage.CommittedTail) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.storage.CompactCommitted(stream); err != nil {
		return err
	}
	d.bumpLatestLocked(stream.TailCursor(d.latest.Cursor))
	d.signalLocked()
	return nil
}

func (d *NetworkDriver) Size() (int64, error) {
	return d.storage.Size()
}

func (d *NetworkDriver) currentTailLocked() (rootstorage.TailAdvance, error) {
	observed, err := rootstorage.ObserveCommitted(d.storage, 0)
	if err != nil {
		return rootstorage.TailAdvance{}, err
	}
	d.latest.Cursor = observed.LastCursor()
	return rootstorage.TailAdvance{
		Token: d.latest,
		Tail:  observed.Tail,
	}, nil
}

func (d *NetworkDriver) bumpLatestLocked(cursor rootstate.Cursor) {
	d.latest.Cursor = cursor
	d.latest.Revision++
}

func (d *NetworkDriver) signalLocked() {
	select {
	case d.notifyCh <- struct{}{}:
	default:
	}
}

type networkNode struct {
	id      uint64
	peerIDs []uint64
	storage *myraft.MemoryStorage
	raw     *myraft.RawNode
}

func newNetworkNode(cfg NetworkConfig, handler MessageHandler) (*networkNode, error) {
	storage := myraft.NewMemoryStorage()
	state, err := loadProtocolState(cfg.WorkDir)
	if err != nil {
		return nil, err
	}
	if !myraft.IsEmptySnap(state.Snapshot) {
		if err := storage.ApplySnapshot(state.Snapshot); err != nil {
			return nil, err
		}
	}
	if !myraft.IsEmptyHardState(state.HardState) {
		if err := storage.SetHardState(state.HardState); err != nil {
			return nil, err
		}
	}
	if len(state.Entries) > 0 {
		if err := storage.Append(state.Entries); err != nil {
			return nil, err
		}
	}
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
	restarted := !myraft.IsEmptyHardState(state.HardState) || !myraft.IsEmptySnap(state.Snapshot) || len(state.Entries) > 0
	if !restarted {
		peers := make([]myraft.Peer, 0, len(cfg.PeerIDs))
		for _, id := range cfg.PeerIDs {
			peers = append(peers, myraft.Peer{ID: id})
		}
		if err := raw.Bootstrap(peers); err != nil {
			return nil, err
		}
	} else {
		for _, id := range cfg.PeerIDs {
			raw.ApplyConfChange(raftpb.ConfChange{NodeID: id, Type: raftpb.ConfChangeAddNode}.AsV2())
		}
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
