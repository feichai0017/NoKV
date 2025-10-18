package peer

import (
	"errors"
	"sync"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/engine"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	"github.com/feichai0017/NoKV/raftstore/transport"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

// Peer wraps a RawNode with simple storage and apply plumbing.
type Peer struct {
	mu               sync.Mutex
	id               uint64
	node             *myraft.RawNode
	storage          engine.PeerStorage
	transport        transport.Transport
	apply            ApplyFunc
	raftLog          *raftLogTracker
	snapshotQueue    *snapshotResendQueue
	logRetainEntries uint64
}

// NewPeer constructs a peer using the provided configuration. The caller must
// register the peer with the transport before invoking Bootstrap.
func NewPeer(cfg *Config) (*Peer, error) {
	if cfg == nil {
		return nil, errors.New("raftstore: config is nil")
	}
	if cfg.Transport == nil {
		return nil, errors.New("raftstore: transport must be provided")
	}
	if cfg.Apply == nil {
		return nil, errors.New("raftstore: apply function must be provided")
	}
	storage, err := ResolveStorage(cfg)
	if err != nil {
		return nil, err
	}
	raftCfg := cfg.RaftConfig
	if raftCfg.ID == 0 {
		return nil, errors.New("raftstore: raft config must specify ID")
	}
	raftCfg.Storage = storage
	node, err := myraft.NewRawNode(&raftCfg)
	if err != nil {
		return nil, err
	}
	peer := &Peer{
		id:               raftCfg.ID,
		node:             node,
		storage:          storage,
		transport:        cfg.Transport,
		apply:            cfg.Apply,
		raftLog:          newRaftLogTracker(cfg.Manifest, cfg.WAL, nonZeroGroupID(cfg.GroupID)),
		snapshotQueue:    newSnapshotResendQueue(),
		logRetainEntries: cfg.LogRetainEntries,
	}
	if peer.logRetainEntries == 0 {
		peer.logRetainEntries = defaultLogRetainEntries
	}
	return peer, nil
}

// ID returns the peer ID.
func (p *Peer) ID() uint64 {
	return p.id
}

// Bootstrap injects the initial configuration into the node. It must be called
// after the peer has been registered with the transport.
func (p *Peer) Bootstrap(peers []myraft.Peer) error {
	if len(peers) == 0 {
		return nil
	}
	last, err := p.storage.LastIndex()
	if err == nil && last > 0 {
		return nil
	}
	if hs, cs, err := p.storage.InitialState(); err == nil {
		if !myraft.IsEmptyHardState(hs) || len(cs.Voters) > 0 || len(cs.Learners) > 0 {
			return nil
		}
	}
	p.mu.Lock()
	err = p.node.Bootstrap(peers)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// Tick increments the logical clock, driving elections and heartbeats.
func (p *Peer) Tick() error {
	p.mu.Lock()
	p.node.Tick()
	p.mu.Unlock()
	p.resendPendingSnapshots()
	return p.processReady()
}

// Step forwards a received raft message to the underlying node.
func (p *Peer) Step(msg myraft.Message) error {
	if msg.Type == myraft.MsgSnapshotStatus && !msg.Reject {
		if q := p.snapshotQueue; q != nil {
			q.drop(msg.From)
		}
	}
	p.mu.Lock()
	err := p.node.Step(msg)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// Propose submits application data to the raft log.
func (p *Peer) Propose(data []byte) error {
	p.mu.Lock()
	err := p.node.Propose(data)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// Campaign transitions this peer into candidate state.
func (p *Peer) Campaign() error {
	p.mu.Lock()
	err := p.node.Campaign()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// Flush forces processing of any pending Ready state.
func (p *Peer) Flush() error {
	return p.processReady()
}

// Status returns the raft status snapshot.
func (p *Peer) Status() myraft.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.node.Status()
}

func (p *Peer) processReady() error {
	for {
		p.mu.Lock()
		hasReady := p.node.HasReady()
		var rd myraft.Ready
		if hasReady {
			rd = p.node.Ready()
		}
		p.mu.Unlock()

		if !hasReady {
			return nil
		}

		if err := p.handleReady(rd); err != nil {
			return err
		}

		p.mu.Lock()
		p.node.Advance(rd)
		p.mu.Unlock()
	}
}

func (p *Peer) handleReady(rd myraft.Ready) error {
	if info := p.raftLog; info != nil {
		info.setInjected(failpoints.ShouldFailBeforeStorage())
	}

	if !myraft.IsEmptyHardState(rd.HardState) {
		if err := p.raftLog.injectFailure("before_hard_state"); err != nil {
			return err
		}
		if err := p.storage.SetHardState(rd.HardState); err != nil {
			return err
		}
		if info := p.raftLog; info != nil {
			info.capturePointer(manifest.RaftLogPointer{
				GroupID:      info.groupID,
				AppliedIndex: rd.HardState.Commit,
				AppliedTerm:  rd.HardState.Term,
			})
		}
	}
	if !myraft.IsEmptySnap(rd.Snapshot) {
		if err := p.raftLog.injectFailure("before_snapshot"); err != nil {
			return err
		}
		if err := p.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
		if info := p.raftLog; info != nil {
			meta := rd.Snapshot.Metadata
			info.capturePointer(manifest.RaftLogPointer{
				GroupID:       info.groupID,
				SnapshotIndex: meta.Index,
				SnapshotTerm:  meta.Term,
			})
		}
	}
	if len(rd.Entries) > 0 {
		if err := p.raftLog.injectFailure("before_entries"); err != nil {
			return err
		}
		if err := p.storage.Append(rd.Entries); err != nil {
			return err
		}
		if info := p.raftLog; info != nil {
			last := rd.Entries[len(rd.Entries)-1]
			info.capturePointer(manifest.RaftLogPointer{
				GroupID:      info.groupID,
				AppliedIndex: last.Index,
				AppliedTerm:  last.Term,
			})
		}
	}
	for _, msg := range rd.Messages {
		if msg.Type == myraft.MsgSnapshot {
			if q := p.snapshotQueue; q != nil {
				q.record(msg)
			}
		}
		if p.transport != nil {
			p.transport.Send(msg)
		}
	}
	if len(rd.CommittedEntries) > 0 {
		var toApply []myraft.Entry
		for _, entry := range rd.CommittedEntries {
			switch entry.Type {
			case myraft.EntryConfChange:
				var cc raftpb.ConfChange
				if err := cc.Unmarshal(entry.Data); err != nil {
					return err
				}
				p.node.ApplyConfChange(cc.AsV2())
			case myraft.EntryConfChangeV2:
				var cc raftpb.ConfChangeV2
				if err := cc.Unmarshal(entry.Data); err != nil {
					return err
				}
				p.node.ApplyConfChange(cc)
			default:
				toApply = append(toApply, entry)
			}
		}
		if len(toApply) > 0 && p.apply != nil {
			if err := p.apply(toApply); err != nil {
				return err
			}
		}
		lastApplied := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		if err := p.maybeCompact(lastApplied); err != nil {
			return err
		}
	}
	return nil
}

func (p *Peer) maybeCompact(applied uint64) error {
	if applied == 0 {
		return nil
	}
	ws, ok := p.storage.(*engine.WALStorage)
	if !ok {
		return nil
	}
	return ws.MaybeCompact(applied, p.logRetainEntries)
}

// PopPendingSnapshot returns the most recent snapshot recorded during Ready
// handling, clearing the queue. It returns false when no snapshot is pending.
func (p *Peer) PopPendingSnapshot() (myraft.Snapshot, bool) {
	if p == nil || p.snapshotQueue == nil {
		return myraft.Snapshot{}, false
	}
	msg, ok := p.snapshotQueue.first()
	if !ok || myraft.IsEmptySnap(msg.Snapshot) {
		return myraft.Snapshot{}, false
	}
	p.snapshotQueue.drop(msg.To)
	return msg.Snapshot, true
}

// PendingSnapshot returns the snapshot retained for resend without removing it
// from the queue.
func (p *Peer) PendingSnapshot() (myraft.Snapshot, bool) {
	if p == nil || p.snapshotQueue == nil {
		return myraft.Snapshot{}, false
	}
	msg, ok := p.snapshotQueue.first()
	if !ok {
		return myraft.Snapshot{}, false
	}
	return msg.Snapshot, true
}

// ResendSnapshot attempts to resend the last snapshot destined for the provided
// peer ID. It returns true when a snapshot message was re-enqueued.
func (p *Peer) ResendSnapshot(to uint64) bool {
	if p == nil || p.transport == nil || p.snapshotQueue == nil || to == 0 {
		return false
	}
	msg, ok := p.snapshotQueue.pendingFor(to)
	if !ok {
		return false
	}
	p.transport.Send(msg)
	return true
}

func (p *Peer) resendPendingSnapshots() {
	if p == nil || p.transport == nil || p.snapshotQueue == nil {
		return
	}
	p.snapshotQueue.forEach(func(msg myraft.Message) {
		p.transport.Send(msg)
	})
}
