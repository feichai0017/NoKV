package raftstore

import (
	"errors"
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
)

// Transport is used by peers to send raft messages to other peers.
type Transport interface {
	Send(msg myraft.Message)
}

// ApplyFunc is invoked with committed raft log entries.
type ApplyFunc func(entries []myraft.Entry) error

// Config contains the parameters to start a raft peer.
type Config struct {
	RaftConfig myraft.Config
	Peers      []myraft.Peer
	Transport  Transport
	Apply      ApplyFunc
}

// Peer wraps a RawNode with simple storage and apply plumbing.
type Peer struct {
	mu        sync.Mutex
	id        uint64
	node      *myraft.RawNode
	storage   *myraft.MemoryStorage
	transport Transport
	apply     ApplyFunc
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
	storage := myraft.NewMemoryStorage()
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
		id:        raftCfg.ID,
		node:      node,
		storage:   storage,
		transport: cfg.Transport,
		apply:     cfg.Apply,
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
	p.mu.Lock()
	err := p.node.Bootstrap(peers)
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
	return p.processReady()
}

// Step forwards a received raft message to the underlying node.
func (p *Peer) Step(msg myraft.Message) error {
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
	if !myraft.IsEmptyHardState(rd.HardState) {
		if err := p.storage.SetHardState(rd.HardState); err != nil {
			return err
		}
	}
	if !myraft.IsEmptySnap(rd.Snapshot) {
		if err := p.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if len(rd.Entries) > 0 {
		if err := p.storage.Append(rd.Entries); err != nil {
			return err
		}
	}
	for _, msg := range rd.Messages {
		if p.transport != nil {
			p.transport.Send(msg)
		}
	}
	if len(rd.CommittedEntries) > 0 && p.apply != nil {
		if err := p.apply(rd.CommittedEntries); err != nil {
			return err
		}
	}
	return nil
}
