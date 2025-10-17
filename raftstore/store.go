package raftstore

import (
	"errors"
	"path/filepath"
	"sync"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/wal"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

type readyStorage interface {
	myraft.Storage
	Append([]myraft.Entry) error
	ApplySnapshot(myraft.Snapshot) error
	SetHardState(myraft.HardState) error
}

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
	StorageDir string
	WAL        *wal.Manager
	Manifest   *manifest.Manager
	GroupID    uint64
}

// Peer wraps a RawNode with simple storage and apply plumbing.
type Peer struct {
	mu        sync.Mutex
	id        uint64
	node      *myraft.RawNode
	storage   readyStorage
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
	var storage readyStorage

	groupID := cfg.GroupID
	if groupID == 0 {
		groupID = 1
	}

	switch {
	case cfg.WAL != nil && cfg.Manifest != nil:
		var err error
		storage, err = openWalStorage(WalStorageConfig{
			GroupID:  groupID,
			WAL:      cfg.WAL,
			Manifest: cfg.Manifest,
		})
		if err != nil {
			return nil, err
		}
	case cfg.WAL != nil || cfg.Manifest != nil:
		return nil, errors.New("raftstore: WAL and manifest must both be provided")
	case cfg.StorageDir != "":
		dir := filepath.Clean(cfg.StorageDir)
		var err error
		storage, err = OpenDiskStorage(dir)
		if err != nil {
			return nil, err
		}
	default:
		storage = myraft.NewMemoryStorage()
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
	}
	return nil
}
