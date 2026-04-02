package rootraft

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	myraft "github.com/feichai0017/NoKV/raft"
)

// Node binds the raft algorithm, root state machine, and transport together.
type Node struct {
	cfg        Config
	raw        *myraft.RawNode
	storage    *Storage
	machine    *StateMachine
	transport  Transport
	checkpoint CheckpointStore
}

func OpenNode(cfg Config, checkpoint Checkpoint, transport Transport) (*Node, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	var checkpointStore CheckpointStore
	var storage *Storage
	if cfg.WorkDir != "" {
		checkpointStore, err = OpenFileCheckpointStore(cfg.WorkDir, cfg.FS)
		if err != nil {
			return nil, err
		}
		checkpoint, err = checkpointStore.Load()
		if err != nil {
			return nil, err
		}
		storage, err = OpenStorage(cfg.WorkDir, cfg.FS)
		if err != nil {
			return nil, err
		}
	} else {
		storage = NewStorage()
	}
	raw, err := myraft.NewRawNode(&myraft.Config{
		ID:              cfg.NodeID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         storage,
		Applied:         checkpoint.State.LastCommitted.Index,
		MaxSizePerMsg:   cfg.MaxSizePerMsg,
		MaxInflightMsgs: cfg.MaxInflightMsgs,
		PreVote:         true,
	})
	if err != nil {
		return nil, err
	}
	if cfg.Bootstrap {
		peers := make([]myraft.Peer, 0, len(cfg.Peers))
		for _, peer := range cfg.Peers {
			peers = append(peers, myraft.Peer{ID: peer.ID})
		}
		if err := raw.Bootstrap(peers); err != nil {
			return nil, err
		}
	}
	if transport == nil {
		transport = nopTransport{}
	}
	node := &Node{cfg: cfg, raw: raw, storage: storage, machine: NewStateMachine(checkpoint), transport: transport, checkpoint: checkpointStore}
	if cfg.Bootstrap || raw.HasReady() {
		if err := node.drainReady(); err != nil {
			return nil, err
		}
	}
	return node, nil
}

func (n *Node) Campaign() error {
	if err := n.raw.Campaign(); err != nil {
		return err
	}
	return n.drainReady()
}

func (n *Node) Tick() error {
	n.raw.Tick()
	return n.drainReady()
}

func (n *Node) Step(msg myraft.Message) error {
	if err := n.raw.Step(msg); err != nil {
		return err
	}
	return n.drainReady()
}

func (n *Node) Current() rootpkg.State {
	return n.machine.Current()
}

func (n *Node) ID() uint64 {
	return n.cfg.NodeID
}

func (n *Node) Status() myraft.Status {
	return n.raw.Status()
}

func (n *Node) IsLeader() bool {
	return n.raw.Status().RaftState == myraft.StateLeader
}

func (n *Node) ReadSince(cursor rootpkg.Cursor) ([]rootpkg.Event, rootpkg.Cursor) {
	return n.machine.ReadSince(cursor)
}

func (n *Node) Snapshot() Checkpoint {
	return n.machine.Snapshot()
}

func (n *Node) ProposeEvent(event rootpkg.Event) (rootpkg.CommitInfo, error) {
	if n.raw.Status().Lead != n.cfg.NodeID {
		return rootpkg.CommitInfo{}, ErrNotLeader
	}
	payload, err := encodeEventCommand(event)
	if err != nil {
		return rootpkg.CommitInfo{}, err
	}
	if err := n.raw.Propose(payload); err != nil {
		return rootpkg.CommitInfo{}, err
	}
	return n.drainReadyWithCommit()
}

func (n *Node) ProposeFence(kind rootpkg.AllocatorKind, min uint64) (rootpkg.CommitInfo, error) {
	if n.raw.Status().Lead != n.cfg.NodeID {
		return rootpkg.CommitInfo{}, ErrNotLeader
	}
	if err := n.raw.Propose(encodeFenceCommand(kind, min)); err != nil {
		return rootpkg.CommitInfo{}, err
	}
	return n.drainReadyWithCommit()
}

func (n *Node) drainReady() error {
	_, err := n.drainReadyWithCommit()
	return err
}

func (n *Node) drainReadyWithCommit() (rootpkg.CommitInfo, error) {
	commit := rootpkg.CommitInfo{Cursor: n.machine.Current().LastCommitted, State: n.machine.Current()}
	for n.raw.HasReady() {
		rd := n.raw.Ready()
		if err := n.storage.AppendReady(rd); err != nil {
			return rootpkg.CommitInfo{}, err
		}
		for _, ent := range rd.CommittedEntries {
			cursor := rootpkg.Cursor{Term: ent.Term, Index: ent.Index}
			switch ent.Type {
			case myraft.EntryNormal:
				if len(ent.Data) == 0 {
					commit = n.machine.ApplyBarrier(cursor)
					continue
				}
				cmd, err := decodeCommand(ent.Data)
				if err != nil {
					return rootpkg.CommitInfo{}, err
				}
				commit = n.machine.ApplyCommand(cursor, cmd)
			case myraft.EntryConfChange, myraft.EntryConfChangeV2:
				commit = n.machine.ApplyBarrier(cursor)
			default:
				return rootpkg.CommitInfo{}, ErrUnsupportedType
			}
		}
		outbound := rd.Messages
		n.raw.Advance(rd)
		if n.checkpoint != nil {
			if err := n.checkpoint.Save(n.machine.Snapshot()); err != nil {
				return rootpkg.CommitInfo{}, err
			}
		}
		if len(outbound) > 0 {
			if err := n.transport.Send(outbound); err != nil {
				return rootpkg.CommitInfo{}, err
			}
		}
	}
	return commit, nil
}
