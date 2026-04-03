package rootraft

import rootpkg "github.com/feichai0017/NoKV/meta/root"

// SingleNodeRoot is a bootstrapped one-node metadata root built on the same
// raft state machine and command path as the future quorum-backed root.
type SingleNodeRoot struct {
	node *Node
}

var _ rootpkg.Root = (*SingleNodeRoot)(nil)

func OpenSingleNode(cfg Config) (*SingleNodeRoot, error) {
	cfg.Bootstrap = true
	node, err := OpenNode(cfg, Checkpoint{}, nil)
	if err != nil {
		return nil, err
	}
	if err := node.Campaign(); err != nil {
		return nil, err
	}
	return &SingleNodeRoot{node: node}, nil
}

func (r *SingleNodeRoot) Current() (rootpkg.State, error) {
	if r == nil || r.node == nil {
		return rootpkg.State{}, nil
	}
	return r.node.Current(), nil
}

func (r *SingleNodeRoot) ReadSince(cursor rootpkg.Cursor) ([]rootpkg.Event, rootpkg.Cursor, error) {
	if r == nil || r.node == nil {
		return nil, rootpkg.Cursor{}, nil
	}
	events, tail := r.node.ReadSince(cursor)
	return events, tail, nil
}

func (r *SingleNodeRoot) Append(events ...rootpkg.Event) (rootpkg.CommitInfo, error) {
	if r == nil || r.node == nil {
		return rootpkg.CommitInfo{}, nil
	}
	var commit rootpkg.CommitInfo
	for _, event := range events {
		ci, err := r.node.ProposeEvent(event)
		if err != nil {
			return rootpkg.CommitInfo{}, err
		}
		commit = ci
	}
	if len(events) == 0 {
		state := r.node.Current()
		return rootpkg.CommitInfo{Cursor: state.LastCommitted, State: state}, nil
	}
	return commit, nil
}

func (r *SingleNodeRoot) FenceAllocator(kind rootpkg.AllocatorKind, min uint64) (uint64, error) {
	if r == nil || r.node == nil {
		return 0, nil
	}
	ci, err := r.node.ProposeFence(kind, min)
	if err != nil {
		return 0, err
	}
	switch kind {
	case rootpkg.AllocatorKindID:
		return ci.State.IDFence, nil
	case rootpkg.AllocatorKindTSO:
		return ci.State.TSOFence, nil
	default:
		return 0, nil
	}
}

func (r *SingleNodeRoot) Close() error { return nil }
