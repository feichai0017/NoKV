package raft_test

import (
	"math"
	"testing"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/stretchr/testify/require"
)

type testNode struct {
	id      uint64
	cfg     *myraft.Config
	storage *myraft.MemoryStorage
	raw     *myraft.RawNode
}

func newTestNode(t *testing.T, id uint64) *testNode {
	t.Helper()
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
	require.NoError(t, err)
	require.NoError(t, raw.Bootstrap([]myraft.Peer{{ID: id}}))

	node := &testNode{
		id:      id,
		cfg:     cfg,
		storage: storage,
		raw:     raw,
	}
	node.processReady(t) // drain any bootstrap Ready.
	return node
}

func (n *testNode) processReady(t *testing.T) []myraft.Entry {
	t.Helper()
	var committed []myraft.Entry
	for n.raw.HasReady() {
		rd := n.raw.Ready()
		if !myraft.IsEmptyHardState(rd.HardState) {
			require.NoError(t, n.storage.SetHardState(rd.HardState))
		}
		if !myraft.IsEmptySnap(rd.Snapshot) {
			require.NoError(t, n.storage.ApplySnapshot(rd.Snapshot))
		}
		if len(rd.Entries) > 0 {
			require.NoError(t, n.storage.Append(rd.Entries))
		}
		if len(rd.CommittedEntries) > 0 {
			committed = append(committed, rd.CommittedEntries...)
		}
		// Deliver self-directed messages directly into the state machine.
		for _, msg := range rd.Messages {
			if msg.To == n.id {
				require.NoError(t, n.raw.Step(msg))
			}
		}
		n.raw.Advance(rd)
	}
	return committed
}

func TestRawNodeSingleNodeCampaign(t *testing.T) {
	node := newTestNode(t, 1)
	require.NoError(t, node.raw.Campaign())
	node.processReady(t)

	status := node.raw.Status()
	require.Equal(t, myraft.StateLeader, status.RaftState, "node should become leader after campaign")
}

func TestRawNodeSingleNodeProposeAndCommit(t *testing.T) {
	node := newTestNode(t, 1)
	require.NoError(t, node.raw.Campaign())
	node.processReady(t)
	require.Equal(t, myraft.StateLeader, node.raw.Status().RaftState)

	payload := []byte("tinykv-raft-entry")
	require.NoError(t, node.raw.Propose(payload))

	entries := node.processReady(t)
	require.NotEmpty(t, entries, "expected committed entries")
	last := entries[len(entries)-1]
	require.Equal(t, payload, last.Data)
	require.Equal(t, myraft.EntryNormal, last.Type)
}
