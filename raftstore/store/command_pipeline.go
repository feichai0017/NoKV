package store

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type commandProposal struct {
	ch chan proposalResult
}

type proposalResult struct {
	resp *pb.RaftCmdResponse
	err  error
}

type commandPipeline struct {
	mu        sync.Mutex
	seq       uint64
	proposals map[uint64]*commandProposal
	applier   func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error)
	legacy    atomic.Uint64
}

func newCommandPipeline(applier func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error)) *commandPipeline {
	return &commandPipeline{
		proposals: make(map[uint64]*commandProposal),
		applier:   applier,
	}
}

func (cp *commandPipeline) nextProposalID() uint64 {
	if cp == nil {
		return 0
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.seq++
	return cp.seq
}

func (cp *commandPipeline) registerProposal(id uint64) *commandProposal {
	if cp == nil || id == 0 {
		return nil
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	prop := &commandProposal{ch: make(chan proposalResult, 1)}
	cp.proposals[id] = prop
	return prop
}

func (cp *commandPipeline) removeProposal(id uint64) {
	if cp == nil || id == 0 {
		return
	}
	cp.mu.Lock()
	delete(cp.proposals, id)
	cp.mu.Unlock()
}

func (cp *commandPipeline) completeProposal(id uint64, resp *pb.RaftCmdResponse, err error) {
	if cp == nil || id == 0 {
		return
	}
	cp.mu.Lock()
	prop := cp.proposals[id]
	delete(cp.proposals, id)
	cp.mu.Unlock()
	if prop == nil {
		return
	}
	prop.ch <- proposalResult{resp: resp, err: err}
	close(prop.ch)
}

func (cp *commandPipeline) applyEntries(entries []myraft.Entry, fallback peer.ApplyFunc) error {
	if cp == nil {
		return fmt.Errorf("commandPipeline: pipeline is nil")
	}
	for _, entry := range entries {
		if entry.Type != myraft.EntryNormal {
			if fallback != nil {
				if err := fallback([]myraft.Entry{entry}); err != nil {
					return err
				}
			}
			continue
		}
		if len(entry.Data) == 0 {
			continue
		}
		req, isCmd, err := command.Decode(entry.Data)
		if err != nil {
			return err
		}
		if !isCmd {
			cp.legacy.Add(1)
			if fallback != nil {
				if err := fallback([]myraft.Entry{entry}); err != nil {
					return err
				}
			}
			continue
		}
		if cp.applier == nil {
			if fallback != nil {
				if err := fallback([]myraft.Entry{entry}); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("commandPipeline: apply without handler")
		}
		resp, applyErr := cp.applier(req)
		if applyErr != nil {
			cp.completeProposal(req.GetHeader().GetRequestId(), nil, applyErr)
			continue
		}
		cp.completeProposal(req.GetHeader().GetRequestId(), resp, nil)
	}
	return nil
}

func (cp *commandPipeline) legacyFallbackCount() uint64 {
	if cp == nil {
		return 0
	}
	return cp.legacy.Load()
}
