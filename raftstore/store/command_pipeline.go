package store

import (
	"fmt"
	"sync"

	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
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

func (cp *commandPipeline) registerProposal(id uint64) (*commandProposal, error) {
	if cp == nil || id == 0 {
		return nil, nil
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if _, exists := cp.proposals[id]; exists {
		return nil, fmt.Errorf("commandPipeline: duplicate proposal id %d", id)
	}
	prop := &commandProposal{ch: make(chan proposalResult, 1)}
	cp.proposals[id] = prop
	return prop, nil
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

func (cp *commandPipeline) applyEntries(entries []myraft.Entry) error {
	if cp == nil {
		return fmt.Errorf("commandPipeline: pipeline is nil")
	}
	for _, entry := range entries {
		if entry.Type != myraft.EntryNormal {
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
			return fmt.Errorf("commandPipeline: unsupported legacy raft payload")
		}
		if cp.applier == nil {
			return fmt.Errorf("commandPipeline: apply without handler")
		}
		resp, applyErr := cp.applier(req)
		if applyErr != nil {
			requestID := req.GetHeader().GetRequestId()
			cp.completeProposal(requestID, nil, applyErr)
			return fmt.Errorf("commandPipeline: apply request %d failed: %w", requestID, applyErr)
		}
		cp.completeProposal(req.GetHeader().GetRequestId(), resp, nil)
	}
	return nil
}
