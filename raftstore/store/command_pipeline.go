package store

import (
	"fmt"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"sync"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
)

type commandRuntime struct {
	apply   func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)
	pipe    *commandPipeline
	timeout time.Duration
}

type commandProposal struct {
	ch chan proposalResult
}

type proposalResult struct {
	resp *raftcmdpb.RaftCmdResponse
	err  error
}

type commandPipeline struct {
	mu        sync.Mutex
	seq       uint64
	proposals map[uint64]*commandProposal
	applier   func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)
}

type applyEventEmitter func(myraft.Entry, *raftcmdpb.RaftCmdRequest, *raftcmdpb.RaftCmdResponse)

func newCommandPipeline(applier func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)) *commandPipeline {
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

func (cp *commandPipeline) completeProposal(id uint64, resp *raftcmdpb.RaftCmdResponse, err error) {
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

func (cp *commandPipeline) applyEntries(entries []myraft.Entry, emitters ...applyEventEmitter) error {
	if cp == nil {
		return fmt.Errorf("commandPipeline: pipeline is nil")
	}
	var emit applyEventEmitter
	if len(emitters) > 0 {
		emit = emitters[0]
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
			return fmt.Errorf("commandPipeline: unsupported unframed raft payload")
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
		if emit != nil {
			emit(entry, req, resp)
		}
		cp.completeProposal(req.GetHeader().GetRequestId(), resp, nil)
	}
	return nil
}

func (s *Store) applyEntries(entries []myraft.Entry) error {
	if s == nil {
		return errNilStore
	}
	if s.cmds == nil || s.cmds.pipe == nil {
		return errCommandApplyWithoutHandler
	}
	return s.cmds.pipe.applyEntries(entries, s.emitApplyEvents)
}
