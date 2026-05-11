package store

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
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

type commandProposalKey struct {
	regionID  uint64
	peerID    uint64
	requestID uint64
}

func proposalKeyFromHeader(header *raftcmdpb.CmdHeader) commandProposalKey {
	if header == nil {
		return commandProposalKey{}
	}
	return commandProposalKey{
		regionID:  header.GetRegionId(),
		peerID:    header.GetPeerId(),
		requestID: header.GetRequestId(),
	}
}

func (k commandProposalKey) valid() bool {
	return k.regionID != 0 && k.peerID != 0 && k.requestID != 0
}

type commandPipeline struct {
	mu        sync.Mutex
	seq       uint64
	proposals map[commandProposalKey]*commandProposal
	applier   func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)
	parallel  int
}

type applyEventEmitter func(myraft.Entry, *raftcmdpb.RaftCmdRequest, *raftcmdpb.RaftCmdResponse)

func newCommandPipeline(applier func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error), parallelism ...int) *commandPipeline {
	parallel := 0
	if len(parallelism) > 0 {
		parallel = parallelism[0]
	}
	return &commandPipeline{
		proposals: make(map[commandProposalKey]*commandProposal),
		applier:   applier,
		parallel:  normalizeCommandApplyParallelism(parallel),
	}
}

func normalizeCommandApplyParallelism(parallelism int) int {
	if parallelism == 1 {
		return 1
	}
	if parallelism <= 0 {
		parallelism = runtime.GOMAXPROCS(0)
	}
	if parallelism < 1 {
		return 1
	}
	return parallelism
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

func (cp *commandPipeline) registerProposal(key commandProposalKey) (*commandProposal, error) {
	if cp == nil || !key.valid() {
		return nil, nil
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if _, exists := cp.proposals[key]; exists {
		return nil, fmt.Errorf("commandPipeline: duplicate proposal id region=%d peer=%d request=%d", key.regionID, key.peerID, key.requestID)
	}
	prop := &commandProposal{ch: make(chan proposalResult, 1)}
	cp.proposals[key] = prop
	return prop, nil
}

func (cp *commandPipeline) removeProposal(key commandProposalKey) {
	if cp == nil || !key.valid() {
		return
	}
	cp.mu.Lock()
	delete(cp.proposals, key)
	cp.mu.Unlock()
}

func (cp *commandPipeline) completeProposal(key commandProposalKey, resp *raftcmdpb.RaftCmdResponse, err error) {
	if cp == nil || !key.valid() {
		return
	}
	cp.mu.Lock()
	prop := cp.proposals[key]
	delete(cp.proposals, key)
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
	plans, err := commandApplyPlans(entries)
	if err != nil {
		return err
	}
	if len(plans) == 0 {
		return nil
	}
	if cp.parallel <= 1 || len(plans) == 1 {
		return cp.applyPlansSerial(plans, emit)
	}
	return cp.applyPlansParallel(plans, emit)
}

func (cp *commandPipeline) applyPlansSerial(plans []commandApplyPlan, emit applyEventEmitter) error {
	for _, plan := range plans {
		if err := cp.applyOne(plan, emit); err != nil {
			return err
		}
	}
	return nil
}

func (cp *commandPipeline) applyPlansParallel(plans []commandApplyPlan, emit applyEventEmitter) error {
	var wave []commandApplyPlan
	waveDeps := make(map[commandApplyDependencyKey]commandApplyDependencyMode)
	flushWave := func() error {
		if len(wave) == 0 {
			return nil
		}
		err := cp.applyWave(wave, emit)
		wave = nil
		clear(waveDeps)
		return err
	}
	for _, plan := range plans {
		if plan.barrier {
			if err := flushWave(); err != nil {
				return err
			}
			if err := cp.applyOne(plan, emit); err != nil {
				return err
			}
			continue
		}
		if commandApplyPlanConflicts(waveDeps, plan) {
			if err := flushWave(); err != nil {
				return err
			}
		}
		commandApplyPlanAddDependencies(waveDeps, plan)
		wave = append(wave, plan)
	}
	return flushWave()
}

type commandApplyResult struct {
	resp *raftcmdpb.RaftCmdResponse
	err  error
}

func (cp *commandPipeline) applyWave(wave []commandApplyPlan, emit applyEventEmitter) error {
	if len(wave) == 1 {
		return cp.applyOne(wave[0], emit)
	}
	if cp.applier == nil {
		return fmt.Errorf("commandPipeline: apply without handler")
	}
	results := make([]commandApplyResult, len(wave))
	var wg sync.WaitGroup
	sem := make(chan struct{}, cp.parallel)
	for i, plan := range wave {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int, plan commandApplyPlan) {
			defer wg.Done()
			defer func() { <-sem }()
			resp, err := cp.applier(plan.req)
			results[i] = commandApplyResult{resp: resp, err: err}
		}(i, plan)
	}
	wg.Wait()
	for i, result := range results {
		if result.err != nil {
			key := wave[i].proposalKey
			err := fmt.Errorf("commandPipeline: fatal parallel apply wave failed at request %d: %w", key.requestID, result.err)
			for _, plan := range wave {
				cp.completeProposal(plan.proposalKey, nil, err)
			}
			return err
		}
	}
	// Storage writes may complete out of order inside a conflict-free wave, but
	// observer delivery and client completion remain in raft-log order.
	for i, result := range results {
		plan := wave[i]
		if emit != nil {
			emit(plan.entry, plan.req, result.resp)
		}
		cp.completeProposal(plan.proposalKey, result.resp, nil)
	}
	return nil
}

func (cp *commandPipeline) applyOne(plan commandApplyPlan, emit applyEventEmitter) error {
	if cp.applier == nil {
		return fmt.Errorf("commandPipeline: apply without handler")
	}
	resp, applyErr := cp.applier(plan.req)
	if applyErr != nil {
		key := plan.proposalKey
		cp.completeProposal(key, nil, applyErr)
		return fmt.Errorf("commandPipeline: apply request %d failed: %w", key.requestID, applyErr)
	}
	if emit != nil {
		emit(plan.entry, plan.req, resp)
	}
	// Request IDs are generated by each store, not by the raft group. A
	// follower can therefore apply a foreign leader's entry with the same
	// request ID as a local in-flight proposal. The region/peer fence keeps
	// foreign raft entries from completing the wrong client waiter.
	cp.completeProposal(plan.proposalKey, resp, nil)
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
