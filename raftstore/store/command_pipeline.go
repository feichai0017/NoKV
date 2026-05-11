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
	orderSeq  uint64
	proposals map[commandProposalKey]*commandProposal
	applier   func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)
	parallel  int
	window    *commandApplyWindow
}

type applyEventEmitter func(myraft.Entry, *raftcmdpb.RaftCmdRequest, *raftcmdpb.RaftCmdResponse)

func newCommandPipeline(applier func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error), parallelism ...int) *commandPipeline {
	parallel := 0
	if len(parallelism) > 0 {
		parallel = parallelism[0]
	}
	cp := &commandPipeline{
		proposals: make(map[commandProposalKey]*commandProposal),
		applier:   applier,
		parallel:  normalizeCommandApplyParallelism(parallel),
	}
	cp.window = newCommandApplyWindow(cp, cp.parallel)
	return cp
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

func (cp *commandPipeline) assignPlanOrders(plans []commandApplyPlan) {
	if cp == nil || len(plans) == 0 {
		return
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	for i := range plans {
		cp.orderSeq++
		plans[i].order = cp.orderSeq
	}
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
	cp.assignPlanOrders(plans)
	if cp.parallel <= 1 || len(plans) == 1 {
		return cp.applyPlansSerial(plans, emit)
	}
	done := make(chan error, 1)
	if err := cp.window.submit(plans, emit, func(err error) {
		done <- err
	}); err != nil {
		return err
	}
	return <-done
}

func (cp *commandPipeline) applyEntriesAsync(entries []myraft.Entry, emit applyEventEmitter, done func(error)) error {
	if cp == nil {
		return fmt.Errorf("commandPipeline: pipeline is nil")
	}
	if done == nil {
		done = func(error) {}
	}
	plans, err := commandApplyPlans(entries)
	if err != nil {
		return err
	}
	if len(plans) == 0 {
		done(nil)
		return nil
	}
	cp.assignPlanOrders(plans)
	if cp.parallel <= 1 {
		go func() {
			done(cp.applyPlansSerial(plans, emit))
		}()
		return nil
	}
	return cp.window.submit(plans, emit, done)
}

func (cp *commandPipeline) applyPlansSerial(plans []commandApplyPlan, emit applyEventEmitter) error {
	for _, plan := range plans {
		if err := cp.applyOne(plan, emit); err != nil {
			return err
		}
	}
	return nil
}

type commandApplyWindow struct {
	mu            sync.Mutex
	cp            *commandPipeline
	parallel      int
	queue         []*commandApplyTask
	active        int
	activeBarrier bool
	activeDeps    map[commandApplyDependencyKey]commandApplyActiveDependency
	completed     map[uint64]*commandApplyTask
	nextComplete  uint64
	fatalErr      error
}

type commandApplyTask struct {
	plan         commandApplyPlan
	emit         applyEventEmitter
	group        *commandApplyGroup
	resp         *raftcmdpb.RaftCmdResponse
	err          error
	externalDone bool
}

type commandApplyActiveDependency struct {
	readers int
	writers int
}

type commandApplyGroup struct {
	wg   sync.WaitGroup
	mu   sync.Mutex
	err  error
	done func(error)
}

func newCommandApplyWindow(cp *commandPipeline, parallel int) *commandApplyWindow {
	if parallel < 1 {
		parallel = 1
	}
	return &commandApplyWindow{
		cp:           cp,
		parallel:     parallel,
		activeDeps:   make(map[commandApplyDependencyKey]commandApplyActiveDependency),
		completed:    make(map[uint64]*commandApplyTask),
		nextComplete: 1,
	}
}

func (w *commandApplyWindow) submit(plans []commandApplyPlan, emit applyEventEmitter, done func(error)) error {
	if len(plans) == 0 {
		if done != nil {
			done(nil)
		}
		return nil
	}
	if done == nil {
		done = func(error) {}
	}
	group := newCommandApplyGroup(len(plans), done)
	tasks := make([]*commandApplyTask, 0, len(plans))
	for _, plan := range plans {
		tasks = append(tasks, &commandApplyTask{plan: plan, emit: emit, group: group})
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fatalErr != nil {
		for _, task := range tasks {
			w.finishTaskExternalLocked(task, w.fatalErr)
		}
		return nil
	}
	w.queue = append(w.queue, tasks...)
	w.scheduleLocked()
	return nil
}

func (w *commandApplyWindow) scheduleLocked() {
	for w.fatalErr == nil && !w.activeBarrier && w.active < w.parallel {
		idx := w.nextSchedulableLocked()
		if idx < 0 {
			return
		}
		task := w.queue[idx]
		copy(w.queue[idx:], w.queue[idx+1:])
		w.queue[len(w.queue)-1] = nil
		w.queue = w.queue[:len(w.queue)-1]
		w.active++
		if task.plan.barrier {
			w.activeBarrier = true
		} else {
			commandApplyWindowAddDependencies(w.activeDeps, task.plan)
		}
		go w.runTask(task)
	}
}

func (w *commandApplyWindow) nextSchedulableLocked() int {
	if len(w.queue) == 0 {
		return -1
	}
	if w.queue[0].plan.barrier {
		if w.active == 0 {
			return 0
		}
		return -1
	}
	for i, task := range w.queue {
		if task.plan.barrier {
			return -1
		}
		if !commandApplyWindowConflicts(w.activeDeps, task.plan) {
			return i
		}
	}
	return -1
}

func (w *commandApplyWindow) runTask(task *commandApplyTask) {
	resp, err := w.cp.applyPlan(task.plan)
	w.finishTask(task, resp, err)
}

func (w *commandApplyWindow) finishTask(task *commandApplyTask, resp *raftcmdpb.RaftCmdResponse, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.active--
	if task.plan.barrier {
		w.activeBarrier = false
	} else {
		commandApplyWindowRemoveDependencies(w.activeDeps, task.plan)
	}
	task.resp = resp
	task.err = err
	if w.fatalErr != nil {
		w.finishTaskExternalLocked(task, w.fatalErr)
		return
	}
	if err != nil {
		fatalErr := fmt.Errorf("commandPipeline: fatal apply window failed at request %d: %w", task.plan.proposalKey.requestID, err)
		w.finishTaskExternalLocked(task, fatalErr)
		w.failAllLocked(fatalErr)
		return
	}
	w.completed[task.plan.order] = task
	w.flushCompletedLocked()
	w.scheduleLocked()
}

func (w *commandApplyWindow) failAllLocked(err error) {
	if w.fatalErr == nil {
		w.fatalErr = err
	}
	for _, task := range w.completed {
		w.finishTaskExternalLocked(task, err)
	}
	clear(w.completed)
	for _, task := range w.queue {
		w.finishTaskExternalLocked(task, err)
	}
	clear(w.queue)
	w.queue = w.queue[:0]
}

func (w *commandApplyWindow) flushCompletedLocked() {
	for {
		task := w.completed[w.nextComplete]
		if task == nil {
			return
		}
		delete(w.completed, w.nextComplete)
		w.nextComplete++
		w.finishTaskExternalLocked(task, nil)
	}
}

func (w *commandApplyWindow) finishTaskExternalLocked(task *commandApplyTask, err error) {
	if task == nil || task.externalDone {
		return
	}
	task.externalDone = true
	if err == nil {
		if task.emit != nil {
			task.emit(task.plan.entry, task.plan.req, task.resp)
		}
		w.cp.completeProposal(task.plan.proposalKey, task.resp, nil)
	} else {
		w.cp.completeProposal(task.plan.proposalKey, nil, err)
	}
	task.group.complete(err)
}

func (g *commandApplyGroup) complete(err error) {
	if g == nil {
		return
	}
	g.mu.Lock()
	if err != nil && g.err == nil {
		g.err = err
	}
	g.mu.Unlock()
	g.wg.Done()
}

func newCommandApplyGroup(count int, done func(error)) *commandApplyGroup {
	g := &commandApplyGroup{done: done}
	g.wg.Add(count)
	go func() {
		g.wg.Wait()
		g.mu.Lock()
		err := g.err
		done := g.done
		g.done = nil
		g.mu.Unlock()
		if done != nil {
			done(err)
		}
	}()
	return g
}

func commandApplyWindowConflicts(active map[commandApplyDependencyKey]commandApplyActiveDependency, plan commandApplyPlan) bool {
	for _, dep := range plan.deps {
		state, ok := active[dep.key]
		if !ok {
			continue
		}
		if dep.mode == commandApplyDependencyWrite {
			if state.readers > 0 || state.writers > 0 {
				return true
			}
			continue
		}
		if state.writers > 0 {
			return true
		}
	}
	return false
}

func commandApplyWindowAddDependencies(active map[commandApplyDependencyKey]commandApplyActiveDependency, plan commandApplyPlan) {
	for _, dep := range plan.deps {
		state := active[dep.key]
		if dep.mode == commandApplyDependencyWrite {
			state.writers++
		} else {
			state.readers++
		}
		active[dep.key] = state
	}
}

func commandApplyWindowRemoveDependencies(active map[commandApplyDependencyKey]commandApplyActiveDependency, plan commandApplyPlan) {
	for _, dep := range plan.deps {
		state, ok := active[dep.key]
		if !ok {
			continue
		}
		if dep.mode == commandApplyDependencyWrite {
			if state.writers > 0 {
				state.writers--
			}
		} else if state.readers > 0 {
			state.readers--
		}
		if state.readers == 0 && state.writers == 0 {
			delete(active, dep.key)
			continue
		}
		active[dep.key] = state
	}
}

func (cp *commandPipeline) applyOne(plan commandApplyPlan, emit applyEventEmitter) error {
	resp, applyErr := cp.applyPlan(plan)
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

func (cp *commandPipeline) applyPlan(plan commandApplyPlan) (*raftcmdpb.RaftCmdResponse, error) {
	if cp.applier == nil {
		return nil, fmt.Errorf("commandPipeline: apply without handler")
	}
	return cp.applier(plan.req)
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

func (s *Store) applyEntriesAsync(entries []myraft.Entry, done func(error)) error {
	if s == nil {
		return errNilStore
	}
	if s.cmds == nil || s.cmds.pipe == nil {
		return errCommandApplyWithoutHandler
	}
	return s.cmds.pipe.applyEntriesAsync(entries, s.emitApplyEvents, done)
}
