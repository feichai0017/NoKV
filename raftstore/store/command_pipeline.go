package store

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"time"

	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type commandRuntime struct {
	pipe    *commandPipeline
	timeout time.Duration
}

const commandApplyMaxBatchTasks = 64

type commandProposal struct {
	ch chan proposalResult
}

type proposalResult struct {
	resp *raftcmdpb.RaftCmdResponse
	err  error
}

type commandProposalCompletion struct {
	key  commandProposalKey
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
	mu           sync.Mutex
	seq          uint64
	orderSeq     uint64
	proposals    map[commandProposalKey]*commandProposal
	applier      func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)
	batchApplier func([]*raftcmdpb.RaftCmdRequest) ([]*raftcmdpb.RaftCmdResponse, error)
	parallel     int
	window       *commandApplyWindow
}

type applyEventEmitter func(myraft.Entry, *raftcmdpb.RaftCmdRequest, *raftcmdpb.RaftCmdResponse)

func newCommandPipeline(applier func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error), parallelism ...int) *commandPipeline {
	return newCommandPipelineWithBatch(applier, nil, parallelism...)
}

func newCommandPipelineWithBatch(
	applier func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error),
	batchApplier func([]*raftcmdpb.RaftCmdRequest) ([]*raftcmdpb.RaftCmdResponse, error),
	parallelism ...int,
) *commandPipeline {
	parallel := 0
	if len(parallelism) > 0 {
		parallel = parallelism[0]
	}
	cp := &commandPipeline{
		proposals:    make(map[commandProposalKey]*commandProposal),
		applier:      applier,
		batchApplier: batchApplier,
		parallel:     normalizeCommandApplyParallelism(parallel),
	}
	cp.window = newCommandApplyWindow(cp, cp.parallel)
	return cp
}

func (cp *commandPipeline) close() {
	if cp == nil || cp.window == nil {
		return
	}
	cp.window.close()
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

func (cp *commandPipeline) completeProposalBatch(completions []commandProposalCompletion) {
	if cp == nil || len(completions) == 0 {
		return
	}
	type completedProposal struct {
		prop *commandProposal
		resp *raftcmdpb.RaftCmdResponse
		err  error
	}
	var propBuf [commandApplyMaxBatchTasks]completedProposal
	props := propBuf[:0]
	cp.mu.Lock()
	for _, completion := range completions {
		if !completion.key.valid() {
			continue
		}
		prop := cp.proposals[completion.key]
		delete(cp.proposals, completion.key)
		if prop == nil {
			continue
		}
		props = append(props, completedProposal{
			prop: prop,
			resp: completion.resp,
			err:  completion.err,
		})
	}
	cp.mu.Unlock()
	for _, item := range props {
		item.prop.ch <- proposalResult{resp: item.resp, err: item.err}
		close(item.prop.ch)
	}
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
	if cp.parallel <= 1 {
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
	mu           sync.Mutex
	cp           *commandPipeline
	parallel     int
	ready        chan commandApplyTaskBatch
	stop         chan struct{}
	workerWG     sync.WaitGroup
	closed       bool
	drainCh      chan struct{}
	readyQueue   []*commandApplyTask
	readyHead    int
	active       int
	pending      map[*commandApplyTask]struct{}
	lastWriter   map[commandApplyDependencyKey]*commandApplyTask
	readers      map[commandApplyDependencyKey][]*commandApplyTask
	tails        map[*commandApplyTask]struct{}
	barrierTail  *commandApplyTask
	completed    map[uint64]*commandApplyTask
	nextComplete uint64
	fatalErr     error
}

type commandApplyTask struct {
	plan         commandApplyPlan
	emit         applyEventEmitter
	group        *commandApplyGroup
	resp         *raftcmdpb.RaftCmdResponse
	err          error
	pendingDeps  int
	successors   []*commandApplyTask
	running      bool
	done         bool
	externalDone bool
}

type commandApplyTaskBatch struct {
	first *commandApplyTask
	rest  []*commandApplyTask
}

type commandApplyGroupCompletion struct {
	group *commandApplyGroup
	err   error
}

func (b commandApplyTaskBatch) len() int {
	if b.first == nil {
		return 0
	}
	return 1 + len(b.rest)
}

func (b commandApplyTaskBatch) forEach(fn func(*commandApplyTask)) {
	if b.first == nil || fn == nil {
		return
	}
	fn(b.first)
	for _, task := range b.rest {
		fn(task)
	}
}

type commandApplyGroup struct {
	mu        sync.Mutex
	remaining int
	err       error
	done      func(error)
}

func newCommandApplyWindow(cp *commandPipeline, parallel int) *commandApplyWindow {
	if parallel < 1 {
		parallel = 1
	}
	w := &commandApplyWindow{
		cp:           cp,
		parallel:     parallel,
		ready:        make(chan commandApplyTaskBatch, parallel),
		stop:         make(chan struct{}),
		pending:      make(map[*commandApplyTask]struct{}),
		lastWriter:   make(map[commandApplyDependencyKey]*commandApplyTask),
		readers:      make(map[commandApplyDependencyKey][]*commandApplyTask),
		tails:        make(map[*commandApplyTask]struct{}),
		completed:    make(map[uint64]*commandApplyTask),
		nextComplete: 1,
	}
	w.workerWG.Add(parallel)
	for range parallel {
		go w.runWorker()
	}
	return w
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
	var taskBuf [commandApplyMaxBatchTasks]*commandApplyTask
	tasks := taskBuf[:0]
	if len(plans) > cap(taskBuf) {
		tasks = make([]*commandApplyTask, 0, len(plans))
	}
	for _, plan := range plans {
		tasks = append(tasks, &commandApplyTask{plan: plan, emit: emit, group: group})
	}
	var groupCompletions []commandApplyGroupCompletion
	w.mu.Lock()
	if w.closed || w.fatalErr != nil {
		err := w.fatalErr
		if err == nil {
			err = errCommandPipelineUnavailable
		}
		for _, task := range tasks {
			w.finishTaskExternalLocked(task, err, &groupCompletions)
		}
		w.mu.Unlock()
		completeCommandApplyGroups(groupCompletions)
		return nil
	}
	for _, task := range tasks {
		w.addTaskLocked(task)
	}
	w.scheduleLocked()
	w.mu.Unlock()
	return nil
}

func (w *commandApplyWindow) scheduleLocked() {
	for !w.closed && w.fatalErr == nil && w.active < w.parallel {
		task := w.popReadyLocked()
		if task == nil {
			return
		}
		if task.plan.barrier && w.active > 0 {
			w.pushReadyFrontLocked(task)
			return
		}
		batch := w.buildReadyBatchLocked(task)
		batch.forEach(func(task *commandApplyTask) {
			task.running = true
		})
		w.active++
		w.ready <- batch
	}
}

func (w *commandApplyWindow) buildReadyBatchLocked(first *commandApplyTask) commandApplyTaskBatch {
	if first == nil {
		return commandApplyTaskBatch{}
	}
	if first.plan.barrier || w.cp == nil || w.cp.batchApplier == nil {
		return commandApplyTaskBatch{first: first}
	}
	class, ok := commandApplyBatchClass(first.plan)
	if !ok {
		return commandApplyTaskBatch{first: first}
	}
	batch := commandApplyTaskBatch{first: first}
	for batch.len() < commandApplyMaxBatchTasks {
		next := w.popReadyLocked()
		if next == nil {
			return batch
		}
		nextClass, nextOK := commandApplyBatchClass(next.plan)
		if next.plan.barrier || !nextOK || nextClass != class {
			w.pushReadyFrontLocked(next)
			return batch
		}
		batch.rest = append(batch.rest, next)
	}
	return batch
}

func (w *commandApplyWindow) addTaskLocked(task *commandApplyTask) {
	if task == nil {
		return
	}
	w.pending[task] = struct{}{}
	var predBuf [8]*commandApplyTask
	preds := predBuf[:0]
	addPred := func(pred *commandApplyTask) {
		if pred == nil || pred == task || pred.done || pred.externalDone {
			return
		}
		if slices.Contains(preds, pred) {
			return
		}
		preds = append(preds, pred)
	}

	if task.plan.barrier {
		for pred := range w.tails {
			addPred(pred)
		}
		clear(w.lastWriter)
		clear(w.readers)
		clear(w.tails)
		w.barrierTail = task
	} else {
		addPred(w.barrierTail)
		for _, dep := range task.plan.deps {
			if dep.mode == commandApplyDependencyWrite {
				addPred(w.lastWriter[dep.key])
				for _, reader := range w.readers[dep.key] {
					addPred(reader)
				}
				w.lastWriter[dep.key] = task
				delete(w.readers, dep.key)
				continue
			}
			addPred(w.lastWriter[dep.key])
			w.readers[dep.key] = append(w.readers[dep.key], task)
		}
	}

	for _, pred := range preds {
		task.pendingDeps++
		pred.successors = append(pred.successors, task)
		delete(w.tails, pred)
	}
	w.tails[task] = struct{}{}
	if task.pendingDeps == 0 {
		w.pushReadyLocked(task)
	}
}

func (w *commandApplyWindow) pushReadyLocked(task *commandApplyTask) {
	if task == nil {
		return
	}
	w.readyQueue = append(w.readyQueue, task)
}

func (w *commandApplyWindow) pushReadyFrontLocked(task *commandApplyTask) {
	if task == nil {
		return
	}
	if w.readyHead > 0 {
		w.readyHead--
		w.readyQueue[w.readyHead] = task
		return
	}
	w.readyQueue = append([]*commandApplyTask{task}, w.readyQueue...)
}

func (w *commandApplyWindow) popReadyLocked() *commandApplyTask {
	for w.readyHead < len(w.readyQueue) {
		task := w.readyQueue[w.readyHead]
		w.readyQueue[w.readyHead] = nil
		w.readyHead++
		if w.readyHead > 64 && w.readyHead*2 >= len(w.readyQueue) {
			copy(w.readyQueue, w.readyQueue[w.readyHead:])
			n := len(w.readyQueue) - w.readyHead
			clear(w.readyQueue[n:])
			w.readyQueue = w.readyQueue[:n]
			w.readyHead = 0
		}
		if task != nil && !task.done && !task.externalDone {
			return task
		}
	}
	clear(w.readyQueue)
	w.readyQueue = w.readyQueue[:0]
	w.readyHead = 0
	return nil
}

func (w *commandApplyWindow) runWorker() {
	defer w.workerWG.Done()
	for {
		select {
		case batch := <-w.ready:
			if batch.len() == 0 {
				continue
			}
			if batch.len() == 1 {
				resp, err := w.cp.applyPlan(batch.first.plan)
				w.finishTask(batch.first, resp, err)
				continue
			}
			resps, err := w.cp.applyPlanBatch(batch)
			w.finishTasks(batch, resps, err)
		case <-w.stop:
			return
		}
	}
}

func (w *commandApplyWindow) close() {
	if w == nil {
		return
	}
	var groupCompletions []commandApplyGroupCompletion
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		w.workerWG.Wait()
		return
	}
	w.closed = true
	if w.fatalErr == nil {
		w.fatalErr = errCommandPipelineUnavailable
	}
	for task := range w.pending {
		if task.running {
			continue
		}
		w.finishTaskExternalLocked(task, w.fatalErr, &groupCompletions)
	}
	clear(w.completed)
	clear(w.readyQueue)
	w.readyQueue = w.readyQueue[:0]
	w.readyHead = 0
	if w.active == 0 {
		close(w.stop)
		w.mu.Unlock()
		completeCommandApplyGroups(groupCompletions)
		w.workerWG.Wait()
		return
	}
	drainCh := make(chan struct{})
	w.drainCh = drainCh
	w.mu.Unlock()
	completeCommandApplyGroups(groupCompletions)
	<-drainCh
	close(w.stop)
	w.workerWG.Wait()
}

func (w *commandApplyWindow) finishTask(task *commandApplyTask, resp *raftcmdpb.RaftCmdResponse, err error) {
	var groupCompletions []commandApplyGroupCompletion
	w.mu.Lock()
	w.active--
	task.running = false
	task.resp = resp
	task.err = err
	if w.fatalErr != nil {
		task.done = true
		w.cleanupFinishedTaskLocked(task)
		w.finishTaskExternalLocked(task, w.fatalErr, &groupCompletions)
		w.notifyDrainedLocked()
		w.mu.Unlock()
		completeCommandApplyGroups(groupCompletions)
		return
	}
	if err != nil {
		fatalErr := fmt.Errorf("commandPipeline: fatal apply window failed at request %d: %w", task.plan.proposalKey.requestID, err)
		w.finishTaskExternalLocked(task, fatalErr, &groupCompletions)
		w.failAllLocked(fatalErr, &groupCompletions)
		w.notifyDrainedLocked()
		w.mu.Unlock()
		completeCommandApplyGroups(groupCompletions)
		return
	}
	w.finishSuccessfulTaskLocked(task)
	w.flushCompletedLocked(&groupCompletions)
	w.scheduleLocked()
	w.notifyDrainedLocked()
	w.mu.Unlock()
	completeCommandApplyGroups(groupCompletions)
}

func (w *commandApplyWindow) finishTasks(batch commandApplyTaskBatch, resps []*raftcmdpb.RaftCmdResponse, err error) {
	var groupCompletions []commandApplyGroupCompletion
	w.mu.Lock()
	w.active--
	batchLen := batch.len()
	if len(resps) != batchLen && err == nil {
		err = fmt.Errorf("commandPipeline: batch apply returned %d responses for %d tasks", len(resps), batchLen)
	}
	i := 0
	batch.forEach(func(task *commandApplyTask) {
		task.running = false
		if i < len(resps) {
			task.resp = resps[i]
		}
		task.err = err
		i++
	})
	if w.fatalErr != nil {
		batch.forEach(func(task *commandApplyTask) {
			task.done = true
			w.cleanupFinishedTaskLocked(task)
			w.finishTaskExternalLocked(task, w.fatalErr, &groupCompletions)
		})
		w.notifyDrainedLocked()
		w.mu.Unlock()
		completeCommandApplyGroups(groupCompletions)
		return
	}
	if err != nil {
		fatalErr := fmt.Errorf("commandPipeline: fatal apply window failed: %w", err)
		batch.forEach(func(task *commandApplyTask) {
			w.finishTaskExternalLocked(task, fatalErr, &groupCompletions)
		})
		w.failAllLocked(fatalErr, &groupCompletions)
		w.notifyDrainedLocked()
		w.mu.Unlock()
		completeCommandApplyGroups(groupCompletions)
		return
	}
	batch.forEach(func(task *commandApplyTask) {
		w.finishSuccessfulTaskLocked(task)
	})
	w.flushCompletedLocked(&groupCompletions)
	w.scheduleLocked()
	w.notifyDrainedLocked()
	w.mu.Unlock()
	completeCommandApplyGroups(groupCompletions)
}

func (w *commandApplyWindow) finishSuccessfulTaskLocked(task *commandApplyTask) {
	if task == nil {
		return
	}
	task.done = true
	w.cleanupFinishedTaskLocked(task)
	for _, successor := range task.successors {
		if successor == nil || successor.externalDone || successor.done {
			continue
		}
		if successor.pendingDeps > 0 {
			successor.pendingDeps--
		}
		if successor.pendingDeps == 0 {
			w.pushReadyLocked(successor)
		}
	}
	task.successors = nil
	w.completed[task.plan.order] = task
}

func (w *commandApplyWindow) cleanupFinishedTaskLocked(task *commandApplyTask) {
	if task == nil {
		return
	}
	delete(w.tails, task)
	if w.barrierTail == task {
		w.barrierTail = nil
	}
	for _, dep := range task.plan.deps {
		if dep.mode == commandApplyDependencyWrite {
			if w.lastWriter[dep.key] == task {
				delete(w.lastWriter, dep.key)
			}
			continue
		}
		readers := w.readers[dep.key]
		for i, reader := range readers {
			if reader != task {
				continue
			}
			copy(readers[i:], readers[i+1:])
			readers[len(readers)-1] = nil
			readers = readers[:len(readers)-1]
			break
		}
		if len(readers) == 0 {
			delete(w.readers, dep.key)
		} else {
			w.readers[dep.key] = readers
		}
	}
}

func (w *commandApplyWindow) notifyDrainedLocked() {
	if w == nil || !w.closed || w.active != 0 || w.drainCh == nil {
		return
	}
	close(w.drainCh)
	w.drainCh = nil
}

func (w *commandApplyWindow) failAllLocked(err error, groupCompletions *[]commandApplyGroupCompletion) {
	if w.fatalErr == nil {
		w.fatalErr = err
	}
	for task := range w.pending {
		if task.running {
			continue
		}
		w.finishTaskExternalLocked(task, err, groupCompletions)
	}
	clear(w.completed)
	clear(w.readyQueue)
	w.readyQueue = w.readyQueue[:0]
	w.readyHead = 0
}

func (w *commandApplyWindow) flushCompletedLocked(groupCompletions *[]commandApplyGroupCompletion) {
	task := w.completed[w.nextComplete]
	if task == nil {
		return
	}
	delete(w.completed, w.nextComplete)
	w.nextComplete++
	next := w.completed[w.nextComplete]
	if next == nil {
		w.finishTaskExternalSingleLocked(task, nil, groupCompletions)
		return
	}
	var buf [commandApplyMaxBatchTasks]*commandApplyTask
	tasks := buf[:0]
	tasks = append(tasks, task)
	for {
		task = w.completed[w.nextComplete]
		if task == nil {
			w.finishTaskExternalBatchLocked(tasks, nil, groupCompletions)
			return
		}
		delete(w.completed, w.nextComplete)
		w.nextComplete++
		tasks = append(tasks, task)
		if len(tasks) == cap(buf) {
			w.finishTaskExternalBatchLocked(tasks, nil, groupCompletions)
			tasks = buf[:0]
		}
	}
}

func (w *commandApplyWindow) finishTaskExternalLocked(
	task *commandApplyTask,
	err error,
	groupCompletions *[]commandApplyGroupCompletion,
) {
	w.finishTaskExternalSingleLocked(task, err, groupCompletions)
}

func (w *commandApplyWindow) finishTaskExternalBatchLocked(
	tasks []*commandApplyTask,
	err error,
	groupCompletions *[]commandApplyGroupCompletion,
) {
	if len(tasks) == 0 {
		return
	}
	if len(tasks) == 1 {
		w.finishTaskExternalSingleLocked(tasks[0], err, groupCompletions)
		return
	}
	var completionBuf [commandApplyMaxBatchTasks]commandProposalCompletion
	var completedBuf [commandApplyMaxBatchTasks]*commandApplyTask
	completions := completionBuf[:0]
	completedTasks := completedBuf[:0]
	for _, task := range tasks {
		w.collectTaskExternalCompletionLocked(task, err, &completions, &completedTasks)
	}
	w.cp.completeProposalBatch(completions)
	for _, task := range completedTasks {
		appendCommandApplyGroupCompletion(groupCompletions, task.group, err)
	}
}

func (w *commandApplyWindow) finishTaskExternalSingleLocked(
	task *commandApplyTask,
	err error,
	groupCompletions *[]commandApplyGroupCompletion,
) {
	if task == nil || task.externalDone {
		return
	}
	task.externalDone = true
	delete(w.pending, task)
	if err == nil {
		if task.emit != nil {
			task.emit(task.plan.entry, task.plan.req, task.resp)
		}
		w.cp.completeProposal(task.plan.proposalKey, task.resp, nil)
	} else {
		w.cp.completeProposal(task.plan.proposalKey, nil, err)
	}
	appendCommandApplyGroupCompletion(groupCompletions, task.group, err)
}

func (w *commandApplyWindow) collectTaskExternalCompletionLocked(
	task *commandApplyTask,
	err error,
	completions *[]commandProposalCompletion,
	completedTasks *[]*commandApplyTask,
) {
	if task == nil || task.externalDone {
		return
	}
	task.externalDone = true
	delete(w.pending, task)
	if err == nil {
		if task.emit != nil {
			task.emit(task.plan.entry, task.plan.req, task.resp)
		}
		*completions = append(*completions, commandProposalCompletion{
			key:  task.plan.proposalKey,
			resp: task.resp,
		})
	} else {
		*completions = append(*completions, commandProposalCompletion{
			key: task.plan.proposalKey,
			err: err,
		})
	}
	*completedTasks = append(*completedTasks, task)
}

func (g *commandApplyGroup) complete(err error) {
	if g == nil {
		return
	}
	var done func(error)
	var doneErr error
	g.mu.Lock()
	if err != nil && g.err == nil {
		g.err = err
	}
	if g.remaining > 0 {
		g.remaining--
	}
	if g.remaining == 0 && g.done != nil {
		done = g.done
		doneErr = g.err
		g.done = nil
	}
	g.mu.Unlock()
	if done != nil {
		done(doneErr)
	}
}

func appendCommandApplyGroupCompletion(
	completions *[]commandApplyGroupCompletion,
	group *commandApplyGroup,
	err error,
) {
	if completions == nil || group == nil {
		return
	}
	*completions = append(*completions, commandApplyGroupCompletion{group: group, err: err})
}

func completeCommandApplyGroups(completions []commandApplyGroupCompletion) {
	for _, completion := range completions {
		completion.group.complete(completion.err)
	}
}

func newCommandApplyGroup(count int, done func(error)) *commandApplyGroup {
	return &commandApplyGroup{remaining: count, done: done}
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

func (cp *commandPipeline) applyPlanBatch(batch commandApplyTaskBatch) ([]*raftcmdpb.RaftCmdResponse, error) {
	batchLen := batch.len()
	if batchLen == 0 {
		return nil, nil
	}
	if batchLen == 1 || cp.batchApplier == nil {
		return nil, fmt.Errorf("commandPipeline: batch apply called without a batch")
	}
	var reqBuf [commandApplyMaxBatchTasks]*raftcmdpb.RaftCmdRequest
	reqs := reqBuf[:0]
	batch.forEach(func(task *commandApplyTask) {
		reqs = append(reqs, task.plan.req)
	})
	return cp.batchApplier(reqs)
}

func commandApplyBatchClass(plan commandApplyPlan) (raftcmdpb.CmdType, bool) {
	if plan.barrier || plan.req == nil || len(plan.req.GetRequests()) != 1 {
		return 0, false
	}
	req := plan.req.GetRequests()[0]
	if req == nil {
		return 0, false
	}
	switch req.GetCmdType() {
	case raftcmdpb.CmdType_CMD_PREWRITE,
		raftcmdpb.CmdType_CMD_COMMIT,
		raftcmdpb.CmdType_CMD_BATCH_ROLLBACK,
		raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
		raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
		raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT:
		return req.GetCmdType(), true
	default:
		return 0, false
	}
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

func (s *Store) SubmitApply(task peer.ApplyTask, done func(peer.ApplyResult)) error {
	if s == nil {
		return errNilStore
	}
	if s.cmds == nil || s.cmds.pipe == nil {
		return errCommandApplyWithoutHandler
	}
	return s.cmds.pipe.applyEntriesAsync(task.Entries, s.emitApplyEvents, func(err error) {
		if done != nil {
			done(peer.ApplyResult{Entries: task.Entries, Err: err})
		}
	})
}
