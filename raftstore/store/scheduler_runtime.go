package store

import (
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"syscall"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultPublishRetryBackoff = 200 * time.Millisecond
const maxPublishRetryBackoff = 5 * time.Second
const defaultSchedulerOperationRetryBackoff = 200 * time.Millisecond
const maxSchedulerOperationRetryBackoff = 5 * time.Second
const maxSchedulerOperationAttempts = 8

type schedulerRuntime struct {
	client SchedulerClient

	operation operationRuntime
	publish   publishRuntime
	health    schedulerHealth
}

type operationRuntime struct {
	input    chan scheduledOp
	stop     chan struct{}
	wg       sync.WaitGroup
	cooldown time.Duration
	interval time.Duration
	burst    int

	mu        sync.Mutex
	pending   map[operationKey]bool
	lastApply map[operationKey]time.Time
	dropped   uint64
}

type publishRuntime struct {
	regionSignal chan struct{}

	mu            sync.Mutex
	descriptors   map[uint64]descriptor.Descriptor
	regionUpdates map[uint64]regionEvent
	nextRegionSeq uint64

	heartbeat        time.Duration
	heartbeatTimeout time.Duration
	publishTimeout   time.Duration
	heartbeatStop    chan struct{}
	heartbeatWG      sync.WaitGroup
	retryBackoff     time.Duration
	retryScheduled   bool
}

type schedulerHealth struct {
	mu          sync.Mutex
	degraded    bool
	lastError   string
	lastErrorAt time.Time
}

type operationKey struct {
	region uint64
	typeID OperationType
}

type regionEvent struct {
	root         rootevent.Event
	transitionID string
	seq          uint64
}

type scheduledOp struct {
	op       Operation
	ready    time.Time
	attempts uint32
}

func (s *Store) schedulerClient() SchedulerClient {
	if s == nil || s.sched == nil {
		return nil
	}
	return s.sched.client
}

// SchedulerStatus returns the current scheduler health view by combining the
// local queue state with the control-plane client status.
func (s *Store) SchedulerStatus() SchedulerStatus {
	if s == nil {
		return SchedulerStatus{}
	}
	status := SchedulerStatus{Mode: SchedulerModeHealthy}
	if s.schedulerClient() != nil {
		status = s.schedulerClient().Status()
		if status.Mode == "" {
			if status.Degraded {
				status.Mode = SchedulerModeUnavailable
			} else {
				status.Mode = SchedulerModeHealthy
			}
		}
	}
	if s.sched == nil {
		return status
	}
	s.sched.operation.mu.Lock()
	status.DroppedOperations += s.sched.operation.dropped
	s.sched.operation.mu.Unlock()
	s.sched.health.mu.Lock()
	defer s.sched.health.mu.Unlock()
	if s.sched.health.degraded {
		status.Degraded = true
		if status.Mode != SchedulerModeUnavailable {
			status.Mode = SchedulerModeDegraded
		}
		if status.LastErrorAt.Before(s.sched.health.lastErrorAt) || status.LastError == "" {
			status.LastError = s.sched.health.lastError
			status.LastErrorAt = s.sched.health.lastErrorAt
		}
	}
	return status
}

func (s *Store) applyOperation(op Operation) bool {
	if s == nil {
		return false
	}
	switch op.Type {
	case OperationLeaderTransfer:
		if op.Source == 0 || op.Target == 0 {
			return false
		}
		s.VisitPeers(func(p *peer.Peer) {
			if p.ID() == op.Source {
				_ = p.TransferLeader(op.Target)
			}
		})
		return true
	}
	return false
}

func (s *Store) enqueueOperation(op Operation) {
	if s == nil {
		return
	}
	if op.Type == OperationNone || op.Region == 0 {
		return
	}
	if s.sched == nil || s.sched.operation.input == nil {
		s.clearLocalSchedulerDegraded()
		s.applyOperation(op)
		return
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.sched.operation.mu.Lock()
	if _, exists := s.sched.operation.pending[key]; exists {
		s.sched.operation.mu.Unlock()
		return
	}
	s.sched.operation.pending[key] = false
	s.sched.operation.mu.Unlock()
	item := scheduledOp{op: op}
	if !s.hasDurableSchedulerQueue() {
		if s.dispatchPendingSchedulerOperation(item) {
			s.clearLocalSchedulerDegraded()
			return
		}
		s.recordLocalSchedulerDrop(op)
		return
	}
	if err := s.persistPendingSchedulerOperation(item); err != nil {
		s.dropQueuedOperations([]Operation{op}, fmt.Sprintf("persist failed: %v", err))
		return
	}
	if s.dispatchPendingSchedulerOperation(item) {
		s.clearLocalSchedulerDegraded()
		return
	}
	s.recordLocalSchedulerBacklog(op, "queue full")
}

func (s *Store) hasDurableSchedulerQueue() bool {
	if s == nil {
		return false
	}
	rm := s.regionMgr()
	return rm != nil && rm.localMeta != nil
}

func (s *Store) startHeartbeatLoop() {
	if s == nil || s.schedulerClient() == nil || s.sched == nil || s.sched.publish.heartbeat <= 0 || s.sched.publish.heartbeatStop != nil {
		return
	}
	s.sched.publish.heartbeatStop = make(chan struct{})
	s.sched.publish.regionSignal = make(chan struct{}, 1)
	s.sendHeartbeats()
	s.sched.publish.heartbeatWG.Add(1)
	go s.runHeartbeatLoop()
}

func (s *Store) stopHeartbeatLoop() {
	if s == nil || s.sched == nil || s.sched.publish.heartbeatStop == nil {
		return
	}
	close(s.sched.publish.heartbeatStop)
	s.sched.publish.heartbeatWG.Wait()
	s.sched.publish.heartbeatStop = nil
	s.sched.publish.regionSignal = nil
}

func (s *Store) runHeartbeatLoop() {
	defer s.sched.publish.heartbeatWG.Done()
	ticker := time.NewTicker(s.sched.publish.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.retryPendingSchedulerOperations()
			s.sendHeartbeats()
		case <-s.sched.publish.regionSignal:
			s.flushRegionUpdates()
		case <-s.sched.publish.heartbeatStop:
			return
		}
	}
}

func (s *Store) sendHeartbeats() {
	if s == nil || s.schedulerClient() == nil {
		return
	}
	for _, regionID := range s.schedulerRegionIDs() {
		ctx, cancel := s.schedulerHeartbeatContext()
		s.schedulerClient().ReportRegionHeartbeat(ctx, regionID)
		cancel()
	}
	if s.storeID == 0 {
		return
	}
	ops := s.reportStoreHeartbeat()
	for _, op := range ops {
		s.enqueueOperation(op)
	}
}

func (s *Store) reportStoreHeartbeat() []Operation {
	if s == nil || s.schedulerClient() == nil || s.storeID == 0 {
		return nil
	}
	ctx, cancel := s.schedulerHeartbeatContext()
	ops := s.schedulerClient().StoreHeartbeat(ctx, s.storeStatsSnapshot())
	cancel()
	return ops
}

func (s *Store) enqueueRegionEvent(ev regionEvent) {
	if s == nil || s.schedulerClient() == nil || s.sched == nil {
		return
	}
	if ev.root.Kind == rootevent.KindUnknown {
		return
	}
	regionID, ok := schedulerRegionEventKey(ev.root)
	if !ok || regionID == 0 {
		return
	}
	ev.root = rootevent.CloneEvent(ev.root)
	if err := s.persistPendingRegionEvent(&ev); err != nil {
		s.recordLocalSchedulerPublishFailure(ev, err)
		return
	}
	s.sched.publish.mu.Lock()
	if s.sched.publish.descriptors == nil {
		s.sched.publish.descriptors = make(map[uint64]descriptor.Descriptor)
	}
	if s.sched.publish.regionUpdates == nil {
		s.sched.publish.regionUpdates = make(map[uint64]regionEvent)
	}
	rootmaterialize.ApplyEventToDescriptors(s.sched.publish.descriptors, ev.root)
	s.sched.publish.regionUpdates[ev.seq] = ev
	s.sched.publish.mu.Unlock()
	s.signalRegionFlush()
}

func (s *Store) hasPendingRegionUpdate(regionID uint64) bool {
	if s == nil || s.sched == nil || regionID == 0 {
		return false
	}
	s.sched.publish.mu.Lock()
	defer s.sched.publish.mu.Unlock()
	for _, ev := range s.sched.publish.regionUpdates {
		if currentRegionID, ok := schedulerRegionEventKey(ev.root); ok && currentRegionID == regionID {
			return true
		}
	}
	return false
}

func (s *Store) schedulerRegionIDs() []uint64 {
	if s == nil {
		return nil
	}
	metas := s.RegionMetas()
	out := make([]uint64, 0, len(metas))
	for _, meta := range metas {
		if meta.ID == 0 {
			continue
		}
		out = append(out, meta.ID)
	}
	return out
}

func (s *Store) flushRegionUpdates() {
	if s == nil || s.schedulerClient() == nil || s.sched == nil {
		return
	}
	s.sched.publish.mu.Lock()
	if len(s.sched.publish.regionUpdates) == 0 {
		s.sched.publish.mu.Unlock()
		return
	}
	pending := make([]regionEvent, 0, len(s.sched.publish.regionUpdates))
	for seq, ev := range s.sched.publish.regionUpdates {
		if ev.seq == 0 {
			ev.seq = seq
		}
		pending = append(pending, ev)
	}
	clear(s.sched.publish.regionUpdates)
	s.sched.publish.mu.Unlock()
	sort.Slice(pending, func(i, j int) bool { return pending[i].seq < pending[j].seq })

	failed := make([]regionEvent, 0)
	for _, ev := range pending {
		ctx, cancel := s.schedulerPublishContext()
		if err := s.schedulerClient().PublishRootEvent(ctx, ev.root); err != nil {
			cancel()
			if isPermanentSchedulerPublishError(err) {
				if moveErr := s.blockPendingRegionEvent(ev, err); moveErr != nil {
					failed = append(failed, ev)
					s.recordTopologyPublishFailure(rootstateTransitionEvent{transitionID: ev.transitionID}, moveErr)
					s.recordLocalSchedulerPublishFailure(ev, moveErr)
					continue
				}
				s.recordTopologyPublishBlocked(rootstateTransitionEvent{transitionID: ev.transitionID}, err)
				s.recordLocalSchedulerPublishFailure(ev, err)
				continue
			}
			failed = append(failed, ev)
			s.recordTopologyPublishFailure(rootstateTransitionEvent{transitionID: ev.transitionID}, err)
			s.recordLocalSchedulerPublishFailure(ev, err)
			continue
		}
		cancel()
		if err := s.deletePendingRegionEvent(ev.seq); err != nil {
			failed = append(failed, ev)
			s.recordTopologyPublishFailure(rootstateTransitionEvent{transitionID: ev.transitionID}, err)
			s.recordLocalSchedulerPublishFailure(ev, err)
			continue
		}
		s.recordTopologyPublished(rootstateTransitionEvent{transitionID: ev.transitionID})
		s.clearLocalSchedulerDegraded()
	}
	if len(failed) == 0 {
		s.sched.publish.mu.Lock()
		s.sched.publish.retryBackoff = 0
		s.sched.publish.retryScheduled = false
		s.sched.publish.mu.Unlock()
		return
	}
	s.sched.publish.mu.Lock()
	for _, ev := range failed {
		s.sched.publish.regionUpdates[ev.seq] = ev
	}
	delay := s.nextRegionFlushRetryLocked()
	shouldSchedule := !s.sched.publish.retryScheduled
	s.sched.publish.retryScheduled = true
	s.sched.publish.mu.Unlock()
	if shouldSchedule {
		s.scheduleRegionFlushRetry(delay)
	}
}

func schedulerRegionEventKey(event rootevent.Event) (uint64, bool) {
	switch {
	case event.RegionDescriptor != nil:
		return event.RegionDescriptor.Descriptor.RegionID, true
	case event.RegionRemoval != nil:
		return event.RegionRemoval.RegionID, true
	case event.PeerChange != nil:
		return event.PeerChange.RegionID, true
	case event.RangeSplit != nil:
		return event.RangeSplit.ParentRegionID, true
	case event.RangeMerge != nil:
		return event.RangeMerge.Merged.RegionID, true
	default:
		return 0, false
	}
}

func (s *Store) signalRegionFlush() {
	if s == nil || s.sched == nil || s.sched.publish.regionSignal == nil {
		return
	}
	select {
	case s.sched.publish.regionSignal <- struct{}{}:
	default:
	}
}

func (s *Store) persistPendingRegionEvent(ev *regionEvent) error {
	if s == nil || s.sched == nil || ev == nil {
		return nil
	}
	s.sched.publish.mu.Lock()
	s.sched.publish.nextRegionSeq++
	ev.seq = s.sched.publish.nextRegionSeq
	s.sched.publish.mu.Unlock()
	if rm := s.regionMgr(); rm != nil && rm.localMeta != nil {
		if err := rm.localMeta.SavePendingRootEvent(localmeta.PendingRootEvent{
			Sequence: ev.seq,
			Event:    ev.root,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) blockPendingRegionEvent(ev regionEvent, cause error) error {
	if s == nil || ev.seq == 0 {
		return nil
	}
	rm := s.regionMgr()
	if rm == nil || rm.localMeta == nil {
		return nil
	}
	return rm.localMeta.MovePendingRootEventToBlocked(ev.seq, localmeta.BlockedRootEvent{
		Sequence:     ev.seq,
		Event:        ev.root,
		TransitionID: ev.transitionID,
		LastError:    errorString(cause),
	})
}

func (s *Store) deletePendingRegionEvent(seq uint64) error {
	if s == nil || seq == 0 {
		return nil
	}
	if rm := s.regionMgr(); rm != nil && rm.localMeta != nil {
		return rm.localMeta.DeletePendingRootEvent(seq)
	}
	return nil
}

func (s *Store) persistPendingSchedulerOperation(item scheduledOp) error {
	if s == nil {
		return nil
	}
	op, ok := localmetaSchedulerOperation(item)
	if !ok {
		return nil
	}
	if rm := s.regionMgr(); rm != nil && rm.localMeta != nil {
		return rm.localMeta.SavePendingSchedulerOperation(op)
	}
	return nil
}

func (s *Store) deletePendingSchedulerOperation(op Operation) error {
	if s == nil {
		return nil
	}
	item, ok := localmetaSchedulerOperation(scheduledOp{op: op})
	if !ok {
		return nil
	}
	if rm := s.regionMgr(); rm != nil && rm.localMeta != nil {
		return rm.localMeta.DeletePendingSchedulerOperation(item.Kind, item.RegionID)
	}
	return nil
}

func localmetaSchedulerOperation(item scheduledOp) (localmeta.PendingSchedulerOperation, bool) {
	switch item.op.Type {
	case OperationLeaderTransfer:
		return localmeta.PendingSchedulerOperation{
			Kind:         localmeta.PendingSchedulerOperationLeaderTransfer,
			RegionID:     item.op.Region,
			SourcePeerID: item.op.Source,
			TargetPeerID: item.op.Target,
			Attempts:     item.attempts,
		}, true
	default:
		return localmeta.PendingSchedulerOperation{}, false
	}
}

func storeOperationFromLocalMeta(op localmeta.PendingSchedulerOperation) (Operation, bool) {
	switch op.Kind {
	case localmeta.PendingSchedulerOperationLeaderTransfer:
		return Operation{
			Type:   OperationLeaderTransfer,
			Region: op.RegionID,
			Source: op.SourcePeerID,
			Target: op.TargetPeerID,
		}, true
	default:
		return Operation{}, false
	}
}

func (s *Store) dispatchPendingSchedulerOperation(item scheduledOp) bool {
	if s == nil || s.sched == nil || s.sched.operation.input == nil {
		return false
	}
	key := operationKey{region: item.op.Region, typeID: item.op.Type}
	s.sched.operation.mu.Lock()
	if queued, exists := s.sched.operation.pending[key]; exists && queued {
		s.sched.operation.mu.Unlock()
		return true
	}
	s.sched.operation.mu.Unlock()
	select {
	case s.sched.operation.input <- item:
		s.sched.operation.mu.Lock()
		if _, exists := s.sched.operation.pending[key]; exists {
			s.sched.operation.pending[key] = true
		}
		s.sched.operation.mu.Unlock()
		return true
	default:
		return false
	}
}

func (s *Store) enqueueRecoveredPendingRegionEvents(events []localmeta.PendingRootEvent) {
	if s == nil || s.sched == nil || len(events) == 0 {
		return
	}
	s.sched.publish.mu.Lock()
	if s.sched.publish.descriptors == nil {
		s.sched.publish.descriptors = make(map[uint64]descriptor.Descriptor)
	}
	if s.sched.publish.regionUpdates == nil {
		s.sched.publish.regionUpdates = make(map[uint64]regionEvent)
	}
	for _, item := range events {
		ev := regionEvent{
			root:         rootevent.CloneEvent(item.Event),
			transitionID: rootstate.TransitionIDFromEvent(item.Event),
			seq:          item.Sequence,
		}
		if ev.seq > s.sched.publish.nextRegionSeq {
			s.sched.publish.nextRegionSeq = ev.seq
		}
		rootmaterialize.ApplyEventToDescriptors(s.sched.publish.descriptors, ev.root)
		s.sched.publish.regionUpdates[ev.seq] = ev
	}
	s.sched.publish.mu.Unlock()
	s.signalRegionFlush()
}

func (s *Store) enqueueRecoveredPendingSchedulerOperations(ops []localmeta.PendingSchedulerOperation) {
	if s == nil || s.sched == nil || len(ops) == 0 {
		return
	}
	for _, item := range ops {
		op, ok := storeOperationFromLocalMeta(item)
		if !ok {
			continue
		}
		if item.Attempts >= maxSchedulerOperationAttempts {
			s.abandonScheduledOperation(scheduledOp{op: op, attempts: item.Attempts}, "attempt limit reached")
			continue
		}
		key := operationKey{region: op.Region, typeID: op.Type}
		s.sched.operation.mu.Lock()
		s.sched.operation.pending[key] = false
		s.sched.operation.mu.Unlock()
		if s.dispatchPendingSchedulerOperation(scheduledOp{op: op, attempts: item.Attempts}) {
			s.clearLocalSchedulerDegraded()
			continue
		}
		s.recordLocalSchedulerBacklog(op, "queue full")
	}
}

func (s *Store) retryPendingSchedulerOperations() {
	if s == nil || s.sched == nil || s.sched.operation.input == nil {
		return
	}
	rm := s.regionMgr()
	if rm == nil || rm.localMeta == nil {
		return
	}
	for _, item := range rm.localMeta.PendingSchedulerOperations() {
		op, ok := storeOperationFromLocalMeta(item)
		if !ok {
			continue
		}
		if item.Attempts >= maxSchedulerOperationAttempts {
			s.abandonScheduledOperation(scheduledOp{op: op, attempts: item.Attempts}, "attempt limit reached")
			continue
		}
		if s.dispatchPendingSchedulerOperation(scheduledOp{op: op, attempts: item.Attempts}) {
			s.clearLocalSchedulerDegraded()
			continue
		}
		s.recordLocalSchedulerBacklog(op, "queue full")
		return
	}
}

func (s *Store) stopOperationLoop() {
	if s == nil || s.sched == nil || s.sched.operation.stop == nil {
		return
	}
	close(s.sched.operation.stop)
	s.sched.operation.wg.Wait()
	s.sched.operation.stop = nil
	s.sched.operation.input = nil
}

func (s *Store) runOperationLoop() {
	defer s.sched.operation.wg.Done()
	interval := s.sched.operation.interval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var pending []scheduledOp
	for {
		select {
		case <-s.sched.operation.stop:
			if !s.hasDurableSchedulerQueue() {
				s.dropQueuedOperations(flattenScheduledOps(pending), "store closing")
				s.dropQueuedOperations(s.drainBufferedOperations(), "store closing")
			}
			return
		case item := <-s.sched.operation.input:
			if item.ready.IsZero() {
				item.ready = s.nextOperationReadyTime(item.op)
			}
			pending = append(pending, item)
		case <-ticker.C:
			now := time.Now()
			limit := s.sched.operation.burst
			if limit <= 0 {
				limit = len(pending)
			}
			applied := 0
			var remaining []scheduledOp
			for _, item := range pending {
				if limit > 0 && applied >= limit {
					remaining = append(remaining, item)
					continue
				}
				if !item.ready.IsZero() && item.ready.After(now) {
					remaining = append(remaining, item)
					continue
				}
				if s.applyOperation(item.op) {
					if s.markOperationApplied(item.op, now) {
						applied++
						continue
					}
				}
				item.attempts++
				if item.attempts >= maxSchedulerOperationAttempts {
					s.abandonScheduledOperation(item, "attempt limit reached")
					continue
				}
				if err := s.persistPendingSchedulerOperation(item); err != nil {
					s.abandonScheduledOperation(item, fmt.Sprintf("persist retry failed: %v", err))
					continue
				}
				item.ready = now.Add(s.nextOperationRetryDelay(item.attempts))
				remaining = append(remaining, item)
			}
			pending = remaining
		}
	}
}

func (s *Store) nextOperationReadyTime(op Operation) time.Time {
	if s == nil || s.sched == nil || s.sched.operation.cooldown <= 0 {
		return time.Time{}
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.sched.operation.mu.Lock()
	defer s.sched.operation.mu.Unlock()
	last := s.sched.operation.lastApply[key]
	if last.IsZero() {
		return time.Time{}
	}
	return last.Add(s.sched.operation.cooldown)
}

func (s *Store) nextOperationRetryDelay(attempts uint32) time.Duration {
	if attempts == 0 {
		return 0
	}
	delay := defaultSchedulerOperationRetryBackoff
	for i := uint32(1); i < attempts; i++ {
		if delay >= maxSchedulerOperationRetryBackoff/2 {
			return maxSchedulerOperationRetryBackoff
		}
		delay *= 2
	}
	if delay > maxSchedulerOperationRetryBackoff {
		return maxSchedulerOperationRetryBackoff
	}
	return delay
}

func (s *Store) markOperationApplied(op Operation, ts time.Time) bool {
	if s == nil {
		return false
	}
	if err := s.deletePendingSchedulerOperation(op); err != nil {
		key := operationKey{region: op.Region, typeID: op.Type}
		s.sched.operation.mu.Lock()
		if _, exists := s.sched.operation.pending[key]; exists {
			s.sched.operation.pending[key] = false
		}
		s.sched.operation.mu.Unlock()
		s.recordLocalSchedulerBacklog(op, fmt.Sprintf("persist delete failed: %v", err))
		return false
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.sched.operation.mu.Lock()
	if ts.IsZero() {
		delete(s.sched.operation.lastApply, key)
	} else {
		s.sched.operation.lastApply[key] = ts
	}
	delete(s.sched.operation.pending, key)
	s.sched.operation.mu.Unlock()
	return true
}

func (s *Store) clearLocalSchedulerDegraded() {
	if s == nil {
		return
	}
	s.sched.health.mu.Lock()
	s.sched.health.degraded = false
	if s.sched.health.lastError == "" {
		s.sched.health.lastErrorAt = time.Time{}
	}
	s.sched.health.mu.Unlock()
}

func (s *Store) recordLocalSchedulerDrop(op Operation) {
	s.dropQueuedOperations([]Operation{op}, "queue full")
}

func (s *Store) recordLocalSchedulerBacklog(op Operation, reason string) {
	if s == nil || s.sched == nil {
		return
	}
	now := time.Now()
	msg := fmt.Sprintf("scheduler %s: pending %s for region %d", reason, op.Type.String(), op.Region)
	s.sched.health.mu.Lock()
	s.sched.health.degraded = true
	s.sched.health.lastError = msg
	s.sched.health.lastErrorAt = now
	s.sched.health.mu.Unlock()
}

func (s *Store) abandonScheduledOperation(item scheduledOp, reason string) {
	if s == nil {
		return
	}
	_ = s.deletePendingSchedulerOperation(item.op)
	s.dropQueuedOperations([]Operation{item.op}, reason)
}

func (s *Store) dropQueuedOperations(ops []Operation, reason string) {
	if s == nil || s.sched == nil {
		return
	}
	if len(ops) == 0 {
		return
	}
	now := time.Now()
	s.sched.operation.mu.Lock()
	s.sched.operation.dropped += uint64(len(ops))
	for _, op := range ops {
		key := operationKey{region: op.Region, typeID: op.Type}
		delete(s.sched.operation.pending, key)
	}
	s.sched.operation.mu.Unlock()
	msg := fmt.Sprintf("scheduler %s: dropped %d operation(s)", reason, len(ops))
	if len(ops) == 1 {
		msg = fmt.Sprintf("scheduler %s: dropped %s for region %d", reason, ops[0].Type.String(), ops[0].Region)
	}
	s.sched.health.mu.Lock()
	s.sched.health.degraded = true
	s.sched.health.lastError = msg
	s.sched.health.lastErrorAt = now
	s.sched.health.mu.Unlock()
	slog.Default().Warn(msg)
}

func (s *Store) recordLocalSchedulerPublishFailure(ev regionEvent, err error) {
	if s == nil || s.sched == nil || err == nil {
		return
	}
	regionID, _ := schedulerRegionEventKey(ev.root)
	msg := fmt.Sprintf("scheduler publish failed: region=%d kind=%d err=%v", regionID, ev.root.Kind, err)
	now := time.Now()
	s.sched.health.mu.Lock()
	s.sched.health.degraded = true
	s.sched.health.lastError = msg
	s.sched.health.lastErrorAt = now
	s.sched.health.mu.Unlock()
	slog.Default().Warn(msg)
}

func isPermanentSchedulerPublishError(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.AlreadyExists, codes.PermissionDenied, codes.Unimplemented:
		return true
	default:
		return false
	}
}

func (s *Store) storeStatsSnapshot() StoreStats {
	if s == nil {
		return StoreStats{}
	}
	leaderRegions := s.leaderRegionIDs()
	s.addressMu.RLock()
	clientAddr := s.clientAddr
	raftAddr := s.raftAddr
	s.addressMu.RUnlock()
	stats := StoreStats{
		StoreID:         s.storeID,
		ClientAddr:      clientAddr,
		RaftAddr:        raftAddr,
		RegionNum:       uint64(len(s.RegionMetas())),
		LeaderNum:       uint64(len(leaderRegions)),
		LeaderRegionIDs: leaderRegions,
	}
	if s.sched != nil {
		s.sched.operation.mu.Lock()
		stats.DroppedOperations = s.sched.operation.dropped
		s.sched.operation.mu.Unlock()
	}
	if capacity, available, ok := s.diskStats(); ok {
		stats.Capacity = capacity
		stats.Available = available
	}
	return stats
}

func flattenScheduledOps(pending []scheduledOp) []Operation {
	if len(pending) == 0 {
		return nil
	}
	out := make([]Operation, 0, len(pending))
	for _, item := range pending {
		out = append(out, item.op)
	}
	return out
}

func (s *Store) drainBufferedOperations() []Operation {
	if s == nil || s.sched == nil || s.sched.operation.input == nil {
		return nil
	}
	var drained []Operation
	for {
		select {
		case item := <-s.sched.operation.input:
			drained = append(drained, item.op)
		default:
			return drained
		}
	}
}

func (s *Store) nextRegionFlushRetryLocked() time.Duration {
	if s == nil || s.sched == nil {
		return defaultPublishRetryBackoff
	}
	if s.sched.publish.retryBackoff <= 0 {
		s.sched.publish.retryBackoff = defaultPublishRetryBackoff
		return jitterDuration(defaultPublishRetryBackoff, 20)
	}
	next := min(s.sched.publish.retryBackoff*2, maxPublishRetryBackoff)
	s.sched.publish.retryBackoff = next
	return jitterDuration(next, 20)
}

func (s *Store) scheduleRegionFlushRetry(delay time.Duration) {
	if s == nil || s.sched == nil {
		return
	}
	stop := s.sched.publish.heartbeatStop
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		if stop == nil {
			<-timer.C
		} else {
			select {
			case <-timer.C:
			case <-stop:
				return
			}
		}
		s.sched.publish.mu.Lock()
		s.sched.publish.retryScheduled = false
		s.sched.publish.mu.Unlock()
		s.signalRegionFlush()
	}()
}

func jitterDuration(base time.Duration, percent int64) time.Duration {
	if base <= 0 || percent <= 0 {
		return base
	}
	window := percent*2 + 1
	offsetPercent := (time.Now().UnixNano() % window) - percent
	jittered := base + time.Duration(int64(base)*offsetPercent/100)
	if jittered < 10*time.Millisecond {
		return 10 * time.Millisecond
	}
	return jittered
}

// leaderRegionIDs returns the sorted list of region ids for which this
// store's peer is the raft leader at call time. The store reports this in
// StoreHeartbeat so the coordinator can populate its rooted region
// directory with authoritative per-region leadership without needing a
// separate RegionHeartbeat RPC per region.
func (s *Store) leaderRegionIDs() []uint64 {
	if s == nil {
		return nil
	}
	var ids []uint64
	s.VisitPeers(func(p *peer.Peer) {
		if p.Status().RaftState != myraft.StateLeader {
			return
		}
		meta := p.RegionMeta()
		if meta == nil {
			return
		}
		ids = append(ids, meta.ID)
	})
	slices.Sort(ids)
	return ids
}

func (s *Store) diskStats() (uint64, uint64, bool) {
	if s == nil || s.workDir == "" {
		return 0, 0, false
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.workDir, &st); err != nil {
		return 0, 0, false
	}
	capacity := uint64(st.Blocks) * uint64(st.Bsize)
	available := uint64(st.Bavail) * uint64(st.Bsize)
	return capacity, available, true
}
