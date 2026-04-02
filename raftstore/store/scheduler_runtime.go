package store

import (
	"fmt"
	"log/slog"
	"sort"
	"syscall"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

type operationKey struct {
	region uint64
	typeID OperationType
}

type regionEventKind uint8

const (
	regionEventNone regionEventKind = iota
	regionEventApply
	regionEventRemove
)

type regionEvent struct {
	kind     regionEventKind
	regionID uint64
	meta     localmeta.RegionMeta
	seq      uint64
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
	s.sched.mu.Lock()
	defer s.sched.mu.Unlock()
	status.DroppedOperations += s.sched.dropped
	if s.sched.degraded {
		status.Degraded = true
		if status.Mode != SchedulerModeUnavailable {
			status.Mode = SchedulerModeDegraded
		}
		if status.LastErrorAt.Before(s.sched.lastErrorAt) || status.LastError == "" {
			status.LastError = s.sched.lastError
			status.LastErrorAt = s.sched.lastErrorAt
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

func (s *Store) applyEntries(entries []myraft.Entry) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if s.commandPipe() == nil {
		return fmt.Errorf("raftstore: command apply without handler")
	}
	return s.commandPipe().applyEntries(entries)
}

func (s *Store) enqueueOperation(op Operation) {
	if s == nil {
		return
	}
	if op.Type == OperationNone || op.Region == 0 {
		return
	}
	if s.sched == nil || s.sched.input == nil {
		s.clearLocalSchedulerDegraded()
		s.applyOperation(op)
		return
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.sched.mu.Lock()
	if _, exists := s.sched.pending[key]; exists {
		s.sched.mu.Unlock()
		return
	}
	s.sched.pending[key] = struct{}{}
	s.sched.mu.Unlock()
	select {
	case s.sched.input <- op:
		s.clearLocalSchedulerDegraded()
	default:
		s.sched.mu.Lock()
		delete(s.sched.pending, key)
		s.sched.mu.Unlock()
		s.recordLocalSchedulerDrop(op)
	}
}

func (s *Store) startHeartbeatLoop() {
	if s == nil || s.schedulerClient() == nil || s.sched == nil || s.sched.heartbeat <= 0 || s.sched.heartbeatStop != nil {
		return
	}
	s.sched.heartbeatStop = make(chan struct{})
	s.sched.regionSignal = make(chan struct{}, 1)
	s.sendHeartbeats()
	s.sched.heartbeatWG.Add(1)
	go s.runHeartbeatLoop()
}

func (s *Store) stopHeartbeatLoop() {
	if s == nil || s.sched == nil || s.sched.heartbeatStop == nil {
		return
	}
	close(s.sched.heartbeatStop)
	s.sched.heartbeatWG.Wait()
	s.sched.heartbeatStop = nil
	s.sched.regionSignal = nil
}

func (s *Store) runHeartbeatLoop() {
	defer s.sched.heartbeatWG.Done()
	ticker := time.NewTicker(s.sched.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sendHeartbeats()
		case <-s.sched.regionSignal:
			s.flushRegionUpdates()
		case <-s.sched.heartbeatStop:
			return
		}
	}
}

func (s *Store) sendHeartbeats() {
	if s == nil || s.schedulerClient() == nil {
		return
	}
	ctx := s.runtimeContext()
	for _, meta := range s.RegionMetas() {
		s.schedulerClient().PublishRegion(ctx, meta)
	}
	if s.storeID == 0 {
		return
	}
	for _, op := range s.schedulerClient().StoreHeartbeat(ctx, s.storeStatsSnapshot()) {
		s.enqueueOperation(op)
	}
}

func (s *Store) enqueueRegionEvent(ev regionEvent) {
	if s == nil || s.schedulerClient() == nil || s.sched == nil {
		return
	}
	switch ev.kind {
	case regionEventApply:
		if ev.meta.ID == 0 {
			return
		}
		ev.regionID = ev.meta.ID
		ev.meta = localmeta.CloneRegionMeta(ev.meta)
	case regionEventRemove:
		if ev.regionID == 0 {
			return
		}
	default:
		return
	}
	s.sched.mu.Lock()
	if s.sched.regionUpdates == nil {
		s.sched.regionUpdates = make(map[uint64]regionEvent)
	}
	s.sched.nextRegionSeq++
	ev.seq = s.sched.nextRegionSeq
	s.sched.regionUpdates[ev.regionID] = ev
	signal := s.sched.regionSignal
	s.sched.mu.Unlock()
	if signal == nil {
		return
	}
	select {
	case signal <- struct{}{}:
	default:
	}
}

func (s *Store) flushRegionUpdates() {
	if s == nil || s.schedulerClient() == nil || s.sched == nil {
		return
	}
	s.sched.mu.Lock()
	if len(s.sched.regionUpdates) == 0 {
		s.sched.mu.Unlock()
		return
	}
	pending := make([]regionEvent, 0, len(s.sched.regionUpdates))
	for _, ev := range s.sched.regionUpdates {
		pending = append(pending, ev)
	}
	clear(s.sched.regionUpdates)
	s.sched.mu.Unlock()
	sort.Slice(pending, func(i, j int) bool { return pending[i].seq < pending[j].seq })

	ctx := s.runtimeContext()
	for _, ev := range pending {
		switch ev.kind {
		case regionEventApply:
			s.schedulerClient().PublishRegion(ctx, ev.meta)
		case regionEventRemove:
			s.schedulerClient().RemoveRegion(ctx, ev.regionID)
		}
	}
}

func (s *Store) stopOperationLoop() {
	if s == nil || s.sched == nil || s.sched.stop == nil {
		return
	}
	close(s.sched.stop)
	s.sched.wg.Wait()
	s.sched.stop = nil
}

func (s *Store) runOperationLoop() {
	defer s.sched.wg.Done()
	interval := s.sched.interval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	type scheduledOp struct {
		op    Operation
		ready time.Time
	}
	var pending []scheduledOp
	for {
		select {
		case <-s.sched.stop:
			return
		case op := <-s.sched.input:
			pending = append(pending, scheduledOp{op: op, ready: s.nextOperationReadyTime(op)})
		case <-ticker.C:
			now := time.Now()
			limit := s.sched.burst
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
					s.markOperationApplied(item.op, now)
					applied++
					continue
				}
				remaining = append(remaining, item)
			}
			pending = remaining
		}
	}
}

func (s *Store) nextOperationReadyTime(op Operation) time.Time {
	if s == nil || s.sched == nil || s.sched.cooldown <= 0 {
		return time.Time{}
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.sched.mu.Lock()
	defer s.sched.mu.Unlock()
	last := s.sched.lastApply[key]
	if last.IsZero() {
		return time.Time{}
	}
	return last.Add(s.sched.cooldown)
}

func (s *Store) markOperationApplied(op Operation, ts time.Time) {
	if s == nil {
		return
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.sched.mu.Lock()
	if ts.IsZero() {
		delete(s.sched.lastApply, key)
	} else {
		s.sched.lastApply[key] = ts
	}
	delete(s.sched.pending, key)
	s.sched.mu.Unlock()
}

func (s *Store) clearLocalSchedulerDegraded() {
	if s == nil {
		return
	}
	s.sched.mu.Lock()
	s.sched.degraded = false
	if s.sched.lastError == "" {
		s.sched.lastErrorAt = time.Time{}
	}
	s.sched.mu.Unlock()
}

func (s *Store) recordLocalSchedulerDrop(op Operation) {
	if s == nil {
		return
	}
	msg := fmt.Sprintf("scheduler queue full: dropped %s for region %d", op.Type.String(), op.Region)
	now := time.Now()
	s.sched.mu.Lock()
	s.sched.dropped++
	s.sched.degraded = true
	s.sched.lastError = msg
	s.sched.lastErrorAt = now
	s.sched.mu.Unlock()
	slog.Default().Warn(msg)
}

func (s *Store) storeStatsSnapshot() StoreStats {
	stats := StoreStats{
		StoreID:   s.storeID,
		RegionNum: uint64(len(s.RegionMetas())),
		LeaderNum: s.countLeaders(),
	}
	if capacity, available, ok := s.diskStats(); ok {
		stats.Capacity = capacity
		stats.Available = available
	}
	return stats
}

func (s *Store) countLeaders() uint64 {
	if s == nil {
		return 0
	}
	var leaders uint64
	s.VisitPeers(func(p *peer.Peer) {
		if p.Status().RaftState == myraft.StateLeader {
			leaders++
		}
	})
	return leaders
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
