package store

import (
	"fmt"
	"log/slog"
	"syscall"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

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
	if s.command == nil {
		return fmt.Errorf("raftstore: command apply without handler")
	}
	return s.command.applyEntries(entries)
}

func (s *Store) enqueueOperation(op Operation) {
	if s == nil {
		return
	}
	if op.Type == OperationNone || op.Region == 0 {
		return
	}
	if s.operationInput == nil {
		s.clearLocalSchedulerDegraded()
		s.applyOperation(op)
		return
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.operationMu.Lock()
	if _, exists := s.operationPending[key]; exists {
		s.operationMu.Unlock()
		return
	}
	s.operationPending[key] = struct{}{}
	s.operationMu.Unlock()
	select {
	case s.operationInput <- op:
		s.clearLocalSchedulerDegraded()
	default:
		s.operationMu.Lock()
		delete(s.operationPending, key)
		s.operationMu.Unlock()
		s.recordLocalSchedulerDrop(op)
	}
}

func (s *Store) startHeartbeatLoop() {
	if s == nil || s.scheduler == nil || s.heartbeatInterval <= 0 || s.heartbeatStop != nil {
		return
	}
	s.heartbeatStop = make(chan struct{})
	s.sendHeartbeats()
	s.heartbeatWG.Add(1)
	go s.runHeartbeatLoop()
}

func (s *Store) stopHeartbeatLoop() {
	if s == nil || s.heartbeatStop == nil {
		return
	}
	close(s.heartbeatStop)
	s.heartbeatWG.Wait()
	s.heartbeatStop = nil
}

func (s *Store) runHeartbeatLoop() {
	defer s.heartbeatWG.Done()
	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sendHeartbeats()
		case <-s.heartbeatStop:
			return
		}
	}
}

func (s *Store) sendHeartbeats() {
	if s == nil || s.scheduler == nil {
		return
	}
	for _, meta := range s.RegionMetas() {
		s.scheduler.PublishRegion(meta)
	}
	if s.storeID == 0 {
		return
	}
	for _, op := range s.scheduler.StoreHeartbeat(s.storeStatsSnapshot()) {
		s.enqueueOperation(op)
	}
}

func (s *Store) stopOperationLoop() {
	if s == nil || s.operationStop == nil {
		return
	}
	close(s.operationStop)
	s.operationWG.Wait()
	s.operationStop = nil
}

func (s *Store) runOperationLoop() {
	defer s.operationWG.Done()
	interval := s.operationInterval
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
		case <-s.operationStop:
			return
		case op := <-s.operationInput:
			pending = append(pending, scheduledOp{op: op, ready: s.nextOperationReadyTime(op)})
		case <-ticker.C:
			now := time.Now()
			limit := s.operationBurst
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
	if s == nil || s.operationCooldown <= 0 {
		return time.Time{}
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	last := s.operationLastApply[key]
	if last.IsZero() {
		return time.Time{}
	}
	return last.Add(s.operationCooldown)
}

func (s *Store) markOperationApplied(op Operation, ts time.Time) {
	if s == nil {
		return
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.operationMu.Lock()
	if ts.IsZero() {
		delete(s.operationLastApply, key)
	} else {
		s.operationLastApply[key] = ts
	}
	delete(s.operationPending, key)
	s.operationMu.Unlock()
}

func (s *Store) clearLocalSchedulerDegraded() {
	if s == nil {
		return
	}
	s.operationMu.Lock()
	s.schedulerDegraded = false
	s.operationMu.Unlock()
}

func (s *Store) recordLocalSchedulerDrop(op Operation) {
	if s == nil {
		return
	}
	msg := fmt.Sprintf("scheduler queue full: dropped %s for region %d", op.Type.String(), op.Region)
	now := time.Now()
	s.operationMu.Lock()
	s.schedulerDropped++
	s.schedulerDegraded = true
	s.schedulerLastError = msg
	s.schedulerLastAt = now
	s.operationMu.Unlock()
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
