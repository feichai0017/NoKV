package store

import (
	"fmt"
	"syscall"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
)

func (s *Store) applyOperation(op scheduler.Operation) bool {
	if s == nil {
		return false
	}
	switch op.Type {
	case scheduler.OperationLeaderTransfer:
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

func (s *Store) enqueueOperation(op scheduler.Operation) {
	if s == nil || s.operations == nil {
		return
	}
	s.operations.enqueue(op)
}

func (s *Store) storeStatsSnapshot() scheduler.StoreStats {
	stats := scheduler.StoreStats{
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

// SchedulerSnapshot returns the scheduler snapshot if the store is connected to
// a coordinator that implements SnapshotProvider. When unavailable, an empty
// snapshot is returned.
func (s *Store) SchedulerSnapshot() scheduler.Snapshot {
	if s == nil {
		return scheduler.Snapshot{}
	}
	snap := scheduler.Snapshot{}
	if provider, ok := s.scheduler.(scheduler.SnapshotProvider); ok {
		regions := provider.RegionSnapshot()
		stores := provider.StoreSnapshot()
		snap.Stores = append(snap.Stores, stores...)
		for _, info := range regions {
			snap.Regions = append(snap.Regions, s.buildRegionDescriptor(info))
		}
	}
	return snap
}

func (s *Store) buildRegionDescriptor(info scheduler.RegionInfo) scheduler.RegionDescriptor {
	meta := info.Meta
	desc := scheduler.RegionDescriptor{
		ID:            meta.ID,
		StartKey:      append([]byte(nil), meta.StartKey...),
		EndKey:        append([]byte(nil), meta.EndKey...),
		Epoch:         meta.Epoch,
		LastHeartbeat: info.LastHeartbeat,
	}
	var leaderPeerID uint64
	if local := s.regions.peer(meta.ID); local != nil {
		if local.Status().RaftState == myraft.StateLeader {
			leaderPeerID = local.ID()
		}
	}
	if !info.LastHeartbeat.IsZero() {
		lag := max(time.Since(info.LastHeartbeat), 0)
		desc.Lag = lag
	}
	for _, peerMeta := range meta.Peers {
		desc.Peers = append(desc.Peers, scheduler.PeerDescriptor{
			StoreID: peerMeta.StoreID,
			PeerID:  peerMeta.PeerID,
			Leader:  peerMeta.PeerID == leaderPeerID,
		})
	}
	return desc
}

func (s *Store) diskStats() (uint64, uint64, bool) {
	if s == nil || s.manifest == nil {
		return 0, 0, false
	}
	dir := s.manifest.Dir()
	if dir == "" {
		return 0, 0, false
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, false
	}
	capacity := uint64(st.Blocks) * uint64(st.Bsize)
	available := uint64(st.Bavail) * uint64(st.Bsize)
	return capacity, available, true
}
