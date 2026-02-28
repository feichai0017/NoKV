package adapter

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	pdclient "github.com/feichai0017/NoKV/pd/client"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
)

const defaultRPCtimeout = 2 * time.Second

// RegionSinkConfig defines how a PD-backed scheduler sink behaves.
type RegionSinkConfig struct {
	PD      pdclient.Client
	Mirror  scheduler.RegionSink
	Timeout time.Duration
	OnError func(op string, err error)
}

// RegionSink forwards scheduler heartbeats to PD and optionally mirrors them to
// another local sink (e.g. scheduler.Coordinator for debugging snapshots).
type RegionSink struct {
	mu      sync.Mutex
	pd      pdclient.Client
	mirror  scheduler.RegionSink
	timeout time.Duration
	onError func(op string, err error)
	pending []scheduler.Operation
}

// NewRegionSink constructs a PD-backed RegionSink.
func NewRegionSink(cfg RegionSinkConfig) *RegionSink {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultRPCtimeout
	}
	onErr := cfg.OnError
	if onErr == nil {
		onErr = func(op string, err error) {
			log.Printf("pd adapter: %s failed: %v", op, err)
		}
	}
	return &RegionSink{
		pd:      cfg.PD,
		mirror:  cfg.Mirror,
		timeout: timeout,
		onError: onErr,
	}
}

// SubmitRegionHeartbeat publishes region metadata to PD and mirror sink.
func (s *RegionSink) SubmitRegionHeartbeat(meta manifest.RegionMeta) {
	if s == nil || meta.ID == 0 {
		return
	}
	if s.mirror != nil {
		s.mirror.SubmitRegionHeartbeat(meta)
	}
	if s.pd == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	_, err := s.pd.RegionHeartbeat(ctx, &pb.RegionHeartbeatRequest{Region: toPBRegionMeta(meta)})
	if err != nil {
		s.onError("RegionHeartbeat", err)
	}
}

// RemoveRegion removes local mirror state. PD-side remove RPC is not added yet.
func (s *RegionSink) RemoveRegion(regionID uint64) {
	if s == nil || regionID == 0 {
		return
	}
	if s.mirror != nil {
		s.mirror.RemoveRegion(regionID)
	}
}

// SubmitStoreHeartbeat publishes store stats to PD and mirror sink.
func (s *RegionSink) SubmitStoreHeartbeat(stats scheduler.StoreStats) {
	if s == nil || stats.StoreID == 0 {
		return
	}
	if s.mirror != nil {
		s.mirror.SubmitStoreHeartbeat(stats)
	}
	if s.pd == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	resp, err := s.pd.StoreHeartbeat(ctx, &pb.StoreHeartbeatRequest{
		StoreId:   stats.StoreID,
		RegionNum: stats.RegionNum,
		LeaderNum: stats.LeaderNum,
		Capacity:  stats.Capacity,
		Available: stats.Available,
	})
	if err != nil {
		s.onError("StoreHeartbeat", err)
		return
	}
	s.enqueueOperations(resp.GetOperations())
}

// Plan returns and drains pending scheduling operations received from PD.
func (s *RegionSink) Plan(_ scheduler.Snapshot) []scheduler.Operation {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) == 0 {
		return nil
	}
	out := make([]scheduler.Operation, len(s.pending))
	copy(out, s.pending)
	s.pending = s.pending[:0]
	return out
}

func (s *RegionSink) enqueueOperations(ops []*pb.SchedulerOperation) {
	if s == nil || len(ops) == 0 {
		return
	}
	converted := make([]scheduler.Operation, 0, len(ops))
	for _, op := range ops {
		if next, ok := fromPBOperation(op); ok {
			converted = append(converted, next)
		}
	}
	if len(converted) == 0 {
		return
	}
	s.mu.Lock()
	s.pending = append(s.pending, converted...)
	s.mu.Unlock()
}

func fromPBOperation(op *pb.SchedulerOperation) (scheduler.Operation, bool) {
	if op == nil {
		return scheduler.Operation{}, false
	}
	switch op.GetType() {
	case pb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER:
		if op.GetRegionId() == 0 || op.GetSourcePeerId() == 0 || op.GetTargetPeerId() == 0 {
			return scheduler.Operation{}, false
		}
		return scheduler.Operation{
			Type:   scheduler.OperationLeaderTransfer,
			Region: op.GetRegionId(),
			Source: op.GetSourcePeerId(),
			Target: op.GetTargetPeerId(),
		}, true
	default:
		return scheduler.Operation{}, false
	}
}

// RegionSnapshot exposes mirror snapshots when mirror implements SnapshotProvider.
func (s *RegionSink) RegionSnapshot() []scheduler.RegionInfo {
	if s == nil {
		return nil
	}
	provider, ok := s.mirror.(scheduler.SnapshotProvider)
	if !ok {
		return nil
	}
	return provider.RegionSnapshot()
}

// StoreSnapshot exposes mirror snapshots when mirror implements SnapshotProvider.
func (s *RegionSink) StoreSnapshot() []scheduler.StoreStats {
	if s == nil {
		return nil
	}
	provider, ok := s.mirror.(scheduler.SnapshotProvider)
	if !ok {
		return nil
	}
	return provider.StoreSnapshot()
}

// Close closes the PD client if present.
func (s *RegionSink) Close() error {
	if s == nil || s.pd == nil {
		return nil
	}
	return s.pd.Close()
}

func toPBRegionMeta(meta manifest.RegionMeta) *pb.RegionMeta {
	out := &pb.RegionMeta{
		Id:               meta.ID,
		StartKey:         append([]byte(nil), meta.StartKey...),
		EndKey:           append([]byte(nil), meta.EndKey...),
		EpochVersion:     meta.Epoch.Version,
		EpochConfVersion: meta.Epoch.ConfVersion,
	}
	if len(meta.Peers) == 0 {
		return out
	}
	out.Peers = make([]*pb.RegionPeer, 0, len(meta.Peers))
	for _, p := range meta.Peers {
		out.Peers = append(out.Peers, &pb.RegionPeer{
			StoreId: p.StoreID,
			PeerId:  p.PeerID,
		})
	}
	return out
}
