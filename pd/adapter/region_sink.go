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

const defaultRPCTimeout = 2 * time.Second

// RegionSinkConfig defines how a PD-backed scheduler sink behaves.
type RegionSinkConfig struct {
	PD      pdclient.Client
	Timeout time.Duration
	OnError func(op string, err error)
}

// RegionSink forwards scheduler heartbeats to PD.
type RegionSink struct {
	mu      sync.Mutex
	pd      pdclient.Client
	timeout time.Duration
	onError func(op string, err error)
	pending []scheduler.Operation
}

// NewRegionSink constructs a PD-backed RegionSink.
func NewRegionSink(cfg RegionSinkConfig) *RegionSink {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}
	onErr := cfg.OnError
	if onErr == nil {
		onErr = func(op string, err error) {
			log.Printf("pd adapter: %s failed: %v", op, err)
		}
	}
	return &RegionSink{
		pd:      cfg.PD,
		timeout: timeout,
		onError: onErr,
	}
}

// SubmitRegionHeartbeat publishes region metadata to PD.
func (s *RegionSink) SubmitRegionHeartbeat(meta manifest.RegionMeta) {
	if s == nil || meta.ID == 0 {
		return
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

// RemoveRegion removes region metadata from PD.
func (s *RegionSink) RemoveRegion(regionID uint64) {
	if s == nil || regionID == 0 {
		return
	}
	if s.pd == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	_, err := s.pd.RemoveRegion(ctx, &pb.RemoveRegionRequest{RegionId: regionID})
	if err != nil {
		s.onError("RemoveRegion", err)
	}
}

// SubmitStoreHeartbeat publishes store stats to PD.
func (s *RegionSink) SubmitStoreHeartbeat(stats scheduler.StoreStats) {
	if s == nil || stats.StoreID == 0 {
		return
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
// Snapshot input is intentionally ignored because scheduling is centralized in
// PD; this method only forwards already-decided operations into raftstore.
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
