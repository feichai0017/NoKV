package adapter

import (
	"context"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	"log/slog"
	"sync"
	"time"

	pdclient "github.com/feichai0017/NoKV/pd/client"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

const defaultRPCTimeout = 2 * time.Second

// SchedulerClientConfig defines how a PD-backed scheduler client behaves.
type SchedulerClientConfig struct {
	PD      pdclient.Client
	Timeout time.Duration
	OnError func(op string, err error)
}

// SchedulerClient forwards region/store metadata to PD and returns the
// scheduling operations PD wants the store to apply.
type SchedulerClient struct {
	pd      pdclient.Client
	timeout time.Duration
	onError func(op string, err error)
	mu      sync.RWMutex
	status  storepkg.SchedulerStatus
}

// NewSchedulerClient constructs a PD-backed scheduler client.
func NewSchedulerClient(cfg SchedulerClientConfig) *SchedulerClient {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}
	onErr := cfg.OnError
	if onErr == nil {
		onErr = func(op string, err error) {
			slog.Default().Warn("pd adapter operation failed", "op", op, "err", err)
		}
	}
	return &SchedulerClient{
		pd:      cfg.PD,
		timeout: timeout,
		onError: onErr,
	}
}

// PublishRegionDescriptor publishes one region descriptor to PD.
func (s *SchedulerClient) PublishRegionDescriptor(ctx context.Context, desc descriptor.Descriptor) {
	if s == nil || desc.RegionID == 0 || s.pd == nil {
		return
	}
	ctx, cancel := contextWithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.pd.RegionHeartbeat(ctx, &pdpb.RegionHeartbeatRequest{RegionDescriptor: metacodec.DescriptorToProto(desc)})
	if err != nil {
		s.recordError("RegionHeartbeat", err)
		return
	}
	s.markHealthy()
}

// PublishRootEvent publishes one explicit rooted truth event to PD.
func (s *SchedulerClient) PublishRootEvent(ctx context.Context, event rootevent.Event) {
	if s == nil || event.Kind == rootevent.KindUnknown || s.pd == nil {
		return
	}
	ctx, cancel := contextWithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.pd.PublishRootEvent(ctx, &pdpb.PublishRootEventRequest{Event: metacodec.RootEventToProto(event)})
	if err != nil {
		s.recordError("PublishRootEvent", err)
		return
	}
	s.markHealthy()
}

// StoreHeartbeat publishes store stats to PD and returns any operations PD
// wants the store to apply.
func (s *SchedulerClient) StoreHeartbeat(ctx context.Context, stats storepkg.StoreStats) []storepkg.Operation {
	if s == nil || stats.StoreID == 0 || s.pd == nil {
		return nil
	}
	ctx, cancel := contextWithTimeout(ctx, s.timeout)
	defer cancel()
	resp, err := s.pd.StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
		StoreId:   stats.StoreID,
		RegionNum: stats.RegionNum,
		LeaderNum: stats.LeaderNum,
		Capacity:  stats.Capacity,
		Available: stats.Available,
	})
	if err != nil {
		s.recordError("StoreHeartbeat", err)
		return nil
	}
	s.markHealthy()
	return fromPBOperations(resp.GetOperations())
}

// Status returns the current control-plane health view for this scheduler client.
func (s *SchedulerClient) Status() storepkg.SchedulerStatus {
	if s == nil {
		return storepkg.SchedulerStatus{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func fromPBOperations(ops []*pdpb.SchedulerOperation) []storepkg.Operation {
	if len(ops) == 0 {
		return nil
	}
	converted := make([]storepkg.Operation, 0, len(ops))
	for _, op := range ops {
		if next, ok := fromPBOperation(op); ok {
			converted = append(converted, next)
		}
	}
	if len(converted) == 0 {
		return nil
	}
	return converted
}

func fromPBOperation(op *pdpb.SchedulerOperation) (storepkg.Operation, bool) {
	if op == nil {
		return storepkg.Operation{}, false
	}
	switch op.GetType() {
	case pdpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER:
		if op.GetRegionId() == 0 || op.GetSourcePeerId() == 0 || op.GetTargetPeerId() == 0 {
			return storepkg.Operation{}, false
		}
		return storepkg.Operation{
			Type:   storepkg.OperationLeaderTransfer,
			Region: op.GetRegionId(),
			Source: op.GetSourcePeerId(),
			Target: op.GetTargetPeerId(),
		}, true
	default:
		return storepkg.Operation{}, false
	}
}

// Close closes the PD client if present.
func (s *SchedulerClient) Close() error {
	if s == nil || s.pd == nil {
		return nil
	}
	return s.pd.Close()
}

func (s *SchedulerClient) recordError(op string, err error) {
	if s == nil {
		return
	}
	msg := op + ": " + err.Error()
	now := time.Now()
	s.mu.Lock()
	s.status.Mode = storepkg.SchedulerModeUnavailable
	s.status.Degraded = true
	s.status.LastError = msg
	s.status.LastErrorAt = now
	s.mu.Unlock()
	s.onError(op, err)
}

func (s *SchedulerClient) markHealthy() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.status.Mode = storepkg.SchedulerModeHealthy
	s.status.Degraded = false
	s.mu.Unlock()
}

func contextWithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout > 0 {
		return context.WithTimeout(parent, timeout)
	}
	return context.WithCancel(parent)
}
