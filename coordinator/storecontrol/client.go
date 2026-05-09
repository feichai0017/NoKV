package storecontrol

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

const defaultRPCTimeout = 2 * time.Second

// Config defines how a coordinator-backed store-control client behaves.
type Config struct {
	Coordinator coordclient.Client
	Timeout     time.Duration
	OnError     func(op string, err error)
}

// CoordinatorClient forwards store-facing control traffic to the coordinator and returns
// the operations the coordinator wants the store to apply.
type CoordinatorClient struct {
	coordinator coordclient.Client
	timeout     time.Duration
	onError     func(op string, err error)
	mu          sync.RWMutex
	status      Status
}

var _ Client = (*CoordinatorClient)(nil)

// NewClient constructs a coordinator-backed store-control client.
func NewClient(cfg Config) *CoordinatorClient {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}
	onErr := cfg.OnError
	if onErr == nil {
		onErr = func(op string, err error) {
			slog.Default().Warn("coordinator store-control operation failed", "op", op, "err", err)
		}
	}
	return &CoordinatorClient{
		coordinator: cfg.Coordinator,
		timeout:     timeout,
		onError:     onErr,
		status: Status{
			Mode: ModeHealthy,
		},
	}
}

// ReportRegionHeartbeat forwards one runtime region-liveness heartbeat to coordinator.
func (s *CoordinatorClient) ReportRegionHeartbeat(ctx context.Context, regionID uint64) {
	if s == nil || regionID == 0 || s.coordinator == nil {
		return
	}
	ctx, cancel := contextWithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.coordinator.RegionLiveness(ctx, &coordpb.RegionLivenessRequest{
		RegionId: regionID,
	})
	if err != nil {
		s.recordError("RegionLiveness", err)
		return
	}
	s.markHealthy()
}

// PublishRootEvent publishes one explicit rooted truth event to coordinator.
func (s *CoordinatorClient) PublishRootEvent(ctx context.Context, event rootevent.Event) error {
	if s == nil || event.Kind == rootevent.KindUnknown || s.coordinator == nil {
		return nil
	}
	expected, normalized, err := prepareRootEventRequest(event)
	if err != nil {
		s.recordError("PublishRootEvent", err)
		return err
	}
	ctx, cancel := contextWithTimeout(ctx, s.timeout)
	defer cancel()
	_, err = s.coordinator.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event:                metawire.RootEventToProto(normalized),
		ExpectedClusterEpoch: expected,
	})
	if err != nil {
		s.recordError("PublishRootEvent", err)
		return err
	}
	s.markHealthy()
	return nil
}

// StoreHeartbeat publishes store stats to the coordinator and returns any operations the coordinator
// wants the store to apply.
func (s *CoordinatorClient) StoreHeartbeat(ctx context.Context, stats StoreStats) []Operation {
	if s == nil || stats.StoreID == 0 || s.coordinator == nil {
		return nil
	}
	ctx, cancel := contextWithTimeout(ctx, s.timeout)
	defer cancel()
	resp, err := s.coordinator.StoreHeartbeat(ctx, &coordpb.StoreHeartbeatRequest{
		StoreId:           stats.StoreID,
		ClientAddr:        stats.ClientAddr,
		RaftAddr:          stats.RaftAddr,
		RegionNum:         stats.RegionNum,
		LeaderNum:         stats.LeaderNum,
		Capacity:          stats.Capacity,
		Available:         stats.Available,
		DroppedOperations: stats.DroppedOperations,
		LeaderRegionIds:   stats.LeaderRegionIDs,
		RegionStats:       regionStatsToPB(stats.RegionStats),
	})
	if err != nil {
		s.recordError("StoreHeartbeat", err)
		return nil
	}
	s.markHealthy()
	return fromPBOperations(resp.GetOperations())
}

// Status returns the current control-plane health view for this client.
func (s *CoordinatorClient) Status() Status {
	if s == nil {
		return Status{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func fromPBOperations(ops []*coordpb.SchedulerOperation) []Operation {
	if len(ops) == 0 {
		return nil
	}
	converted := make([]Operation, 0, len(ops))
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

func fromPBOperation(op *coordpb.SchedulerOperation) (Operation, bool) {
	if op == nil {
		return Operation{}, false
	}
	switch op.GetType() {
	case coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER:
		if op.GetRegionId() == 0 || op.GetSourcePeerId() == 0 || op.GetTargetPeerId() == 0 {
			return Operation{}, false
		}
		return Operation{
			Type:   OperationLeaderTransfer,
			Region: op.GetRegionId(),
			Source: op.GetSourcePeerId(),
			Target: op.GetTargetPeerId(),
		}, true
	case coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_SPLIT_REGION:
		child := metawire.DescriptorFromProto(op.GetSplitChild())
		if op.GetRegionId() == 0 || len(op.GetSplitKey()) == 0 || child.RegionID == 0 {
			return Operation{}, false
		}
		return Operation{
			Type:       OperationSplitRegion,
			Region:     op.GetRegionId(),
			SplitKey:   append([]byte(nil), op.GetSplitKey()...),
			SplitChild: child,
		}, true
	case coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_MERGE_REGION:
		if op.GetRegionId() == 0 || op.GetSourceRegionId() == 0 {
			return Operation{}, false
		}
		return Operation{
			Type:         OperationMergeRegion,
			Region:       op.GetRegionId(),
			SourceRegion: op.GetSourceRegionId(),
		}, true
	default:
		return Operation{}, false
	}
}

func regionStatsToPB(in []RegionStats) []*coordpb.RegionRuntimeStats {
	if len(in) == 0 {
		return nil
	}
	out := make([]*coordpb.RegionRuntimeStats, 0, len(in))
	for _, stat := range in {
		if stat.RegionID == 0 {
			continue
		}
		out = append(out, &coordpb.RegionRuntimeStats{
			RegionId:          stat.RegionID,
			ReadQps:           stat.ReadQPS,
			WriteQps:          stat.WriteQPS,
			WriteBytesPerSec:  stat.WriteBytesPerSecond,
			ApproxRegionBytes: stat.ApproxRegionBytes,
			AtomicMutateQps:   stat.AtomicMutateQPS,
			LeaderStoreId:     stat.LeaderStoreID,
			PendingAdmin:      stat.PendingAdmin,
		})
	}
	return out
}

// Close closes the coordinator client if present.
func (s *CoordinatorClient) Close() error {
	if s == nil || s.coordinator == nil {
		return nil
	}
	return s.coordinator.Close()
}

func (s *CoordinatorClient) recordError(op string, err error) {
	if s == nil {
		return
	}
	msg := op + ": " + err.Error()
	now := time.Now()
	s.mu.Lock()
	s.status.Mode = ModeUnavailable
	s.status.Degraded = true
	s.status.LastError = msg
	s.status.LastErrorAt = now
	s.mu.Unlock()
	s.onError(op, err)
}

func (s *CoordinatorClient) markHealthy() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.status.Mode = ModeHealthy
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

func prepareRootEventRequest(event rootevent.Event) (uint64, rootevent.Event, error) {
	out := rootevent.CloneEvent(event)
	var expected uint64
	collect := func(epoch uint64) error {
		if epoch == 0 {
			return nil
		}
		if expected == 0 {
			expected = epoch
			return nil
		}
		if expected != epoch {
			return fmt.Errorf("coordinator store-control: conflicting root epochs in one root event (%d vs %d)", expected, epoch)
		}
		return nil
	}
	zero := func(desc topology.Descriptor) topology.Descriptor {
		desc = desc.Clone()
		desc.RootEpoch = 0
		return desc
	}
	switch {
	case out.RegionDescriptor != nil:
		if err := collect(out.RegionDescriptor.Descriptor.RootEpoch); err != nil {
			return 0, rootevent.Event{}, err
		}
		out.RegionDescriptor.Descriptor = zero(out.RegionDescriptor.Descriptor)
	case out.RangeSplit != nil:
		if err := collect(out.RangeSplit.Left.RootEpoch); err != nil {
			return 0, rootevent.Event{}, err
		}
		if err := collect(out.RangeSplit.Right.RootEpoch); err != nil {
			return 0, rootevent.Event{}, err
		}
		out.RangeSplit.Left = zero(out.RangeSplit.Left)
		out.RangeSplit.Right = zero(out.RangeSplit.Right)
	case out.RangeMerge != nil:
		if err := collect(out.RangeMerge.Merged.RootEpoch); err != nil {
			return 0, rootevent.Event{}, err
		}
		out.RangeMerge.Merged = zero(out.RangeMerge.Merged)
	case out.PeerChange != nil:
		if err := collect(out.PeerChange.Region.RootEpoch); err != nil {
			return 0, rootevent.Event{}, err
		}
		out.PeerChange.Region = zero(out.PeerChange.Region)
	}
	return expected, out, nil
}
