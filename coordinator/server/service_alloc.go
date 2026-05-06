package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/feichai0017/NoKV/coordinator/idalloc"
	nokverrors "github.com/feichai0017/NoKV/errors"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/meta/topology"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc/codes"
)

func (s *Service) normalizeRootEvent(event rootevent.Event) (rootevent.Event, error) {
	out := rootevent.CloneEvent(event)
	switch {
	case out.RegionDescriptor != nil:
		desc, err := s.normalizeDescriptorRootEpoch(out.RegionDescriptor.Descriptor)
		if err != nil {
			return rootevent.Event{}, err
		}
		out.RegionDescriptor.Descriptor = desc
	case out.RangeSplit != nil:
		left, err := s.normalizeDescriptorRootEpoch(out.RangeSplit.Left)
		if err != nil {
			return rootevent.Event{}, err
		}
		right, err := s.normalizeDescriptorRootEpoch(out.RangeSplit.Right)
		if err != nil {
			return rootevent.Event{}, err
		}
		out.RangeSplit.Left = left
		out.RangeSplit.Right = right
	case out.RangeMerge != nil:
		merged, err := s.normalizeDescriptorRootEpoch(out.RangeMerge.Merged)
		if err != nil {
			return rootevent.Event{}, err
		}
		out.RangeMerge.Merged = merged
	case out.PeerChange != nil:
		desc, err := s.normalizeDescriptorRootEpoch(out.PeerChange.Region)
		if err != nil {
			return rootevent.Event{}, err
		}
		out.PeerChange.Region = desc
	}
	return out, nil
}

func (s *Service) normalizeDescriptorRootEpoch(desc topology.Descriptor) (topology.Descriptor, error) {
	if desc.RootEpoch != 0 {
		return desc, nil
	}
	if s != nil && s.cluster != nil {
		current, ok := s.cluster.GetRegionDescriptor(desc.RegionID)
		if ok {
			probe := desc.Clone()
			probe.RootEpoch = current.RootEpoch
			if current.Equal(probe) {
				return probe, nil
			}
		}
	}
	nextEpoch, err := s.nextRootEpoch()
	if err != nil {
		return topology.Descriptor{}, err
	}
	desc.RootEpoch = nextEpoch
	return desc, nil
}

func (s *Service) nextRootEpoch() (uint64, error) {
	if s != nil && s.storage != nil {
		snapshot, err := s.storage.Load()
		if err != nil {
			return 0, err
		}
		if snapshot.ClusterEpoch < ^uint64(0) {
			return snapshot.ClusterEpoch + 1, nil
		}
		return snapshot.ClusterEpoch, nil
	}
	var maxEpoch uint64
	if s != nil && s.cluster != nil {
		maxEpoch = s.cluster.MaxDescriptorRevision()
	}
	if maxEpoch < ^uint64(0) {
		return maxEpoch + 1, nil
	}
	return maxEpoch, nil
}

func (s *Service) reserveIDs(ctx context.Context, count uint64) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	if count == 0 {
		return 0, fmt.Errorf("%w: reserve n must be >= 1", idalloc.ErrInvalidBatch)
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()

	current := s.ids.Current()
	next, ok := addUint64(current, count)
	if !ok {
		return 0, fmt.Errorf("%w: reserve would overflow", idalloc.ErrInvalidBatch)
	}
	if s.storage != nil && next > s.idWindowHigh {
		windowHigh, ok := addUint64(current, maxUint64(s.effectiveIDWindowSize(), count))
		if !ok {
			windowHigh = next
		}
		if err := s.storage.SaveAllocatorState(ctx, windowHigh, s.currentTSOFenceLocked()); err != nil {
			return 0, err
		}
		s.idWindowHigh = windowHigh
	}
	s.ids.Fence(next)
	return current + 1, nil
}

func (s *Service) reserveTSO(ctx context.Context, count uint64) (uint64, uint64, error) {
	if s == nil {
		return 0, 0, nil
	}
	if count == 0 {
		return 0, 0, fmt.Errorf("%w: tso reserve n must be >= 1", idalloc.ErrInvalidBatch)
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()

	current := s.tso.Current()
	next, ok := addUint64(current, count)
	if !ok {
		return 0, 0, fmt.Errorf("%w: tso reserve would overflow", idalloc.ErrInvalidBatch)
	}
	if s.storage != nil && next > s.tsoWindowHigh {
		windowHigh, ok := addUint64(current, maxUint64(s.effectiveTSOWindowSize(), count))
		if !ok {
			windowHigh = next
		}
		if err := s.storage.SaveAllocatorState(ctx, s.currentIDFenceLocked(), windowHigh); err != nil {
			return 0, 0, err
		}
		s.tsoWindowHigh = windowHigh
	}
	s.tso.Fence(next)
	return current + 1, count, nil
}

func (s *Service) effectiveIDWindowSize() uint64 {
	if s == nil || s.idWindowSize == 0 {
		return defaultAllocatorWindowSize
	}
	return s.idWindowSize
}

func (s *Service) effectiveTSOWindowSize() uint64 {
	if s == nil || s.tsoWindowSize == 0 {
		return defaultAllocatorWindowSize
	}
	return s.tsoWindowSize
}

func (s *Service) currentIDFenceLocked() uint64 {
	if s == nil {
		return 0
	}
	return maxUint64(s.ids.Current(), s.idWindowHigh)
}

func (s *Service) currentTSOFenceLocked() uint64 {
	if s == nil {
		return 0
	}
	return maxUint64(s.tso.Current(), s.tsoWindowHigh)
}

func (s *Service) installGrantAllocatorWindowsLocked(grant rootproto.AuthorityGrant) (bool, bool) {
	if s == nil || !grant.Present() {
		return false, false
	}
	s.grantMu.RLock()
	holderID := s.coordinatorID
	s.grantMu.RUnlock()
	if holderID == "" || grant.HolderID != holderID {
		return false, false
	}
	idUpper, idGranted := grantMonotoneUpper(grant, rootproto.DutyAllocID)
	if idGranted && idUpper > s.idWindowHigh {
		s.idWindowHigh = idUpper
	}
	tsoUpper, tsoGranted := grantMonotoneUpper(grant, rootproto.DutyTSO)
	if tsoGranted && tsoUpper > s.tsoWindowHigh {
		s.tsoWindowHigh = tsoUpper
	}
	return idGranted, tsoGranted
}

func grantMonotoneUpper(grant rootproto.AuthorityGrant, duty rootproto.DutyID) (uint64, bool) {
	dutyGrant, ok := grant.Duty(duty)
	if !ok || dutyGrant.Bound.Kind != rootproto.DutyBoundMonotone {
		return 0, false
	}
	return dutyGrant.Bound.MonotoneUpper, true
}

func (s *Service) fenceIDFromStorage(fence uint64) {
	if s == nil {
		return
	}
	if s.idWindowHigh != 0 && fence <= s.idWindowHigh {
		return
	}
	s.ids.Fence(fence)
	if fence > s.idWindowHigh {
		s.idWindowHigh = fence
	}
}

func (s *Service) fenceTSOFromStorage(fence uint64) {
	if s == nil {
		return
	}
	if s.tsoWindowHigh != 0 && fence <= s.tsoWindowHigh {
		return
	}
	s.tso.Fence(fence)
	if fence > s.tsoWindowHigh {
		s.tsoWindowHigh = fence
	}
}

func addUint64(a, b uint64) (uint64, bool) {
	if ^uint64(0)-a < b {
		return 0, false
	}
	return a + b, true
}

func allocationConsumedFrontier(first, count uint64) uint64 {
	if first == 0 || count == 0 {
		return 0
	}
	last, ok := addUint64(first, count-1)
	if !ok {
		return 0
	}
	return last
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// AllocID allocates one or more globally unique ids.
func (s *Service) AllocID(ctx context.Context, req *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusContext(err)
	}
	if req == nil {
		return nil, statusInvalidArgument("alloc id request is nil")
	}
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	if err := s.requireRootWriteAccess(); err != nil {
		return nil, err
	}
	admission, err := s.beginDutyAdmission(ctx, rootproto.DutyAllocID)
	if err != nil {
		return nil, err
	}
	defer admission.Done()
	first, err := s.reserveIDs(ctx, count)
	if err != nil {
		if errors.Is(err, idalloc.ErrInvalidBatch) {
			return nil, statusCatalog(nokverrors.KindInvalidArgument, codes.InvalidArgument, err, reasonInvalidRequest)
		}
		return nil, statusInternalf("persist allocator state: %v", err)
	}
	consumedFrontier := allocationConsumedFrontier(first, count)
	proof, err := admission.authorityEvidence(rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumedFrontier})
	if err != nil {
		return nil, err
	}
	return &coordpb.AllocIDResponse{
		FirstId:                 first,
		Count:                   count,
		Era:                     proof.Grant.Era,
		ConsumedFrontier:        consumedFrontier,
		ObservedRetiredEraFloor: proof.ObservedRetiredEraFloor,
		AuthorityEvidence:       proof.Evidence,
	}, nil
}

// Tso allocates one or more timestamps.
func (s *Service) Tso(ctx context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusContext(err)
	}
	if req == nil {
		return nil, statusInvalidArgument("tso request is nil")
	}
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	if err := s.requireRootWriteAccess(); err != nil {
		return nil, err
	}
	admission, err := s.beginDutyAdmission(ctx, rootproto.DutyTSO)
	if err != nil {
		return nil, err
	}
	defer admission.Done()
	first, got, err := s.reserveTSO(ctx, count)
	if err != nil {
		if errors.Is(err, idalloc.ErrInvalidBatch) {
			return nil, statusCatalog(nokverrors.KindInvalidArgument, codes.InvalidArgument, err, reasonInvalidRequest)
		}
		return nil, statusInternalf("persist allocator state: %v", err)
	}
	consumedFrontier := allocationConsumedFrontier(first, got)
	proof, err := admission.authorityEvidence(rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumedFrontier})
	if err != nil {
		return nil, err
	}
	return &coordpb.TsoResponse{
		Timestamp:               first,
		Count:                   got,
		Era:                     proof.Grant.Era,
		ConsumedFrontier:        consumedFrontier,
		ObservedRetiredEraFloor: proof.ObservedRetiredEraFloor,
		AuthorityEvidence:       proof.Evidence,
	}, nil
}

func (s *Service) metadataReplyEra(era uint64) uint64 {
	return era
}
