package kv

import (
	"bytes"
	"context"
	"sync/atomic"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultApplyWatchBuffer = 256

type applyWatchObserver struct {
	prefix  []byte
	ch      chan *kvrpcpb.ApplyWatchEvent
	dropped atomic.Uint64
}

func newApplyWatchObserver(req *kvrpcpb.ApplyWatchRequest) *applyWatchObserver {
	buffer := int(req.GetBuffer())
	if buffer <= 0 {
		buffer = defaultApplyWatchBuffer
	}
	return &applyWatchObserver{
		prefix: append([]byte(nil), req.GetKeyPrefix()...),
		ch:     make(chan *kvrpcpb.ApplyWatchEvent, buffer),
	}
}

func (o *applyWatchObserver) OnApply(evt storepkg.ApplyEvent) {
	if o == nil {
		return
	}
	keys := matchingApplyWatchKeys(evt.Keys, o.prefix)
	if len(keys) == 0 {
		return
	}
	out := &kvrpcpb.ApplyWatchEvent{
		RegionId:      evt.RegionID,
		Term:          evt.Term,
		Index:         evt.Index,
		Source:        applyWatchSourceToProto(evt.Source),
		CommitVersion: evt.CommitVersion,
		Keys:          keys,
	}
	if out.GetSource() == kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_UNSPECIFIED {
		return
	}
	select {
	case o.ch <- out:
	default:
		o.dropped.Add(1)
	}
}

func (s *Service) KvWatchApply(req *kvrpcpb.ApplyWatchRequest, stream kvrpcpb.NoKV_KvWatchApplyServer) error {
	if s == nil || s.store == nil {
		return status.Error(codes.FailedPrecondition, "raftstore kv service is not initialized")
	}
	if req == nil {
		return status.Error(codes.InvalidArgument, "apply watch request is required")
	}
	observer := newApplyWatchObserver(req)
	reg, err := s.store.RegisterApplyObserver(observer, int(req.GetBuffer()))
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	defer reg.Close()

	for {
		select {
		case <-stream.Context().Done():
			if err := stream.Context().Err(); err != nil && err != context.Canceled {
				return rpcStatus(err)
			}
			return nil
		case evt := <-observer.ch:
			if evt == nil {
				continue
			}
			if err := stream.Send(&kvrpcpb.ApplyWatchResponse{
				Event:         evt,
				DroppedEvents: observer.dropped.Load(),
			}); err != nil {
				return rpcStatus(err)
			}
		}
	}
}

func matchingApplyWatchKeys(keys [][]byte, prefix []byte) [][]byte {
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(prefix) > 0 && !bytes.HasPrefix(key, prefix) {
			continue
		}
		out = append(out, append([]byte(nil), key...))
	}
	return out
}

func applyWatchSourceToProto(source storepkg.ApplyEventSource) kvrpcpb.ApplyWatchEventSource {
	switch source {
	case storepkg.ApplyEventSourceCommit:
		return kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_COMMIT
	case storepkg.ApplyEventSourceResolveLock:
		return kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_RESOLVE_LOCK
	default:
		return kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_UNSPECIFIED
	}
}
