package kv

import (
	"bytes"
	"context"
	"sync/atomic"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

const defaultApplyWatchBuffer = 256
const defaultApplyWatchMaxKeysPerMessage = 512
const defaultApplyWatchMaxKeyBytesPerMessage = 512 * 1024

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
	source := applyWatchSourceToProto(evt.Source)
	if source == kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_UNSPECIFIED {
		return
	}
	// Atomic/proposal batching can make one raft apply event contain thousands
	// of keys. WatchApply is a streaming boundary, so split the event into
	// bounded messages with the same raft cursor instead of depending on a large
	// transport-wide gRPC message limit.
	for _, chunk := range chunkApplyWatchKeys(keys) {
		o.enqueue(&kvrpcpb.ApplyWatchEvent{
			RegionId:      evt.RegionID,
			Term:          evt.Term,
			Index:         evt.Index,
			Source:        source,
			CommitVersion: evt.CommitVersion,
			Keys:          chunk,
		})
	}
}

func (o *applyWatchObserver) enqueue(out *kvrpcpb.ApplyWatchEvent) {
	select {
	case o.ch <- out:
	default:
		o.dropped.Add(1)
	}
}

func chunkApplyWatchKeys(keys [][]byte) [][][]byte {
	if len(keys) == 0 {
		return nil
	}
	chunks := make([][][]byte, 0, (len(keys)+defaultApplyWatchMaxKeysPerMessage-1)/defaultApplyWatchMaxKeysPerMessage)
	current := make([][]byte, 0, min(len(keys), defaultApplyWatchMaxKeysPerMessage))
	currentBytes := 0
	flush := func() {
		if len(current) == 0 {
			return
		}
		chunks = append(chunks, current)
		current = make([][]byte, 0, defaultApplyWatchMaxKeysPerMessage)
		currentBytes = 0
	}
	for _, key := range keys {
		keyBytes := len(key)
		if len(current) > 0 &&
			(len(current) >= defaultApplyWatchMaxKeysPerMessage || currentBytes+keyBytes > defaultApplyWatchMaxKeyBytesPerMessage) {
			flush()
		}
		current = append(current, key)
		currentBytes += keyBytes
	}
	flush()
	return chunks
}

func (s *Service) WatchApply(req *kvrpcpb.ApplyWatchRequest, stream kvrpcpb.StoreKV_WatchApplyServer) error {
	if s == nil || s.store == nil {
		return rpcServiceNotInitialized()
	}
	if req == nil {
		return rpcInvalidArgument("apply watch request is required")
	}
	observer := newApplyWatchObserver(req)
	reg, err := s.store.RegisterApplyObserver(observer, int(req.GetBuffer()))
	if err != nil {
		return rpcProtocolPrecondition(err.Error())
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
