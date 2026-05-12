package raftstore

import (
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

type perasWatchPublisher interface {
	Publish(fsmeta.WatchEvent)
}

func (c *RemotePerasCommitter) publishVisibleWatch(delta compile.SemanticDelta, ack fsperas.VisibleAck) {
	if c == nil || c.watch == nil {
		return
	}
	keys := perasDentryWatchKeys(delta)
	if len(keys) == 0 {
		return
	}
	cursor := fsmeta.WatchCursor{
		Term:  ack.EpochID,
		Index: c.visibleSeq.Add(1),
	}
	for _, key := range keys {
		evt := fsmeta.WatchEvent{
			Cursor: cursor,
			Source: fsmeta.WatchEventSourcePerasVisible,
			Key:    key,
		}
		if c.watchQueue != nil && c.watchQueue.TryPush(evt) {
			continue
		}
		c.watch.Publish(evt)
	}
}

func (c *RemotePerasCommitter) visibleWatchLoop() {
	if c == nil || c.closer == nil {
		return
	}
	defer c.closer.Done()
	if c.watch == nil || c.watchQueue == nil {
		return
	}
	consumer := c.watchQueue.AcquireConsumer()
	if consumer == nil {
		return
	}
	defer consumer.Close()
	for {
		evt, ok := consumer.Pop()
		if !ok {
			return
		}
		c.watch.Publish(evt)
	}
}

func perasDentryWatchKeys(delta compile.SemanticDelta) [][]byte {
	if len(delta.WriteEffects) == 0 {
		return nil
	}
	keys := make([][]byte, 0, len(delta.WriteEffects))
	seen := make(map[string]struct{}, len(delta.WriteEffects))
	for _, effect := range delta.WriteEffects {
		parts, ok := fsmeta.InspectKey(effect.Key)
		if !ok || parts.Kind != fsmeta.KeyKindDentry {
			continue
		}
		key := string(effect.Key)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, runtimeCloneBytes(effect.Key))
	}
	return keys
}
