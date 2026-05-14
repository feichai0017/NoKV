// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"sync"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

type readState struct {
	mu        sync.RWMutex
	overlay   *fsperas.OverlayView
	sealed    *fsperas.OverlayView
	segments  []fsperas.PerasSegment
	completed map[fsperas.OperationID]perasCompletion
}

func newReadState() *readState {
	return &readState{
		overlay:   fsperas.NewOverlayView(),
		sealed:    fsperas.NewOverlayView(),
		completed: make(map[fsperas.OperationID]perasCompletion),
	}
}

func (c *Runtime) installSegment(plan fsperas.ReplayPlan, segment fsperas.PerasSegment) error {
	if c == nil || c.read == nil {
		return ErrRuntimeInvalid
	}
	stats := segment.Stats()
	if err := c.read.sealed.AddSegment(segment); err != nil {
		return c.recordError(err)
	}
	c.read.overlay.RemovePlan(plan)
	c.read.mu.Lock()
	c.read.segments = append(c.read.segments, segment)
	if c.read.completed == nil {
		c.read.completed = make(map[fsperas.OperationID]perasCompletion)
	}
	for _, completion := range segment.Completions {
		c.read.completed[completion.OpID] = perasCompletion{epochID: segment.EpochID, completion: completion}
	}
	c.read.mu.Unlock()

	c.metrics.segmentTotal.Add(1)
	c.metrics.segmentOpsTotal.Add(stats.OperationCount)
	c.metrics.segmentEntryTotal.Add(stats.EntryCount)
	c.metrics.statsMu.Lock()
	c.metrics.lastSegmentStats = stats
	c.metrics.lastSegmentRoot = segment.Root
	c.metrics.statsMu.Unlock()
	return nil
}

func (c *Runtime) Completion(id fsperas.OperationID) (fsperas.SegmentCompletion, bool) {
	completion, ok := c.completionForOperation(id)
	if !ok {
		return fsperas.SegmentCompletion{}, false
	}
	return completion.completion, true
}

func (c *Runtime) completionForOperation(id fsperas.OperationID) (perasCompletion, bool) {
	if c == nil || c.read == nil || !id.Valid() {
		return perasCompletion{}, false
	}
	c.read.mu.RLock()
	completion, ok := c.read.completed[id]
	c.read.mu.RUnlock()
	return completion, ok
}

func completionMatchesOperation(completion fsperas.SegmentCompletion, op compile.MaterializedOp) bool {
	if completion.Kind != op.Delta.Kind {
		return false
	}
	if completion.MutationCount != uint32(len(op.Effects)) {
		return false
	}
	if completion.DescriptorDigest != op.DescriptorDigest {
		return false
	}
	return completion.ExecutionPlanDigest == compile.ExecutionPlanDigest(op.Segment, op.Atomicity, op.Durability)
}

func (c *Runtime) segmentInstalled(root [32]byte) bool {
	if c == nil || c.read == nil {
		return false
	}
	c.read.mu.RLock()
	defer c.read.mu.RUnlock()
	for _, segment := range c.read.segments {
		if segment.Root == root {
			return true
		}
	}
	return false
}

func (c *Runtime) GetPerasOverlay(key []byte) ([]byte, bool, bool) {
	value, deleted, ok := c.GetPerasOverlayView(key)
	if !ok {
		return nil, false, false
	}
	return cloneBytes(value), deleted, true
}

func (c *Runtime) GetPerasOverlayView(key []byte) ([]byte, bool, bool) {
	if c == nil || c.read == nil {
		return nil, false, false
	}
	if value, deleted, ok := c.read.overlay.GetView(key); ok {
		return value, deleted, true
	}
	return c.read.sealed.GetView(key)
}

func (c *Runtime) KeyState(key []byte) (present bool, known bool) {
	if c == nil || c.read == nil {
		return false, false
	}
	if _, deleted, ok := c.read.overlay.Get(key); ok {
		return !deleted, true
	}
	if _, deleted, ok := c.read.sealed.Get(key); ok {
		return !deleted, true
	}
	return c.read.overlay.KeyState(key)
}

func (c *Runtime) DirectoryEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool {
	if c == nil || c.read == nil {
		return false
	}
	return c.read.overlay.DirectoryEmpty(mount, inode)
}

func (c *Runtime) SessionNamespaceEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool {
	if c == nil || c.read == nil {
		return false
	}
	return c.read.overlay.SessionNamespaceEmpty(mount, inode)
}

func (c *Runtime) RememberKey(key []byte, present bool) {
	if c == nil || c.read == nil {
		return
	}
	c.read.overlay.RememberKey(key, present)
}

func (c *Runtime) RememberEmptyDirectory(mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if c == nil || c.read == nil {
		return
	}
	c.read.overlay.RememberEmptyDirectory(mount, inode)
}

func (c *Runtime) RememberEmptySessionNamespace(mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if c == nil || c.read == nil {
		return
	}
	c.read.overlay.RememberEmptySessionNamespace(mount, inode)
}

func (c *Runtime) ScanPerasOverlay(start []byte, limit uint32) []fsperas.OverlayKV {
	if c == nil || c.read == nil || limit == 0 {
		return nil
	}
	return fsperas.MergeOverlayScans(c.read.sealed.Scan(start, limit), c.read.overlay.Scan(start, limit), limit)
}

func (c *Runtime) ScanPerasDirectory(prefix, start []byte, limit uint32) []fsperas.OverlayKV {
	if c == nil || c.read == nil || limit == 0 {
		return nil
	}
	return fsperas.MergeOverlayScans(c.read.sealed.ScanDirectory(prefix, start, limit), c.read.overlay.ScanDirectory(prefix, start, limit), limit)
}

func (c *Runtime) HasPerasDirectory(prefix []byte) bool {
	if c == nil || c.read == nil {
		return false
	}
	return c.read.overlay.HasDirectory(prefix) || c.read.sealed.HasDirectory(prefix)
}

func (c *Runtime) addOverlay(id fsperas.OperationID, op compile.MaterializedOp) error {
	if c == nil || c.read == nil {
		return ErrRuntimeInvalid
	}
	return c.read.overlay.Add(id, op)
}
