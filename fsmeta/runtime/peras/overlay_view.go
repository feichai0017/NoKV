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
	snapshots map[uint64]*fsperas.OverlayView
}

func newReadState() *readState {
	return &readState{
		overlay:   fsperas.NewOverlayView(),
		sealed:    fsperas.NewOverlayView(),
		completed: make(map[fsperas.OperationID]perasCompletion),
		snapshots: make(map[uint64]*fsperas.OverlayView),
	}
}

func (c *Runtime) installSegment(plan fsperas.ReplayPlan, segment fsperas.PerasSegment, materialize bool) error {
	if c == nil || c.read == nil {
		return ErrRuntimeInvalid
	}
	stats := segment.Stats()
	if !materialize {
		if err := c.read.sealed.AddSegment(segment); err != nil {
			return c.recordError(err)
		}
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
	if completion.PredicateProofDigest != compile.AdmissionProofSetDigest(op.PredicateProofs, op.GuardProofs) {
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

// CapturePerasSnapshot records the installed catalog overlay visible to one
// MVCC snapshot version.
func (c *Runtime) CapturePerasSnapshot(version uint64) error {
	if c == nil || c.read == nil || version == 0 {
		return ErrRuntimeInvalid
	}
	c.read.mu.Lock()
	if c.read.snapshots == nil {
		c.read.snapshots = make(map[uint64]*fsperas.OverlayView)
	}
	c.read.snapshots[version] = c.read.sealed.Clone()
	c.read.mu.Unlock()
	return nil
}

// GetPerasSnapshotOverlayView returns snapshot overlay-owned bytes.
func (c *Runtime) GetPerasSnapshotOverlayView(version uint64, key []byte) ([]byte, bool, bool) {
	snapshot := c.perasSnapshotOverlay(version)
	if snapshot == nil {
		return nil, false, false
	}
	return snapshot.GetView(key)
}

// ScanPerasSnapshotDirectory scans a captured snapshot directory overlay.
func (c *Runtime) ScanPerasSnapshotDirectory(version uint64, prefix, start []byte, limit uint32) []fsperas.OverlayKV {
	snapshot := c.perasSnapshotOverlay(version)
	if snapshot == nil {
		return nil
	}
	return snapshot.ScanDirectory(prefix, start, limit)
}

// HasPerasSnapshotDirectory reports whether a snapshot captured rows for a directory.
func (c *Runtime) HasPerasSnapshotDirectory(version uint64, prefix []byte) bool {
	snapshot := c.perasSnapshotOverlay(version)
	if snapshot == nil {
		return false
	}
	return snapshot.HasDirectory(prefix)
}

func (c *Runtime) perasSnapshotOverlay(version uint64) *fsperas.OverlayView {
	if c == nil || c.read == nil || version == 0 {
		return nil
	}
	c.read.mu.RLock()
	snapshot := c.read.snapshots[version]
	c.read.mu.RUnlock()
	return snapshot
}

func (c *Runtime) KeyState(key []byte) (present bool, known bool) {
	if c == nil || c.read == nil {
		return false, false
	}
	if _, deleted, ok := c.read.overlay.GetView(key); ok {
		return !deleted, true
	}
	if _, deleted, ok := c.read.sealed.GetView(key); ok {
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

func (c *Runtime) ForgetEmptyDirectory(mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if c == nil || c.read == nil {
		return
	}
	c.read.overlay.ForgetEmptyDirectory(mount, inode)
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

func (c *Runtime) HasPerasVisibleDirectory(prefix []byte) bool {
	if c == nil || c.read == nil {
		return false
	}
	return c.read.overlay.HasDirectory(prefix)
}

func (c *Runtime) PerasDirectoryCacheFrontier(prefix []byte) uint64 {
	if c == nil || c.read == nil {
		return 0
	}
	return c.read.sealed.DirectoryFrontier(prefix)
}

func (c *Runtime) addOverlay(id fsperas.OperationID, op compile.MaterializedOp) error {
	if c == nil || c.read == nil {
		return ErrRuntimeInvalid
	}
	return c.read.overlay.Add(id, op)
}
