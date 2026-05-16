// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

type perasSnapshotView struct {
	sealedSegments int
	visible        *fsperas.OverlayView
}

type readState struct {
	mu             sync.RWMutex
	overlay        *fsperas.OverlayView
	sealed         *fsperas.OverlayView
	segments       []fsperas.PerasSegment
	sealedSegments []fsperas.PerasSegment
	completed      map[fsperas.OperationID]perasCompletion
	snapshots      map[uint64]perasSnapshotView
}

func newReadState() *readState {
	return &readState{
		overlay:   fsperas.NewOverlayView(),
		sealed:    fsperas.NewOverlayView(),
		completed: make(map[fsperas.OperationID]perasCompletion),
		snapshots: make(map[uint64]perasSnapshotView),
	}
}

func (c *Runtime) installSegment(plan fsperas.ReplayPlan, segment fsperas.PerasSegment, materialize bool) error {
	if c == nil || c.read == nil {
		return ErrRuntimeInvalid
	}
	stats := segment.Stats()
	c.read.mu.Lock()
	if !materialize {
		if err := c.read.sealed.AddSegment(segment); err != nil {
			c.read.mu.Unlock()
			return c.recordError(err)
		}
		c.read.sealedSegments = append(c.read.sealedSegments, segment)
	}
	c.read.overlay.RemovePlan(plan)
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
		c.read.snapshots = make(map[uint64]perasSnapshotView)
	}
	c.read.snapshots[version] = perasSnapshotView{sealedSegments: len(c.read.sealedSegments)}
	c.read.mu.Unlock()
	return nil
}

// CapturePerasVisibleSnapshot records a local visible snapshot when configured.
func (c *Runtime) CapturePerasVisibleSnapshot(version uint64, scope compile.AuthorityScope) (bool, error) {
	if c == nil || c.read == nil || version == 0 {
		return false, ErrRuntimeInvalid
	}
	if !c.visibleSnapshots {
		return false, nil
	}
	mount, prefix, ok := visibleSnapshotDirectory(scope)
	if !ok {
		return false, nil
	}
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	c.read.mu.Lock()
	if c.read.snapshots == nil {
		c.read.snapshots = make(map[uint64]perasSnapshotView)
	}
	c.read.snapshots[version] = perasSnapshotView{
		sealedSegments: len(c.read.sealedSegments),
		visible:        c.read.overlay.CloneForSnapshotDirectory(mount, prefix),
	}
	c.read.mu.Unlock()
	return true, nil
}

// GetPerasSnapshotOverlayView returns snapshot overlay-owned bytes.
func (c *Runtime) GetPerasSnapshotOverlayView(version uint64, key []byte) ([]byte, bool, bool) {
	snapshot, segments, ok := c.perasSnapshotView(version)
	if !ok {
		return nil, false, false
	}
	if snapshot.visible != nil {
		if value, deleted, found := snapshot.visible.GetView(key); found {
			return value, deleted, true
		}
	}
	for idx := len(segments) - 1; idx >= 0; idx-- {
		if value, deleted, found := segments[idx].GetView(key); found {
			return value, deleted, true
		}
	}
	return nil, false, false
}

// ScanPerasSnapshotDirectory scans a captured snapshot directory overlay.
func (c *Runtime) ScanPerasSnapshotDirectory(version uint64, prefix, start []byte, limit uint32) []fsperas.OverlayKV {
	snapshot, segments, ok := c.perasSnapshotView(version)
	if !ok {
		return nil
	}
	sealed := scanPerasSnapshotSegmentsDirectory(segments, prefix, start, limit)
	if snapshot.visible == nil {
		return sealed
	}
	visible := snapshot.visible.ScanDirectory(prefix, start, limit)
	return fsperas.MergeOverlayScans(sealed, visible, limit)
}

// HasPerasSnapshotDirectory reports whether a snapshot captured rows for a directory.
func (c *Runtime) HasPerasSnapshotDirectory(version uint64, prefix []byte) bool {
	snapshot, segments, ok := c.perasSnapshotView(version)
	if !ok || len(prefix) == 0 {
		return false
	}
	if snapshot.visible != nil && snapshot.visible.HasDirectory(prefix) {
		return true
	}
	for _, segment := range segments {
		for _, entry := range segment.EntriesView() {
			if bytes.HasPrefix(entry.Key, prefix) {
				return true
			}
		}
	}
	return false
}

func (c *Runtime) perasSnapshotView(version uint64) (perasSnapshotView, []fsperas.PerasSegment, bool) {
	if c == nil || c.read == nil || version == 0 {
		return perasSnapshotView{}, nil, false
	}
	c.read.mu.RLock()
	snapshot, ok := c.read.snapshots[version]
	if !ok {
		c.read.mu.RUnlock()
		return perasSnapshotView{}, nil, false
	}
	count := snapshot.sealedSegments
	if count > len(c.read.sealedSegments) {
		count = len(c.read.sealedSegments)
	}
	segments := append([]fsperas.PerasSegment(nil), c.read.sealedSegments[:count]...)
	c.read.mu.RUnlock()
	return snapshot, segments, true
}

func scanPerasSnapshotSegmentsDirectory(segments []fsperas.PerasSegment, prefix, start []byte, limit uint32) []fsperas.OverlayKV {
	if len(segments) == 0 || len(prefix) == 0 || limit == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	rows := make([]fsperas.OverlayKV, 0, int(limit))
	for segmentIdx := len(segments) - 1; segmentIdx >= 0; segmentIdx-- {
		for _, entry := range segments[segmentIdx].EntriesView() {
			if bytes.Compare(entry.Key, start) < 0 || !bytes.HasPrefix(entry.Key, prefix) {
				continue
			}
			key := string(entry.Key)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			rows = append(rows, fsperas.OverlayKV{
				Key:    cloneBytes(entry.Key),
				Value:  cloneBytes(entry.Value),
				Delete: entry.Delete,
			})
		}
	}
	if len(rows) == 0 {
		return nil
	}
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].Key, rows[j].Key) < 0
	})
	if len(rows) > int(limit) {
		rows = rows[:limit]
	}
	return rows
}

func visibleSnapshotDirectory(scope compile.AuthorityScope) (fsmeta.MountIdentity, []byte, bool) {
	if scope.Mount == "" || scope.MountKeyID == 0 || len(scope.Parents) != 1 || len(scope.Inodes) != 0 || scope.Broad {
		return fsmeta.MountIdentity{}, nil, false
	}
	mount := fsmeta.MountIdentity{MountID: scope.Mount, MountKeyID: scope.MountKeyID}
	prefix, err := fsmeta.EncodeDentryPrefix(mount, scope.Parents[0])
	if err != nil {
		return fsmeta.MountIdentity{}, nil, false
	}
	return mount, prefix, true
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

func (c *Runtime) DirectoryBaseEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool {
	if c == nil || c.read == nil {
		return false
	}
	return c.read.overlay.DirectoryBaseEmpty(mount, inode)
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
