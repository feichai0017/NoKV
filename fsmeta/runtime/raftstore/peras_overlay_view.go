package raftstore

import (
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

func (c *RemotePerasCommitter) installSegment(plan fsperas.ReplayPlan, segment fsperas.PerasSegment) error {
	stats := segment.Stats()
	if err := c.sealed.AddSegment(segment); err != nil {
		return c.recordError(err)
	}
	c.overlay.RemovePlan(plan)
	c.overlayMu.Lock()
	c.segments = append(c.segments, segment)
	if c.completed == nil {
		c.completed = make(map[fsperas.OperationID]perasCompletion)
	}
	for _, completion := range segment.Completions {
		c.completed[completion.OpID] = perasCompletion{epochID: segment.EpochID, completion: completion}
	}
	c.overlayMu.Unlock()

	c.segmentTotal.Add(1)
	c.segmentOpsTotal.Add(stats.OperationCount)
	c.segmentEntryTotal.Add(stats.EntryCount)
	c.statsMu.Lock()
	c.lastSegmentStats = stats
	c.lastSegmentRoot = segment.Root
	c.statsMu.Unlock()
	return nil
}

func (c *RemotePerasCommitter) Completion(id fsperas.OperationID) (fsperas.SegmentCompletion, bool) {
	completion, ok := c.completionForOperation(id)
	if !ok {
		return fsperas.SegmentCompletion{}, false
	}
	return completion.completion, true
}

func (c *RemotePerasCommitter) completionForOperation(id fsperas.OperationID) (perasCompletion, bool) {
	if c == nil || !id.Valid() {
		return perasCompletion{}, false
	}
	c.overlayMu.RLock()
	completion, ok := c.completed[id]
	c.overlayMu.RUnlock()
	return completion, ok
}

func (c *RemotePerasCommitter) segmentInstalled(root [32]byte) bool {
	c.overlayMu.RLock()
	defer c.overlayMu.RUnlock()
	for _, segment := range c.segments {
		if segment.Root == root {
			return true
		}
	}
	return false
}

func (c *RemotePerasCommitter) GetPerasOverlay(key []byte) ([]byte, bool, bool) {
	if c == nil {
		return nil, false, false
	}
	if value, deleted, ok := c.overlay.Get(key); ok {
		return value, deleted, true
	}
	return c.sealed.Get(key)
}

func (c *RemotePerasCommitter) KeyState(key []byte) (present bool, known bool) {
	if c == nil {
		return false, false
	}
	if _, deleted, ok := c.overlay.Get(key); ok {
		return !deleted, true
	}
	if _, deleted, ok := c.sealed.Get(key); ok {
		return !deleted, true
	}
	return c.overlay.KeyState(key)
}

func (c *RemotePerasCommitter) DirectoryEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool {
	if c == nil {
		return false
	}
	return c.overlay.DirectoryEmpty(mount, inode)
}

func (c *RemotePerasCommitter) SessionNamespaceEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool {
	if c == nil {
		return false
	}
	return c.overlay.SessionNamespaceEmpty(mount, inode)
}

func (c *RemotePerasCommitter) RememberKey(key []byte, present bool) {
	if c == nil {
		return
	}
	c.overlay.RememberKey(key, present)
}

func (c *RemotePerasCommitter) RememberEmptyDirectory(mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if c == nil {
		return
	}
	c.overlay.RememberEmptyDirectory(mount, inode)
}

func (c *RemotePerasCommitter) RememberEmptySessionNamespace(mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if c == nil {
		return
	}
	c.overlay.RememberEmptySessionNamespace(mount, inode)
}

func (c *RemotePerasCommitter) ScanPerasOverlay(start []byte, limit uint32) []fsperas.OverlayKV {
	if c == nil || limit == 0 {
		return nil
	}
	return fsperas.MergeOverlayScans(c.sealed.Scan(start, limit), c.overlay.Scan(start, limit), limit)
}

func (c *RemotePerasCommitter) addOverlay(id fsperas.OperationID, op compile.MaterializedOp) error {
	return c.overlay.Add(id, op)
}
