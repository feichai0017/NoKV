package vlog

// GC scheduling sits between the per-bucket Manager (which owns Sample /
// Iterate / Rewind) and the host's batch-writer (which pushes the
// rewritten entries back into the LSM through Deps.BatchSet). The
// scheduler is parallelism-bounded by gcTokens, throttled by LSM
// backlog/score thresholds, and serialized per-bucket by gcBucketBusy.

import (
	"fmt"
	"sort"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

// shouldSkipRewrite reports whether an entry can be safely dropped from
// a rewrite batch (deleted/expired, or no longer a value-pointer).
func shouldSkipRewrite(e *kv.Entry) bool {
	if e == nil {
		return true
	}
	if e.IsDeletedOrExpired() {
		return true
	}
	return !kv.IsValuePtr(e)
}

// IteratorRefAdd registers a live iterator that may be reading from
// segments queued for deletion; vlog defers FilterPendingDeletes-driven
// removal until all such iterators have called IteratorRefDel.
func (c *Consumer) IteratorRefAdd() {
	if c == nil {
		return
	}
	c.numActiveIterators.Add(1)
}

// IteratorRefDel undoes IteratorRefAdd.
func (c *Consumer) IteratorRefDel() {
	if c == nil {
		return
	}
	c.numActiveIterators.Add(-1)
}

// IteratorCount returns the number of registered live iterators.
func (c *Consumer) IteratorCount() int {
	if c == nil {
		return 0
	}
	return int(c.numActiveIterators.Load())
}

// RunGC schedules one GC cycle: pick candidate FIDs (limited by the
// effective parallelism), kick off concurrent doRunGC workers under the
// per-bucket-busy guard and gcTokens semaphore, return whichever error
// shape best matches the outcome (nil on at least one rewrite, ErrNoRewrite
// when nothing was eligible, ErrRejected when a previous cycle is still
// in flight).
func (c *Consumer) RunGC(discardRatio float64, heads map[uint32]kv.ValuePtr) error {
	select {
	case c.garbageCh <- struct{}{}:
		defer func() {
			<-c.garbageCh
		}()

		limit, throttled, skipped := c.effectiveGCParallelism()
		if skipped {
			metrics.DefaultValueLogGCCollector().IncSkipped()
			return utils.ErrNoRewrite
		}
		if throttled {
			metrics.DefaultValueLogGCCollector().IncThrottled()
		}
		if limit <= 0 {
			metrics.DefaultValueLogGCCollector().IncSkipped()
			return utils.ErrNoRewrite
		}

		files := c.pickLogs(heads, limit)
		if len(files) == 0 {
			return utils.ErrNoRewrite
		}
		results := make(chan error, len(files))
		scheduled := 0

		for _, id := range files {
			if !c.tryStartBucketGC(id.Bucket) {
				continue
			}
			if !c.tryAcquireGCToken() {
				c.finishBucketGC(id.Bucket)
				continue
			}
			scheduled++
			metrics.DefaultValueLogGCCollector().IncScheduled()
			metrics.DefaultValueLogGCCollector().IncActive()
			go func(job manifest.ValueLogID) {
				defer c.releaseGCToken()
				defer c.finishBucketGC(job.Bucket)
				defer metrics.DefaultValueLogGCCollector().DecActive()
				err := c.doRunGC(job.Bucket, job.FileID, discardRatio)
				results <- err
			}(id)
		}

		if scheduled == 0 {
			return utils.ErrNoRewrite
		}

		success := false
		var firstErr error
		for i := 0; i < scheduled; i++ {
			err := <-results
			if err == nil {
				success = true
				continue
			}
			if err != utils.ErrNoRewrite && firstErr == nil {
				firstErr = err
			}
		}
		if success {
			return nil
		}
		if firstErr != nil {
			return firstErr
		}
		return utils.ErrNoRewrite
	default:
		metrics.DefaultValueLogGCCollector().IncRejected()
		return utils.ErrRejected
	}
}

// effectiveGCParallelism reduces or skips GC based on LSM compaction
// backlog and max-score, both of which signal that the LSM is already
// behind and a rewrite would deepen the hole.
func (c *Consumer) effectiveGCParallelism() (effective int, throttled bool, skipped bool) {
	base := c.gcParallelism
	if base <= 0 {
		return 0, false, true
	}
	if c.deps.LSM == nil {
		return base, false, false
	}

	reduceScore := c.cfg.GCReduceScore
	if reduceScore <= 0 {
		reduceScore = 2.0
	}
	skipScore := c.cfg.GCSkipScore
	if skipScore <= 0 {
		skipScore = 4.0
	}
	reduceBacklog := c.cfg.GCReduceBacklog
	if reduceBacklog <= 0 {
		reduceBacklog = max(c.cfg.NumCompactors, 2)
	}
	skipBacklog := c.cfg.GCSkipBacklog
	if skipBacklog <= 0 {
		skipBacklog = max(reduceBacklog*2, 4)
	}

	diag := c.deps.LSM.Diagnostics()
	backlog, maxScore := diag.Compaction.Backlog, diag.Compaction.MaxScore
	if (skipBacklog > 0 && backlog >= int64(skipBacklog)) || (skipScore > 0 && maxScore >= skipScore) {
		return 0, true, true
	}
	if (reduceBacklog > 0 && backlog >= int64(reduceBacklog)) || (reduceScore > 0 && maxScore >= reduceScore) {
		effective = max(base/2, 1)
		if effective <= 0 {
			effective = 1
		}
		return effective, true, false
	}
	return base, false, false
}

func (c *Consumer) tryStartBucketGC(bucket uint32) bool {
	if int(bucket) >= len(c.gcBucketBusy) {
		return false
	}
	return c.gcBucketBusy[bucket].CompareAndSwap(0, 1)
}

func (c *Consumer) finishBucketGC(bucket uint32) {
	if int(bucket) >= len(c.gcBucketBusy) {
		return
	}
	c.gcBucketBusy[bucket].Store(0)
}

func (c *Consumer) tryAcquireGCToken() bool {
	if c.gcTokens == nil {
		return true
	}
	select {
	case c.gcTokens <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *Consumer) releaseGCToken() {
	if c.gcTokens == nil {
		return
	}
	select {
	case <-c.gcTokens:
	default:
	}
}

// doRunGC samples a candidate segment, decides whether to rewrite it
// based on the discard ratio, and rewrites it through the host's
// batch-writer if the heuristic clears.
func (c *Consumer) doRunGC(bucket uint32, fid uint32, discardRatio float64) (err error) {
	defer func() {
		if err == nil {
			c.lfDiscardStats.Lock()
			delete(c.lfDiscardStats.m, manifest.ValueLogID{Bucket: bucket, FileID: fid})
			c.lfDiscardStats.Unlock()
		}
	}()

	mgr, err := c.ManagerFor(bucket)
	if err != nil {
		return err
	}
	opts := SampleOptions{
		SizeRatio:     c.GCSampleSizeRatio(),
		CountRatio:    c.GCSampleCountRatio(),
		FromBeginning: c.cfg.GCSampleFromHead,
		MaxEntries:    c.cfg.GCMaxEntries,
	}
	start := time.Now()
	stats, err := mgr.Sample(fid, opts, func(e *kv.Entry, vp *kv.ValuePtr) (bool, error) {
		if time.Since(start) > 10*time.Second {
			return false, utils.ErrStop
		}
		if e == nil || len(e.Key) == 0 {
			return false, nil
		}
		cf, userKey, version, ok := kv.SplitInternalKey(e.Key)
		if !ok {
			return false, fmt.Errorf("value log GC sample expects internal key: %x", e.Key)
		}
		if len(userKey) == 0 {
			return false, nil
		}
		entry, err := c.deps.GetInternalEntry(cf, userKey, version)
		if err != nil {
			if errors.Is(err, utils.ErrEmptyKey) {
				return false, nil
			}
			if errors.Is(err, utils.ErrKeyNotFound) {
				return true, nil
			}
			return false, err
		}
		defer entry.DecrRef()
		if shouldSkipRewrite(entry) {
			return true, nil
		}

		if len(entry.Value) == 0 {
			return false, nil
		}
		var newVP kv.ValuePtr
		newVP.Decode(entry.Value)

		if newVP.Bucket != bucket {
			return true, nil
		}
		if newVP.Fid > fid || (newVP.Fid == fid && newVP.Offset > e.Offset) {
			return true, nil
		}
		return false, nil
	})
	if err != nil && err != utils.ErrStop {
		// Skip this round if writes are blocked/DB is closing; GC can retry later.
		if errors.Is(err, utils.ErrBlockedWrites) || errors.Is(err, utils.ErrDBClosed) {
			return utils.ErrNoRewrite
		}
		return err
	}
	if stats == nil {
		return utils.ErrNoRewrite
	}
	if stats.Count == 0 || stats.TotalMiB == 0 {
		return utils.ErrNoRewrite
	}

	c.Logf("Fid: %d bucket: %d. Skipped: %5.2fMB Data status={total:%5.2f discard:%5.2f count:%d}", fid, bucket, stats.SkippedMiB, stats.TotalMiB, stats.DiscardMiB, stats.Count)

	sizeWindow := stats.SizeWindow
	if sizeWindow == 0 {
		sizeWindow = float64(c.cfg.FileSize) / float64(utils.Mi)
	}
	if (stats.Count < stats.CountWindow && stats.TotalMiB < sizeWindow*0.75) || stats.DiscardMiB < discardRatio*stats.TotalMiB {
		return utils.ErrNoRewrite
	}

	if err = c.rewrite(bucket, fid); err != nil {
		return err
	}
	metrics.DefaultValueLogGCCollector().IncRuns()
	return nil
}

// rewrite copies live entries from segment fid back into the LSM through
// the host's BatchSet hook, then queues the segment for deletion (or
// removes it immediately when no iterators are active).
func (c *Consumer) rewrite(bucket uint32, fid uint32) error {
	mgr, err := c.ManagerFor(bucket)
	if err != nil {
		return err
	}
	activeFID := mgr.ActiveFID()
	utils.CondPanicFunc(fid >= activeFID, func() error {
		return fmt.Errorf("fid to move: %d. Current active fid: %d (bucket %d)", fid, activeFID, bucket)
	})

	wb := make([]*kv.Entry, 0, 1000)
	var size int64

	process := func(e *kv.Entry, ptr *kv.ValuePtr) error {
		if e == nil || len(e.Key) == 0 {
			return nil
		}
		entry, err := c.deps.LSM.Get(e.Key)
		if err != nil {
			if errors.Is(err, utils.ErrEmptyKey) {
				return nil
			}
			if errors.Is(err, utils.ErrKeyNotFound) {
				// Compromise policy: treat missing index entries as obsolete and skip
				// this record, so rewrite can still make progress under churn.
				return nil
			}
			return err
		}
		if entry == nil {
			// Fail closed on ambiguous storage results.
			return utils.ErrNoRewrite
		}
		defer entry.DecrRef()
		if shouldSkipRewrite(entry) {
			return nil
		}

		if len(entry.Value) == 0 {
			return errors.Errorf("empty value: %+v", entry)
		}

		var diskVP kv.ValuePtr
		if !kv.IsValuePtr(entry) {
			return nil
		}
		diskVP.Decode(entry.Value)

		if diskVP.Bucket != bucket {
			return nil
		}
		if diskVP.Fid > fid || (diskVP.Fid == fid && diskVP.Offset > ptr.Offset) {
			return nil
		}

		ne := kv.NewEntry(append([]byte(nil), e.Key...), append([]byte(nil), e.Value...))
		ne.Meta = 0
		ne.ExpiresAt = e.ExpiresAt

		es := int64(ne.EstimateSize(c.cfg.FileSize))
		es += int64(len(e.Value))
		limitCount, limitSize := c.cfg.MaxBatchCount, c.cfg.MaxBatchSize
		if int64(len(wb)+1) >= limitCount || size+es >= limitSize {
			if err := c.deps.BatchSet(wb); err != nil {
				return err
			}
			size = 0
			wb = wb[:0]
		}
		wb = append(wb, ne)
		size += es
		return nil
	}

	if _, err := mgr.Iterate(fid, 0, func(e *kv.Entry, vp *kv.ValuePtr) error {
		return process(e, vp)
	}); err != nil && err != utils.ErrStop {
		if errors.Is(err, utils.ErrNoRewrite) {
			return utils.ErrNoRewrite
		}
		return err
	}
	batchSize := 1024
	for i := 0; i < len(wb); {
		end := min(i+batchSize, len(wb))
		if err := c.deps.BatchSet(wb[i:end]); err != nil {
			if err == utils.ErrTxnTooBig {
				if batchSize <= 1 {
					return utils.ErrNoRewrite
				}
				batchSize = batchSize / 2
				continue
			}
			return err
		}
		i += batchSize
	}
	if len(wb) > 0 {
		testKey := wb[len(wb)-1].Key
		if vs, err := c.deps.LSM.Get(testKey); err == nil {
			if vs == nil {
				return utils.ErrKeyNotFound
			}
			defer vs.DecrRef()
			var vp kv.ValuePtr
			vp.Decode(vs.Value)
		} else {
			return err
		}
	}

	deleteNow := false
	c.filesToDeleteLock.Lock()
	if c.IteratorCount() == 0 {
		deleteNow = true
	} else {
		c.filesToBeDeleted = append(c.filesToBeDeleted, manifest.ValueLogID{Bucket: bucket, FileID: fid})
	}
	c.filesToDeleteLock.Unlock()

	if deleteNow {
		if err := c.removeValueLogFile(bucket, fid); err != nil {
			return err
		}
	}
	return nil
}

// FilterPendingDeletes returns a copy of fids with any entries currently
// queued for deletion removed. Used by the GC scheduler to avoid picking
// candidates that are about to disappear.
func (c *Consumer) FilterPendingDeletes(fids []manifest.ValueLogID) []manifest.ValueLogID {
	c.filesToDeleteLock.Lock()
	defer c.filesToDeleteLock.Unlock()

	if len(c.filesToBeDeleted) == 0 {
		out := make([]manifest.ValueLogID, len(fids))
		copy(out, fids)
		return out
	}

	toDelete := make(map[manifest.ValueLogID]struct{}, len(c.filesToBeDeleted))
	for _, id := range c.filesToBeDeleted {
		toDelete[id] = struct{}{}
	}

	out := make([]manifest.ValueLogID, 0, len(fids))
	for _, id := range fids {
		if _, ok := toDelete[id]; ok {
			continue
		}
		out = append(out, id)
	}
	return out
}

func (c *Consumer) pickLogs(heads map[uint32]kv.ValuePtr, limit int) (files []manifest.ValueLogID) {
	if len(c.managers) == 0 || limit <= 0 {
		return nil
	}
	if limit > len(c.managers) {
		limit = len(c.managers)
	}

	existing := make([]map[uint32]struct{}, len(c.managers))
	for bucket, mgr := range c.managers {
		if mgr == nil {
			continue
		}
		fids := mgr.ListFIDs()
		if len(fids) == 0 {
			continue
		}
		set := make(map[uint32]struct{}, len(fids))
		for _, fid := range fids {
			set[fid] = struct{}{}
		}
		existing[bucket] = set
	}

	bestID := make([]manifest.ValueLogID, len(c.managers))
	bestDiscard := make([]int64, len(c.managers))
	bestSet := make([]bool, len(c.managers))

	c.lfDiscardStats.RLock()
	for id, discard := range c.lfDiscardStats.m {
		if int(id.Bucket) >= len(c.managers) {
			continue
		}
		if existing[id.Bucket] == nil {
			continue
		}
		if _, ok := existing[id.Bucket][id.FileID]; !ok {
			continue
		}
		mgr := c.managers[id.Bucket]
		if mgr == nil {
			continue
		}
		activeFID := mgr.ActiveFID()
		head := heads[id.Bucket]
		if id.FileID >= activeFID {
			continue
		}
		if head.Fid != 0 && id.FileID >= head.Fid {
			continue
		}
		if discard > bestDiscard[id.Bucket] {
			bestDiscard[id.Bucket] = discard
			bestID[id.Bucket] = id
			bestSet[id.Bucket] = true
		}
	}
	c.lfDiscardStats.RUnlock()

	files = make([]manifest.ValueLogID, 0, limit)
	selectedBuckets := make([]bool, len(c.managers))
	for bucket := range bestSet {
		if !bestSet[bucket] || bestDiscard[bucket] <= 0 {
			continue
		}
		files = append(files, bestID[bucket])
		selectedBuckets[bucket] = true
	}
	files = c.FilterPendingDeletes(files)
	if len(files) > 1 {
		sort.Slice(files, func(i, j int) bool {
			return bestDiscard[files[i].Bucket] > bestDiscard[files[j].Bucket]
		})
	}
	if len(files) > limit {
		files = files[:limit]
	}
	if len(files) >= limit {
		return files
	}

	candidates := make([]manifest.ValueLogID, 0)
	for bucket, mgr := range c.managers {
		if mgr == nil || selectedBuckets[bucket] {
			continue
		}
		head := heads[uint32(bucket)]
		activeFID := mgr.ActiveFID()
		for _, fid := range mgr.ListFIDs() {
			if fid >= activeFID {
				continue
			}
			if head.Fid != 0 && fid >= head.Fid {
				continue
			}
			candidates = append(candidates, manifest.ValueLogID{Bucket: uint32(bucket), FileID: fid})
		}
	}
	if len(candidates) == 0 {
		return files
	}
	candidates = c.FilterPendingDeletes(candidates)
	if len(candidates) == 0 {
		return files
	}
	start := max(int(c.gcPickSeed.Add(1)), 0)
	for i := 0; i < len(candidates); i++ {
		id := candidates[(start+i)%len(candidates)]
		if selectedBuckets[id.Bucket] {
			continue
		}
		files = append(files, id)
		selectedBuckets[id.Bucket] = true
		if len(files) >= limit {
			break
		}
	}
	return files
}

// GCSampleSizeRatio returns the size-ratio knob for Sample() with a 0.10 default.
func (c *Consumer) GCSampleSizeRatio() float64 {
	r := c.cfg.GCSampleSizeRatio
	if r <= 0 {
		return 0.10
	}
	return r
}

// GCSampleCountRatio returns the count-ratio knob for Sample() with a 0.01 default.
func (c *Consumer) GCSampleCountRatio() float64 {
	r := c.cfg.GCSampleCountRatio
	if r <= 0 {
		return 0.01
	}
	return r
}
