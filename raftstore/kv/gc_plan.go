package kv

import (
	"bytes"
	"context"
	"fmt"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/engine/index"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/percolator"
)

const defaultMVCCGCApplyBatchEntries = 4096

var errMVCCGCStop = fmt.Errorf("kv: stop MVCC GC batch")

// MVCCGCPlanStats summarizes a non-destructive MVCC GC planning pass.
type MVCCGCPlanStats struct {
	Keys                  uint64
	WriteVersions         uint64
	RetainedWrites        uint64
	DroppableWrites       uint64
	AnchorWrites          uint64
	RetainedDefaultRefs   uint64
	DeletedWriteMarkers   uint64
	SafePointClampedKeys  uint64
	MaxVersionsPerKey     uint64
	MinEffectiveSafePoint uint64
	MaxEffectiveSafePoint uint64
}

// MVCCGCApplyStats summarizes a destructive MVCC GC pass. The embedded plan
// counters describe what the scanner decided; Applied* counters describe the
// tombstones actually submitted through ApplyInternalEntries.
type MVCCGCApplyStats struct {
	MVCCGCPlanStats
	AppliedWriteDeletes   uint64
	AppliedDefaultDeletes uint64
}

// MVCCGCApplyOptions configures a destructive MVCC GC pass.
type MVCCGCApplyOptions struct {
	// BatchEntries limits the number of tombstones submitted in one
	// ApplyInternalEntries call. A non-positive value uses the default.
	BatchEntries int
}

func (o MVCCGCApplyOptions) batchEntries() int {
	if o.BatchEntries <= 0 {
		return defaultMVCCGCApplyBatchEntries
	}
	return o.BatchEntries
}

// PlanMVCCGC scans CFWrite and applies MVCC GC policy without deleting data.
// It is the observability step before wiring the same policy into a destructive
// compaction path.
func PlanMVCCGC(ctx context.Context, db NoKV.MVCCStore, policy MVCCGCSafePointPolicy) (MVCCGCPlanStats, error) {
	var stats MVCCGCPlanStats
	_, err := walkMVCCGC(ctx, db, policy, nil, &stats, nil)
	return stats, err
}

// ApplyMVCCGC applies MVCC GC by writing point tombstones for droppable CFWrite
// records and their unreferenced CFDefault payloads. It scans and applies in
// bounded batches, and never writes to the DB while an iterator is open.
func ApplyMVCCGC(ctx context.Context, db NoKV.MVCCStore, policy MVCCGCSafePointPolicy, opt MVCCGCApplyOptions) (MVCCGCApplyStats, error) {
	var stats MVCCGCApplyStats
	var afterUserKey []byte
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		batch, err := collectMVCCGCApplyBatch(ctx, db, policy, afterUserKey, opt.batchEntries())
		if err != nil {
			return stats, err
		}
		stats.add(batch.plan)
		if len(batch.entries) > 0 {
			if err := db.ApplyInternalEntries(batch.entries); err != nil {
				releaseEntries(batch.entries)
				return stats, err
			}
			stats.AppliedWriteDeletes += batch.writeDeletes
			stats.AppliedDefaultDeletes += batch.defaultDeletes
		}
		releaseEntries(batch.entries)
		if batch.done {
			return stats, nil
		}
		afterUserKey = batch.lastUserKey
	}
}

type mvccGCGroupFn func(userKey []byte, decisions []percolator.GCWriteDecision) error

type mvccGCWalkResult struct {
	lastUserKey []byte
	stopped     bool
}

func walkMVCCGC(ctx context.Context, db NoKV.MVCCStore, policy MVCCGCSafePointPolicy, afterUserKey []byte, stats *MVCCGCPlanStats, fn mvccGCGroupFn) (mvccGCWalkResult, error) {
	var result mvccGCWalkResult
	if db == nil {
		return result, fmt.Errorf("kv: nil MVCC store")
	}
	iter := db.NewInternalIterator(&index.Options{IsAsc: true})
	if iter == nil {
		return result, nil
	}
	defer func() { _ = iter.Close() }()

	var (
		currentKey []byte
		versions   []percolator.GCWriteVersion
		decisions  []percolator.GCWriteDecision
	)
	flush := func() error {
		if len(currentKey) == 0 {
			return nil
		}
		result.lastUserKey = entrykv.SafeCopy(result.lastUserKey, currentKey)
		var safePoint uint64
		safePoint, decisions = policy.AppendPlanWritesForKey(decisions[:0], currentKey, versions)
		stats.recordGroup(policy.RequestedSafePoint, safePoint, decisions)
		if fn != nil {
			if err := fn(currentKey, decisions); err != nil {
				if err == errMVCCGCStop {
					result.stopped = true
					currentKey = nil
					versions = versions[:0]
					return nil
				}
				return err
			}
		}
		currentKey = nil
		versions = versions[:0]
		return nil
	}

	seekMVCCGCStart(iter, afterUserKey)
	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		cf, userKey, commitTs, ok := entrykv.SplitInternalKey(entry.Key)
		if !ok {
			return result, fmt.Errorf("kv: MVCC GC plan expects internal key, got %x", entry.Key)
		}
		if cf != entrykv.CFWrite {
			break
		}
		if len(currentKey) > 0 && !bytes.Equal(currentKey, userKey) {
			if err := flush(); err != nil {
				return result, err
			}
			if result.stopped {
				return result, nil
			}
		}
		if len(currentKey) == 0 {
			currentKey = entrykv.SafeCopy(nil, userKey)
		}
		if entry.Meta&entrykv.BitDelete > 0 {
			stats.DeletedWriteMarkers++
			iter.Next()
			continue
		}
		write, err := percolator.DecodeWrite(entry.Value)
		if err != nil {
			return result, fmt.Errorf("kv: decode CFWrite %x@%d: %w", userKey, commitTs, err)
		}
		versions = append(versions, percolator.GCWriteVersion{CommitTs: commitTs, Write: write})
		iter.Next()
	}
	return result, flush()
}

func seekMVCCGCStart(iter index.Iterator, afterUserKey []byte) {
	if len(afterUserKey) == 0 {
		iter.Seek(entrykv.InternalKey(entrykv.CFWrite, nil, entrykv.MaxVersion))
		return
	}
	iter.Seek(entrykv.InternalKey(entrykv.CFWrite, afterUserKey, 0))
	for iter.Valid() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		cf, userKey, _, ok := entrykv.SplitInternalKey(item.Entry().Key)
		if !ok || cf != entrykv.CFWrite || !bytes.Equal(userKey, afterUserKey) {
			return
		}
		iter.Next()
	}
}

type mvccGCApplyBatch struct {
	plan           MVCCGCPlanStats
	entries        []*entrykv.Entry
	lastUserKey    []byte
	writeDeletes   uint64
	defaultDeletes uint64
	done           bool
}

func collectMVCCGCApplyBatch(ctx context.Context, db NoKV.MVCCStore, policy MVCCGCSafePointPolicy, afterUserKey []byte, maxEntries int) (mvccGCApplyBatch, error) {
	var batch mvccGCApplyBatch
	result, err := walkMVCCGC(ctx, db, policy, afterUserKey, &batch.plan, func(userKey []byte, decisions []percolator.GCWriteDecision) error {
		writes, defaults := buildMVCCGCDeletes(userKey, decisions)
		batch.entries = append(batch.entries, writes...)
		batch.entries = append(batch.entries, defaults...)
		batch.writeDeletes += uint64(len(writes))
		batch.defaultDeletes += uint64(len(defaults))
		if len(batch.entries) >= maxEntries {
			return errMVCCGCStop
		}
		return nil
	})
	if err != nil {
		releaseEntries(batch.entries)
		return mvccGCApplyBatch{}, err
	}
	batch.lastUserKey = result.lastUserKey
	batch.done = !result.stopped
	return batch, nil
}

func buildMVCCGCDeletes(userKey []byte, decisions []percolator.GCWriteDecision) (writes, defaults []*entrykv.Entry) {
	retainedDefault := make(map[uint64]struct{})
	for _, decision := range decisions {
		if decision.RetainDefaultStartTs != 0 {
			retainedDefault[decision.RetainDefaultStartTs] = struct{}{}
		}
	}
	for _, decision := range decisions {
		if decision.Keep {
			continue
		}
		writes = append(writes, entrykv.NewInternalEntry(entrykv.CFWrite, userKey, decision.CommitTs, nil, entrykv.BitDelete, 0))
		if !percolator.WriteNeedsDefaultRecord(decision.Write) {
			continue
		}
		if _, ok := retainedDefault[decision.Write.StartTs]; ok {
			continue
		}
		defaults = append(defaults, entrykv.NewInternalEntry(entrykv.CFDefault, userKey, decision.Write.StartTs, nil, entrykv.BitDelete, 0))
	}
	return writes, defaults
}

func (s *MVCCGCPlanStats) recordGroup(requestedSafePoint, safePoint uint64, plan []percolator.GCWriteDecision) {
	s.Keys++
	if safePoint != 0 && safePoint < requestedSafePoint {
		s.SafePointClampedKeys++
	}
	if versions := uint64(len(plan)); versions > s.MaxVersionsPerKey {
		s.MaxVersionsPerKey = versions
	}
	if safePoint != 0 && (s.MinEffectiveSafePoint == 0 || safePoint < s.MinEffectiveSafePoint) {
		s.MinEffectiveSafePoint = safePoint
	}
	if safePoint > s.MaxEffectiveSafePoint {
		s.MaxEffectiveSafePoint = safePoint
	}
	for _, decision := range plan {
		s.WriteVersions++
		if decision.Keep {
			s.RetainedWrites++
		} else {
			s.DroppableWrites++
		}
		if decision.Anchor {
			s.AnchorWrites++
		}
		if decision.RetainDefaultStartTs != 0 {
			s.RetainedDefaultRefs++
		}
	}
}

func (s *MVCCGCPlanStats) add(other MVCCGCPlanStats) {
	s.Keys += other.Keys
	s.WriteVersions += other.WriteVersions
	s.RetainedWrites += other.RetainedWrites
	s.DroppableWrites += other.DroppableWrites
	s.AnchorWrites += other.AnchorWrites
	s.RetainedDefaultRefs += other.RetainedDefaultRefs
	s.DeletedWriteMarkers += other.DeletedWriteMarkers
	s.SafePointClampedKeys += other.SafePointClampedKeys
	if other.MaxVersionsPerKey > s.MaxVersionsPerKey {
		s.MaxVersionsPerKey = other.MaxVersionsPerKey
	}
	if other.MinEffectiveSafePoint != 0 && (s.MinEffectiveSafePoint == 0 || other.MinEffectiveSafePoint < s.MinEffectiveSafePoint) {
		s.MinEffectiveSafePoint = other.MinEffectiveSafePoint
	}
	if other.MaxEffectiveSafePoint > s.MaxEffectiveSafePoint {
		s.MaxEffectiveSafePoint = other.MaxEffectiveSafePoint
	}
}

func releaseEntries(entries []*entrykv.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
}
