// Package lsm implements NoKV's log-structured merge-tree engine.
// It owns the MemTable (with adaptive ART/SkipList index over arena),
// the flush pipeline (Prepare → Build → Install → Release), leveled
// compaction (planner + picker + executor), iterators, caches, range
// tombstones, range filter, external SST ingest, and the per-level
// landing buffer that avoids write stalls on L0 pressure.
//
// Durability ordering (enforced end-to-end):
//
//	WAL append → memtable apply → flush SST → manifest edit
//
// Crash at any point leaves a consistent state; the manifest publication
// is atomic via the CURRENT symlink plus varint edit log, and replay
// walks the shard WALs using segment retention marks.
//
// Design references: docs/memtable.md, docs/flush.md, docs/compaction.md,
// docs/landing_buffer.md, docs/range_filter.md, docs/cache.md.
package lsm

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/slab/negativecache"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/utils"
)

// LSM is the log-structured merge-tree engine. It wires N parallel
// data-plane shards (each owning an active memtable, an immutable queue,
// and a WAL Manager), the level manager, and the flush runtime into one
// coherent storage core. See the package docstring for the durability
// ordering invariant.
type LSM struct {
	shards           []*lsmShard
	shardHints       *shardHintTable
	negatives        *negativecache.Cache
	negativesPersist *negativecache.Persistence
	levels           *levelManager
	option           *Options
	closer           *utils.Closer
	flushPool        *flushPool
	logger           *slog.Logger

	throttle *writeThrottle

	closed atomic.Bool
}

// checkRangeTombstone is the core tombstone coverage check using pre-pinned
// memtables. This avoids a redundant GetMemTables call when the caller
// already holds a reference (e.g. inside lsm.Get).
func (lsm *LSM) checkRangeTombstone(cf kv.ColumnFamily, userKey []byte, entryVersion uint64, tables []*memTable) bool {
	// Check memtable tombstones (O(M), M = in-memory range tombstones).
	for _, mt := range tables {
		if mt == nil {
			continue
		}
		if mt.isKeyCoveredByRangeTombstone(cf, userKey, entryVersion) {
			return true
		}
	}
	// Check flushed range tombstones via collector (version-based).
	if lsm.levels == nil || lsm.levels.rtCollector == nil {
		return false
	}
	return lsm.levels.rtCollector.IsKeyCovered(cf, userKey, entryVersion)
}

// RangeTombstoneCount returns the number of tracked range tombstones.
func (lsm *LSM) RangeTombstoneCount() int {
	if lsm == nil || lsm.levels == nil || lsm.levels.rtCollector == nil {
		return 0
	}
	return lsm.levels.rtCollector.Count()
}

// Close  _
func (lsm *LSM) Close() error {
	if lsm == nil {
		return nil
	}
	if !lsm.closed.CompareAndSwap(false, true) {
		return nil
	}
	var closeErr error
	// wait for all api calls to finish
	lsm.throttle.Apply(WriteThrottleNone, 0, 0)
	if lsm.closer != nil {
		lsm.closer.Close()
	}
	if lsm.flushPool != nil {
		closeErr = errors.Join(closeErr, lsm.flushPool.Close())
	}

	var orphans []*memTable
	for _, s := range lsm.shards {
		s.lock.Lock()
		if s.memTable != nil {
			orphans = append(orphans, s.memTable)
		}
		orphans = append(orphans, s.immutables...)
		s.memTable = nil
		s.immutables = nil
		s.lock.Unlock()
	}

	for _, mt := range orphans {
		if mt == nil {
			continue
		}
		closeErr = errors.Join(closeErr, mt.close())
	}
	if lsm.levels != nil {
		closeErr = errors.Join(closeErr, lsm.levels.close())
	}
	if lsm.negativesPersist != nil {
		if n, err := lsm.negativesPersist.Snapshot(); err != nil {
			lsm.logger.Warn("negative cache snapshot on close failed",
				slog.String("err", err.Error()))
		} else if n > 0 {
			lsm.logger.Info("negative cache snapshot written",
				slog.Int("entries", n))
		}
	}
	return closeErr
}

// shardRetentionMark returns the retention bound for a single shard's WAL
// Manager. Each shard tracks its own highest flushed segment so that
// interleaved cross-shard flushes never truncate an unflushed segment from
// a peer shard.
func shardRetentionMark(s *lsmShard) wal.RetentionMark {
	if s == nil {
		return wal.RetentionMark{FirstSegment: 1}
	}
	return wal.RetentionMark{FirstSegment: s.highestFlushedSeg.Load() + 1}
}

func (lsm *LSM) getLogger() *slog.Logger {
	if lsm == nil || lsm.logger == nil {
		return slog.Default()
	}
	return lsm.logger
}

// ThrottleState reports the current write admission state.
func (lsm *LSM) ThrottleState() WriteThrottleState {
	if lsm == nil {
		return WriteThrottleNone
	}
	return lsm.throttle.State()
}

// ThrottlePressurePermille returns current write pacing pressure [0,1000].
func (lsm *LSM) ThrottlePressurePermille() uint32 {
	if lsm == nil {
		return 0
	}
	return lsm.throttle.PressurePermille()
}

// ThrottleRateBytesPerSec returns the current slowdown target in bytes/sec.
func (lsm *LSM) ThrottleRateBytesPerSec() uint64 {
	if lsm == nil {
		return 0
	}
	return lsm.throttle.RateBytesPerSec()
}

// FlushPending returns the number of pending flush tasks.
func (lsm *LSM) FlushPending() int64 {
	if lsm == nil {
		return 0
	}
	return lsm.flushPool.Pending()
}

// MaxVersion returns the largest commit timestamp known to the LSM tree.
func (lsm *LSM) MaxVersion() uint64 {
	if lsm == nil {
		return 0
	}

	var max uint64

	for _, s := range lsm.shards {
		s.lock.RLock()
		if s.memTable != nil {
			if v := s.memTable.maxVersion.Load(); v > max {
				max = v
			}
		}
		for _, mt := range s.immutables {
			if mt == nil {
				continue
			}
			if v := mt.maxVersion.Load(); v > max {
				max = v
			}
		}
		s.lock.RUnlock()
	}

	if lm := lsm.levels; lm != nil {
		if v := lm.maxVersion(); v > max {
			max = v
		}
	}

	return max
}

// NewLSM constructs the LSM core, binding one shard to each WAL Manager
// in walMgrs. The slice must be non-empty; len(walMgrs) is the data-plane
// shard count.
func NewLSM(opt *Options, walMgrs []*wal.Manager) (*LSM, error) {
	if opt == nil {
		return nil, ErrLSMNilOptions
	}
	if len(walMgrs) == 0 {
		return nil, ErrLSMNilWALManager
	}
	for _, mgr := range walMgrs {
		if mgr == nil {
			return nil, ErrLSMNilWALManager
		}
	}
	frozen := opt.Clone()
	frozen.NormalizeInPlace()
	if frozen == nil {
		return nil, ErrLSMNilClonedOptions
	}
	shards := make([]*lsmShard, len(walMgrs))
	for i, mgr := range walMgrs {
		shards[i] = newLSMShard(i, mgr)
	}
	lsm := &LSM{
		option:     frozen,
		shards:     shards,
		shardHints: newShardHintTable(),
		throttle:   newWriteThrottle(),
		closer:     utils.NewCloser(),
		logger:     frozen.Logger,
	}
	if frozen.NegativeCachePersistent && frozen.WorkDir != "" {
		inner, persist, err := negativecache.OpenWithPersistence(
			negativecache.Config{GroupKeyFn: kv.InternalToBaseKey},
			negativecache.PersistConfig{
				Dir:     filepath.Join(frozen.WorkDir, "negative-slab"),
				MaxSize: frozen.NegativeCacheSlabMaxSize,
			},
		)
		if err != nil {
			if lsm.logger == nil {
				lsm.logger = slog.Default()
			}
			lsm.logger.Warn("negative cache restore failed; cold start",
				slog.String("err", err.Error()))
		}
		lsm.negatives = inner
		lsm.negativesPersist = persist
	} else {
		lsm.negatives = negativecache.New(negativecache.Config{
			GroupKeyFn: kv.InternalToBaseKey,
		})
	}
	if lsm.logger == nil {
		lsm.logger = slog.Default()
	}
	lsm.throttle.SetCallback(frozen.ThrottleCallback)
	// initialize levelManager
	lm, err := lsm.initLevelManager(frozen)
	if err != nil {
		return nil, fmt.Errorf("lsm init level manager: %w", err)
	}
	lsm.levels = lm
	for _, s := range lsm.shards {
		shard := s // closure capture per shard
		if err := shard.wal.RegisterRetention("lsm", func() wal.RetentionMark {
			return shardRetentionMark(shard)
		}); err != nil {
			return nil, fmt.Errorf("lsm register wal retention shard %d: %w", shard.id, err)
		}
	}
	// Populate range tombstone collector from existing SSTables
	if lsm.levels != nil && lsm.levels.rtCollector != nil {
		lsm.levels.rebuildRangeTombstones()
	}
	// Recover each shard's memtable queue from its own WAL.
	for _, s := range lsm.shards {
		s.memTable, s.immutables, err = lsm.recoverShard(s)
		if err != nil {
			_ = lsm.Close()
			return nil, fmt.Errorf("lsm recovery shard %d: %w", s.id, err)
		}
	}
	lsm.flushPool = newFlushPool(len(lsm.shards), lsm.levels.flush)
	lsm.flushPool.Start(len(lsm.shards))
	for _, s := range lsm.shards {
		for _, mt := range s.immutables {
			if err := lsm.flushPool.Submit(mt); err != nil {
				_ = lsm.Close()
				return nil, fmt.Errorf("lsm submit recovered flush task: %w", err)
			}
		}
	}
	return lsm, nil
}

// StartCompacter _
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	lsm.closer.Add(n)
	for i := range n {
		go lsm.levels.sched.Start(i, lsm.closer.Closed(), lsm.closer.Done)
	}
}

const (
	walRecordOverhead     int64 = 9 // length(4) + type(1) + crc(4)
	walBatchCountOverhead int64 = 4 // uint32 entry count
	walBatchLenOverhead   int64 = 4 // uint32 per-entry encoded length
)

func estimatePipelineBatchWALSize(entries []*kv.Entry) int64 {
	if len(entries) == 0 {
		return 0
	}
	size := walRecordOverhead + walBatchCountOverhead
	for _, entry := range entries {
		size += int64(kv.EstimateEncodeSize(entry)) + walBatchLenOverhead
	}
	return size
}

type writeBatch struct {
	entries []*kv.Entry
	index   int
}

func (b *writeBatch) estimate() int64 {
	if b == nil {
		return 0
	}
	return estimatePipelineBatchWALSize(b.entries)
}

func (lsm *LSM) applyWriteBatches(s *lsmShard, batches []*writeBatch) (int, error) {
	for len(batches) > 0 {
		n, err := lsm.writeSome(s, batches)
		if err != nil {
			return batches[0].index, err
		}
		if n == 0 {
			if err := lsm.rotateForWriteShard(s); err != nil {
				return batches[0].index, err
			}
			continue
		}
		batches = batches[n:]
	}
	return -1, nil
}

func (lsm *LSM) writeSome(s *lsmShard, batches []*writeBatch) (int, error) {
	if s == nil {
		return 0, ErrMemtableNotInitialized
	}
	s.lock.RLock()
	mt := s.memTable
	if mt == nil {
		s.lock.RUnlock()
		return 0, ErrMemtableNotInitialized
	}
	n, entries, estimate, err := fitWritePrefix(mt, lsm.option.MemTableSize, batches)
	if err != nil {
		s.lock.RUnlock()
		return 0, err
	}
	if n == 0 {
		s.lock.RUnlock()
		return 0, nil
	}
	info, err := s.wal.AppendEntryBatch(wal.DurabilityFlushed, entries)
	if err != nil {
		s.lock.RUnlock()
		return 0, err
	}
	walBytes := int64(info.Length) + 8
	if estimate > 0 && walBytes > estimate {
		// The estimator is conservative for admission, but the persisted byte
		// count is the WAL return value. Keep this guard to catch encoder drift
		// before it silently overcommits the active memtable.
		s.lock.RUnlock()
		panic(fmt.Sprintf("lsm: WAL batch larger than estimate: got=%d estimate=%d", walBytes, estimate))
	}
	if err := mt.applyBatch(entries, walBytes); err != nil {
		s.lock.RUnlock()
		panic(fmt.Sprintf("lsm: durable WAL batch could not be applied to memtable: %v", err))
	}
	lsm.invalidateNegativeCache(entries)
	lsm.recordShardHints(s.id, entries)
	s.lock.RUnlock()
	return n, nil
}

func fitWritePrefix(mt *memTable, limit int64, batches []*writeBatch) (int, []*kv.Entry, int64, error) {
	if mt == nil || len(batches) == 0 {
		return 0, nil, 0, nil
	}
	var entries []*kv.Entry
	var bestN int
	var bestEstimate int64
	for i, batch := range batches {
		if batch == nil || len(batch.entries) == 0 {
			continue
		}
		if err := validateWriteEntries(batch.entries); err != nil {
			if bestN == 0 {
				return 0, nil, 0, err
			}
			break
		}
		if batch.estimate() > limit {
			if bestN == 0 {
				return 0, nil, 0, utils.ErrTxnTooBig
			}
			break
		}
		entries = append(entries, batch.entries...)
		estimate := estimatePipelineBatchWALSize(entries)
		if !mt.canReserve(estimate, limit) {
			break
		}
		bestN = i + 1
		bestEstimate = estimate
	}
	if bestN == 0 {
		return 0, nil, 0, nil
	}
	return bestN, entries[:totalWriteEntries(batches[:bestN])], bestEstimate, nil
}

func validateWriteEntries(entries []*kv.Entry) error {
	for _, entry := range entries {
		if entry == nil || len(entry.Key) == 0 {
			return utils.ErrEmptyKey
		}
	}
	return nil
}

func totalWriteEntries(batches []*writeBatch) int {
	var total int
	for _, batch := range batches {
		if batch != nil {
			total += len(batch.entries)
		}
	}
	return total
}

func (lsm *LSM) rotateForWriteShard(s *lsmShard) error {
	s.lock.Lock()
	old, err := lsm.rotateShardLocked(s)
	s.lock.Unlock()
	if err != nil {
		return err
	}
	return lsm.flushPool.Submit(old)
}

func (lsm *LSM) prepareWrite() error {
	if lsm == nil {
		return ErrLSMNil
	}
	if lsm.closed.Load() {
		return ErrLSMClosed
	}
	lsm.closer.Add(1)
	if lsm.closed.Load() {
		lsm.closer.Done()
		return ErrLSMClosed
	}
	return nil
}

// Set writes one entry into shard 0's memtable/WAL. Use SetBatchGroup for
// commit-pipeline writes that need explicit shard routing.
// entry.Key must be an InternalKey (CF + user key + timestamp suffix).
func (lsm *LSM) Set(entry *kv.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	return lsm.SetBatch([]*kv.Entry{entry})
}

// SetBatch atomically writes a batch of entries into shard 0's WAL record.
// Used by non-pipeline callers (admin tools, recovery glue, tests).
func (lsm *LSM) SetBatch(entries []*kv.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	_, err := lsm.SetBatchGroup(0, [][]*kv.Entry{entries})
	return err
}

// SetBatchGroup writes multiple atomic batches into the WAL+memtable of the
// specified shard.
//
// Each inner batch remains indivisible: rotation may split between batches,
// but never inside one batch. The returned failedAt is the first batch index
// that was not applied, or -1 on success. Routing is the caller's choice —
// per-key affinity preserves SetBatch atomicity.
func (lsm *LSM) SetBatchGroup(shardID int, groups [][]*kv.Entry) (int, error) {
	if len(groups) == 0 {
		return -1, nil
	}
	if shardID < 0 || shardID >= len(lsm.shards) {
		return 0, fmt.Errorf("lsm: shardID %d out of range [0,%d)", shardID, len(lsm.shards))
	}
	if err := lsm.prepareWrite(); err != nil {
		return 0, err
	}
	defer lsm.closer.Done()
	batches := make([]*writeBatch, 0, len(groups))
	for idx, entries := range groups {
		if len(entries) == 0 {
			continue
		}
		// LSM is the sole consumer of entries for the duration of this call,
		// and it does not mutate the slice. Aliasing the caller's slice avoids
		// a per-batch allocation on the write hot path.
		batches = append(batches, &writeBatch{entries: entries, index: idx})
	}
	if len(batches) == 0 {
		return -1, nil
	}
	return lsm.applyWriteBatches(lsm.shards[shardID], batches)
}

// Get returns the newest visible entry for key.
// key must be an InternalKey.
func (lsm *LSM) Get(key []byte) (*kv.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()

	hasRangeTombstones := lsm.hasRangeTombstones()
	if !hasRangeTombstones && lsm.negativeHit(key) {
		return nil, utils.ErrKeyNotFound
	}

	if kv.Timestamp(key) == kv.MaxVersion {
		if shardID, ok := lsm.lookupShardHint(key); ok && !hasRangeTombstones {
			tables, release := lsm.getMemTablesForShard(shardID)
			best := bestMemtableEntry(key, tables)
			if release != nil {
				release()
			}
			if best != nil {
				return best, nil
			}
		}
	}

	tables, release := lsm.getMemTables()
	if release != nil {
		defer release()
	}

	// isCovered checks range tombstone coverage for a found entry using
	// the already-pinned memtables, avoiding a second GetMemTables call.
	isCovered := func(entry *kv.Entry) bool {
		if entry == nil || entry.IsRangeDelete() {
			return false
		}
		cf, userKey, _, ok := kv.SplitInternalKey(key)
		if !ok {
			return false
		}
		return lsm.checkRangeTombstone(cf, userKey, entry.Version, tables)
	}

	// With multiple shards each memtable is an independent timeline; the
	// same userKey may live on more than one shard at different versions.
	// Walk every memtable and keep the highest-version hit so MVCC reads
	// see the most recent write regardless of which shard accepted it.
	best := bestMemtableEntry(key, tables)
	if best != nil && isCovered(best) {
		best.DecrRef()
		best = nil
	}
	// query from the levels runtime
	entry, err := lsm.levels.Get(key)
	if err != nil && err != utils.ErrKeyNotFound {
		if best != nil {
			best.DecrRef()
		}
		return nil, err
	}
	if entry != nil {
		if isCovered(entry) {
			entry.DecrRef()
		} else if best == nil || entry.Version > best.Version {
			if best != nil {
				best.DecrRef()
			}
			best = entry
		} else {
			entry.DecrRef()
		}
	}
	if best == nil {
		if !hasRangeTombstones {
			lsm.rememberNegative(key)
		}
		return nil, utils.ErrKeyNotFound
	}
	return best, nil
}

func (lsm *LSM) negativeHit(key []byte) bool {
	if lsm == nil {
		return false
	}
	return lsm.negatives.Has(key)
}

func (lsm *LSM) rememberNegative(key []byte) {
	if lsm == nil {
		return
	}
	lsm.negatives.Remember(key)
}

func (lsm *LSM) invalidateNegativeCache(entries []*kv.Entry) {
	if lsm == nil || lsm.negatives == nil {
		return
	}
	for _, entry := range entries {
		if entry == nil || len(entry.Key) == 0 {
			continue
		}
		lsm.negatives.Invalidate(entry.Key)
	}
}

func (lsm *LSM) clearNegativeCache() {
	if lsm == nil {
		return
	}
	lsm.negatives.Clear()
}

func (lsm *LSM) lookupShardHint(key []byte) (int, bool) {
	if lsm == nil || len(lsm.shards) <= 1 || lsm.shardHints == nil {
		return 0, false
	}
	shardID, ok := lsm.shardHints.lookup(key)
	if !ok || shardID < 0 || shardID >= len(lsm.shards) {
		return 0, false
	}
	return shardID, true
}

func (lsm *LSM) recordShardHints(shardID int, entries []*kv.Entry) {
	if lsm == nil || len(lsm.shards) <= 1 || lsm.shardHints == nil {
		return
	}
	for _, entry := range entries {
		if entry == nil || len(entry.Key) == 0 {
			continue
		}
		lsm.shardHints.remember(entry.Key, shardID)
	}
}

func bestMemtableEntry(key []byte, tables []*memTable) *kv.Entry {
	var best *kv.Entry
	for _, mt := range tables {
		if mt == nil {
			continue
		}
		entry, _ := mt.Get(key)
		if !isMemtableHit(entry) {
			if entry != nil {
				entry.DecrRef()
			}
			continue
		}
		if best == nil || entry.Version > best.Version {
			if best != nil {
				best.DecrRef()
			}
			best = entry
		} else {
			entry.DecrRef()
		}
	}
	return best
}

func isMemtableHit(entry *kv.Entry) bool {
	if entry == nil {
		return false
	}
	return entry.Value != nil || entry.Meta != 0 || entry.ExpiresAt != 0
}

// MemSize returns the active memtable memory usage summed across shards.
func (lsm *LSM) MemSize() int64 {
	var total int64
	for _, s := range lsm.shards {
		s.lock.RLock()
		if s.memTable != nil {
			total += s.memTable.Size()
		}
		s.lock.RUnlock()
	}
	return total
}

// memTableIsNil reports whether any shard has a nil active memtable.
func (lsm *LSM) memTableIsNil() bool {
	for _, s := range lsm.shards {
		s.lock.RLock()
		nilMT := s.memTable == nil
		s.lock.RUnlock()
		if nilMT {
			return true
		}
	}
	return false
}

// Rotate seals every shard's active memtable, creates fresh ones, and
// schedules each old memtable for flush.
func (lsm *LSM) Rotate() error {
	for _, s := range lsm.shards {
		s.lock.Lock()
		old, err := lsm.rotateShardLocked(s)
		s.lock.Unlock()
		if err != nil {
			return err
		}
		if err := lsm.flushPool.Submit(old); err != nil {
			return err
		}
	}
	return nil
}

// rotateShardLocked swaps the shard's active memtable; caller must hold s.lock.
func (lsm *LSM) rotateShardLocked(s *lsmShard) (*memTable, error) {
	old := s.memTable
	next, err := lsm.newMemtableForShard(s)
	if err != nil {
		return nil, err
	}
	s.immutables = append(s.immutables, old)
	s.memTable = next
	return old, nil
}

// getMemTables pins active+immutable memtables across all shards and returns
// an unlock callback. Newest-first ordering within each shard is preserved
// (active memtable, then immutables in reverse insertion order). Callers
// that need MVCC ordering across shards rely on internal-key timestamps.
func (lsm *LSM) getMemTables() ([]*memTable, func()) {
	var tables []*memTable
	for _, s := range lsm.shards {
		s.lock.RLock()
		if s.memTable != nil {
			tables = append(tables, s.memTable)
			s.memTable.IncrRef()
		}
		last := len(s.immutables) - 1
		for i := range s.immutables {
			tables = append(tables, s.immutables[last-i])
			s.immutables[last-i].IncrRef()
		}
		s.lock.RUnlock()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

func (lsm *LSM) getMemTablesForShard(shardID int) ([]*memTable, func()) {
	if lsm == nil || shardID < 0 || shardID >= len(lsm.shards) {
		return nil, nil
	}
	s := lsm.shards[shardID]
	s.lock.RLock()
	defer s.lock.RUnlock()
	tables := make([]*memTable, 0, 1+len(s.immutables))
	if s.memTable != nil {
		tables = append(tables, s.memTable)
		s.memTable.IncrRef()
	}
	last := len(s.immutables) - 1
	for i := range s.immutables {
		tables = append(tables, s.immutables[last-i])
		s.immutables[last-i].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

func (lsm *LSM) hasRangeTombstones() bool {
	if lsm == nil {
		return false
	}
	if lsm.levels != nil && lsm.levels.rtCollector != nil && lsm.levels.rtCollector.Count() > 0 {
		return true
	}
	for _, s := range lsm.shards {
		s.lock.RLock()
		if s.memTable != nil && s.memTable.hasRangeTombstones() {
			s.lock.RUnlock()
			return true
		}
		for _, mt := range s.immutables {
			if mt != nil && mt.hasRangeTombstones() {
				s.lock.RUnlock()
				return true
			}
		}
		s.lock.RUnlock()
	}
	return false
}


// ---- Range tombstone view (merged from range_tombstone.go) ----
//
// The view pins a stable memtable snapshot for repeated coverage probes,
// avoiding per-key GetMemTables pin/unpin overhead on scan paths
// (e.g. DB iterators / YCSB-E).

// RangeTombstoneView captures a stable read-view for range tombstone checks.
// Call Close when finished.
type RangeTombstoneView struct {
	lsm     *LSM
	tables  []*memTable
	release func()
}

// HasAnyRangeTombstone reports whether the current LSM state has any in-memory
// or flushed range tombstones.
func (lsm *LSM) HasAnyRangeTombstone() bool {
	if lsm == nil {
		return false
	}
	for _, s := range lsm.shards {
		s.lock.RLock()
		mem := s.memTable
		immutables := s.immutables
		s.lock.RUnlock()
		if mem != nil && mem.hasRangeTombstones() {
			return true
		}
		for _, mt := range immutables {
			if mt != nil && mt.hasRangeTombstones() {
				return true
			}
		}
	}
	return lsm.RangeTombstoneCount() > 0
}

// PinRangeTombstoneView captures and pins the current memtable set for repeated
// range tombstone checks.
func (lsm *LSM) PinRangeTombstoneView() *RangeTombstoneView {
	if lsm == nil {
		return nil
	}
	tables, release := lsm.getMemTables()
	return &RangeTombstoneView{
		lsm:     lsm,
		tables:  tables,
		release: release,
	}
}

// IsKeyCovered checks whether userKey@version is covered in this pinned view.
func (v *RangeTombstoneView) IsKeyCovered(cf kv.ColumnFamily, userKey []byte, version uint64) bool {
	if v == nil || v.lsm == nil {
		return false
	}
	return v.lsm.checkRangeTombstone(cf, userKey, version, v.tables)
}

// Close releases pinned memtables held by this view.
func (v *RangeTombstoneView) Close() {
	if v == nil {
		return
	}
	if v.release != nil {
		v.release()
	}
	v.tables = nil
	v.release = nil
	v.lsm = nil
}
