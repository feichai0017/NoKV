// Package lsm implements NoKV's log-structured merge-tree engine.
// It owns the MemTable (with adaptive ART/SkipList index over arena),
// the flush pipeline (Prepare → Build → Install → Release), leveled
// compaction (planner + picker + executor), iterators, caches, range
// tombstones, range filter, and external SST ingest (with an ingest
// buffer that avoids write stalls on L0 pressure).
//
// Durability ordering (enforced end-to-end):
//
//	vlog append → WAL append → memtable apply → flush SST → manifest edit
//
// Crash at any point leaves a consistent state; the manifest publication
// is atomic via the CURRENT symlink plus varint edit log, and replay
// walks the WAL checkpoint stored in the manifest.
//
// WAL and value log segment managers live in sibling packages
// (engine/wal, engine/vlog). This package does not own their durable
// bytes — it only consumes their APIs.
//
// Design references: docs/memtable.md, docs/flush.md, docs/compaction.md,
// docs/ingest_buffer.md, docs/range_filter.md, docs/cache.md, and the
// dated notes under docs/notes/ beginning with 2026-02-01 through 2026-04-05.
package lsm

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/utils"
)

// LSM is the log-structured merge-tree engine. It wires a single
// active memtable, a queue of immutable memtables, the level manager,
// the flush runtime, and the shared WAL into one coherent storage core.
// See the package docstring for the durability ordering invariant.
type LSM struct {
	lock       sync.RWMutex
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
	wal        *wal.Manager
	flushQueue *flushRuntime
	flushWG    sync.WaitGroup
	logger     *slog.Logger

	discardStatsCh chan map[manifest.ValueLogID]int64
	walGCPolicy    WALGCPolicy

	throttleFn    func(WriteThrottleState)
	throttleState atomic.Int32
	// throttlePressure stores pacing pressure in permille [0,1000].
	throttlePressure atomic.Uint32
	// throttleRate stores the current slowdown target in bytes/sec.
	throttleRate atomic.Uint64

	closed atomic.Bool
}

// WriteThrottleState models write admission control at the DB layer.
//
// Design:
// - WriteThrottleNone: writes proceed without extra delay.
// - WriteThrottleSlowdown: writes are accepted but paced.
// - WriteThrottleStop: writes are blocked until backlog recovers.
type WriteThrottleState int32

const (
	WriteThrottleNone WriteThrottleState = iota
	WriteThrottleSlowdown
	WriteThrottleStop
)

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
	lsm.throttleWrites(WriteThrottleNone, 0, 0)
	if lsm.closer != nil {
		lsm.closer.Close()
	}
	if lsm.flushQueue != nil {
		closeErr = errors.Join(closeErr, lsm.flushQueue.close())
	}
	lsm.flushWG.Wait()

	lsm.lock.Lock()
	mem := lsm.memTable
	immutables := append([]*memTable(nil), lsm.immutables...)
	lsm.memTable = nil
	lsm.immutables = nil
	lsm.lock.Unlock()

	if mem != nil {
		closeErr = errors.Join(closeErr, mem.close())
	}
	for _, mt := range immutables {
		if mt == nil {
			continue
		}
		closeErr = errors.Join(closeErr, mt.close())
	}
	if lsm.levels != nil {
		closeErr = errors.Join(closeErr, lsm.levels.close())
	}
	return closeErr
}

func (lsm *LSM) getDiscardStatsCh() chan map[manifest.ValueLogID]int64 {
	if lsm == nil {
		return nil
	}
	return lsm.discardStatsCh
}

func (lsm *LSM) getWALGCPolicy() WALGCPolicy {
	if lsm == nil {
		return AllowAllWALGCPolicy{}
	}
	return normalizeWALGCPolicy(lsm.walGCPolicy)
}

func (lsm *LSM) canRemoveWalSegment(id uint32) bool {
	return lsm.getWALGCPolicy().CanRemoveSegment(id)
}

func (lsm *LSM) getLogger() *slog.Logger {
	if lsm == nil || lsm.logger == nil {
		return slog.Default()
	}
	return lsm.logger
}

// ThrottleState reports the current write admission state.
func (lsm *LSM) ThrottleState() WriteThrottleState {
	return normalizeWriteThrottleState(WriteThrottleState(lsm.throttleState.Load()))
}

func normalizeWriteThrottleState(state WriteThrottleState) WriteThrottleState {
	switch state {
	case WriteThrottleNone, WriteThrottleSlowdown, WriteThrottleStop:
		return state
	default:
		return WriteThrottleNone
	}
}

// ThrottlePressurePermille returns current write pacing pressure [0,1000].
func (lsm *LSM) ThrottlePressurePermille() uint32 {
	if lsm == nil {
		return 0
	}
	p := lsm.throttlePressure.Load()
	if p > 1000 {
		return 1000
	}
	return p
}

// ThrottleRateBytesPerSec returns the current slowdown target in bytes/sec.
func (lsm *LSM) ThrottleRateBytesPerSec() uint64 {
	if lsm == nil {
		return 0
	}
	return lsm.throttleRate.Load()
}

func (lsm *LSM) throttleWrites(state WriteThrottleState, pressure uint32, rate uint64) {
	state = normalizeWriteThrottleState(state)
	if pressure > 1000 {
		pressure = 1000
	}
	switch state {
	case WriteThrottleNone:
		pressure = 0
		rate = 0
	case WriteThrottleStop:
		pressure = 1000
		rate = 0
	default:
	}
	lsm.throttlePressure.Store(pressure)
	lsm.throttleRate.Store(rate)
	prev := normalizeWriteThrottleState(WriteThrottleState(lsm.throttleState.Swap(int32(state))))
	if prev == state {
		return
	}
	fn := lsm.throttleFn
	if fn == nil {
		return
	}
	fn(state)
}

// FlushPending returns the number of pending flush tasks.
func (lsm *LSM) FlushPending() int64 {
	if lsm == nil || lsm.flushQueue == nil {
		return 0
	}
	return lsm.flushQueue.stats().Pending
}

// MaxVersion returns the largest commit timestamp known to the LSM tree.
func (lsm *LSM) MaxVersion() uint64 {
	if lsm == nil {
		return 0
	}

	var max uint64

	lsm.lock.RLock()
	if lsm.memTable != nil && lsm.memTable.maxVersion > max {
		max = lsm.memTable.maxVersion
	}
	for _, mt := range lsm.immutables {
		if mt == nil {
			continue
		}
		if mt.maxVersion > max {
			max = mt.maxVersion
		}
	}
	lsm.lock.RUnlock()

	if lm := lsm.levels; lm != nil {
		if v := lm.maxVersion(); v > max {
			max = v
		}
	}

	return max
}

// LogValueLogHead persists value log head pointer via manifest.
func (lsm *LSM) LogValueLogHead(ptr *kv.ValuePtr) error {
	if lsm == nil || lsm.levels == nil || lsm.levels.manifestMgr == nil || ptr == nil {
		return nil
	}
	return lsm.levels.manifestMgr.LogValueLogHead(ptr.Bucket, ptr.Fid, uint64(ptr.Offset))
}

// LogValueLogDelete records removal of a value log segment.
func (lsm *LSM) LogValueLogDelete(bucket uint32, fid uint32) error {
	if lsm == nil || lsm.levels == nil || lsm.levels.manifestMgr == nil {
		return nil
	}
	return lsm.levels.manifestMgr.LogValueLogDelete(bucket, fid)
}

// LogValueLogUpdate restores or amends metadata for a value log segment.
func (lsm *LSM) LogValueLogUpdate(meta *manifest.ValueLogMeta) error {
	if lsm == nil || lsm.levels == nil || lsm.levels.manifestMgr == nil || meta == nil {
		return nil
	}
	return lsm.levels.manifestMgr.LogValueLogUpdate(*meta)
}

// NewLSM constructs the LSM core and returns initialization errors.
func NewLSM(opt *Options, walMgr *wal.Manager) (*LSM, error) {
	if opt == nil {
		return nil, ErrLSMNilOptions
	}
	if walMgr == nil {
		return nil, ErrLSMNilWALManager
	}
	frozen := opt.Clone()
	frozen.NormalizeInPlace()
	if frozen == nil {
		return nil, ErrLSMNilClonedOptions
	}
	lsm := &LSM{
		option: frozen,
		wal:    walMgr,
		closer: utils.NewCloser(),
		logger: frozen.Logger,
	}
	if lsm.logger == nil {
		lsm.logger = slog.Default()
	}
	if frozen.DiscardStatsCh != nil {
		lsm.discardStatsCh = *frozen.DiscardStatsCh
	}
	lsm.walGCPolicy = normalizeWALGCPolicy(frozen.WALGCPolicy)
	lsm.throttleFn = frozen.ThrottleCallback
	lsm.flushQueue = newFlushRuntime()
	// initialize levelManager
	lm, err := lsm.initLevelManager(frozen)
	if err != nil {
		return nil, fmt.Errorf("lsm init level manager: %w", err)
	}
	lsm.levels = lm
	// Populate range tombstone collector from existing SSTables
	if lsm.levels != nil && lsm.levels.rtCollector != nil {
		lsm.levels.rebuildRangeTombstones()
	}
	// start the db recovery process to load the wal, if there is no recovery content, create a new memtable
	lsm.memTable, lsm.immutables, err = lsm.recovery()
	if err != nil {
		_ = lsm.Close()
		return nil, fmt.Errorf("lsm recovery: %w", err)
	}
	lsm.startFlushWorkers(1)
	for _, mt := range lsm.immutables {
		if err := lsm.submitFlush(mt); err != nil {
			_ = lsm.Close()
			return nil, fmt.Errorf("lsm submit recovered flush task: %w", err)
		}
	}
	return lsm, nil
}

// StartCompacter _
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	lsm.closer.Add(n)
	for i := range n {
		go lsm.levels.compaction.Start(i, lsm.closer.Closed(), lsm.closer.Done)
	}
}

const (
	walRecordOverhead     int64 = 9 // length(4) + type(1) + crc(4)
	walBatchCountOverhead int64 = 4 // uint32 entry count
	walBatchLenOverhead   int64 = 4 // uint32 per-entry encoded length
)

func estimateSingleEntryWALSize(entry *kv.Entry) int64 {
	return int64(kv.EstimateEncodeSize(entry)) + walRecordOverhead
}

func estimateBatchWALSize(entries []*kv.Entry) int64 {
	if len(entries) <= 1 {
		if len(entries) == 0 {
			return 0
		}
		return estimateSingleEntryWALSize(entries[0])
	}
	size := walRecordOverhead + walBatchCountOverhead
	for _, entry := range entries {
		size += int64(kv.EstimateEncodeSize(entry)) + walBatchLenOverhead
	}
	return size
}

// Set writes one entry into the active memtable/WAL.
// entry.Key must be an InternalKey (CF + user key + timestamp suffix).
func (lsm *LSM) Set(entry *kv.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// graceful shutdown
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	// Reserve capacity under read lock so concurrent writers can proceed without
	// serialising on lsm.lock unless a rotation is required.
	estimate := estimateSingleEntryWALSize(entry)
	for {
		lsm.lock.RLock()
		mt := lsm.memTable
		if mt == nil {
			lsm.lock.RUnlock()
			return ErrMemtableNotInitialized
		}
		if mt.tryReserve(estimate, lsm.option.MemTableSize) {
			err = mt.Set(entry)
			mt.releaseReserve(estimate)
			lsm.lock.RUnlock()
			return err
		}
		lsm.lock.RUnlock()

		var (
			old    *memTable
			rotErr error
		)
		lsm.lock.Lock()
		if lsm.memTable == mt && !mt.canReserve(estimate, lsm.option.MemTableSize) {
			old, rotErr = lsm.rotateLocked()
		}
		lsm.lock.Unlock()
		if rotErr != nil {
			return rotErr
		}
		if old != nil {
			if err := lsm.submitFlush(old); err != nil {
				return err
			}
		}
	}
}

// SetBatch atomically writes a batch of entries into one memtable WAL record.
//
// The batch is treated as an indivisible unit: either the entire batch is
// accepted by the active memtable (after at most one rotation), or the call
// fails. Batches larger than MemTableSize are rejected with ErrTxnTooBig.
// Every entry key in the batch must be an InternalKey.
func (lsm *LSM) SetBatch(entries []*kv.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	for _, entry := range entries {
		if entry == nil || len(entry.Key) == 0 {
			return utils.ErrEmptyKey
		}
	}
	totalEstimate := estimateBatchWALSize(entries)
	if totalEstimate > lsm.option.MemTableSize {
		return utils.ErrTxnTooBig
	}
	for {
		lsm.lock.RLock()
		mt := lsm.memTable
		if mt == nil {
			lsm.lock.RUnlock()
			return ErrMemtableNotInitialized
		}
		if mt.tryReserve(totalEstimate, lsm.option.MemTableSize) {
			err := mt.setBatch(entries)
			mt.releaseReserve(totalEstimate)
			lsm.lock.RUnlock()
			return err
		}
		lsm.lock.RUnlock()

		var (
			old    *memTable
			rotErr error
		)
		lsm.lock.Lock()
		if lsm.memTable == mt && !mt.canReserve(totalEstimate, lsm.option.MemTableSize) {
			old, rotErr = lsm.rotateLocked()
		}
		lsm.lock.Unlock()
		if rotErr != nil {
			return rotErr
		}
		if old != nil {
			if err := lsm.submitFlush(old); err != nil {
				return err
			}
		}
	}
}

// Get returns the newest visible entry for key.
// key must be an InternalKey.
func (lsm *LSM) Get(key []byte) (*kv.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	tables, release := lsm.getMemTables()
	if release != nil {
		defer release()
	}
	isMemHit := func(entry *kv.Entry) bool {
		if entry == nil {
			return false
		}
		return entry.Value != nil || entry.Meta != 0 || entry.ExpiresAt != 0
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

	for _, mt := range tables {
		if mt == nil {
			continue
		}
		entry, err := mt.Get(key)
		if isMemHit(entry) {
			if isCovered(entry) {
				entry.DecrRef()
				return nil, utils.ErrKeyNotFound
			}
			return entry, err
		}
		if entry != nil {
			entry.DecrRef()
		}
	}
	// query from the levels runtime
	entry, err := lsm.levels.Get(key)
	if err != nil || entry == nil {
		return entry, err
	}
	if isCovered(entry) {
		entry.DecrRef()
		return nil, utils.ErrKeyNotFound
	}
	return entry, nil
}

// MemSize returns the current active memtable memory usage.
func (lsm *LSM) MemSize() int64 {
	return lsm.memTable.Size()
}

// memTableIsNil reports whether the active memtable pointer is unset.
func (lsm *LSM) memTableIsNil() bool {
	return lsm.memTable == nil
}

// Rotate seals the active memtable, creates a new one, and schedules flush.
func (lsm *LSM) Rotate() error {
	lsm.lock.Lock()
	old, err := lsm.rotateLocked()
	lsm.lock.Unlock()
	if err != nil {
		return err
	}
	return lsm.submitFlush(old)
}

// rotateLocked swaps the active memtable; caller must hold lsm.lock.
func (lsm *LSM) rotateLocked() (*memTable, error) {
	old := lsm.memTable
	next, err := lsm.NewMemtable()
	if err != nil {
		return nil, err
	}
	lsm.immutables = append(lsm.immutables, old)
	lsm.memTable = next
	return old, nil
}

// getMemTables pins active+immutable memtables and returns an unlock callback.
func (lsm *LSM) getMemTables() ([]*memTable, func()) {
	lsm.lock.RLock()
	defer lsm.lock.RUnlock()

	var tables []*memTable

	tables = append(tables, lsm.memTable)
	lsm.memTable.IncrRef()

	last := len(lsm.immutables) - 1
	for i := range lsm.immutables {
		tables = append(tables, lsm.immutables[last-i])
		lsm.immutables[last-i].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}

}

func (lsm *LSM) submitFlush(mt *memTable) error {
	if mt == nil {
		return nil
	}
	mt.IncrRef()
	if err := lsm.flushQueue.enqueue(mt); err != nil {
		mt.DecrRef()
		return err
	}
	return nil
}

func (lsm *LSM) startFlushWorkers(n int) {
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		lsm.flushWG.Go(func() {
			for {
				task, ok := lsm.flushQueue.next()
				if !ok {
					return
				}
				mt := task.memTable
				if mt == nil {
					lsm.flushQueue.markDone(task)
					continue
				}

				func() {
					defer mt.DecrRef()
					if err := lsm.levels.flush(mt); err != nil {
						lsm.flushQueue.markDone(task)
						return
					}
					lsm.flushQueue.markInstalled(task)
					lsm.lock.Lock()
					for idx, imm := range lsm.immutables {
						if imm == mt {
							lsm.immutables = append(lsm.immutables[:idx], lsm.immutables[idx+1:]...)
							break
						}
					}
					lsm.lock.Unlock()
					_ = mt.close()
					lsm.flushQueue.markDone(task)
				}()
			}
		})
	}
}
