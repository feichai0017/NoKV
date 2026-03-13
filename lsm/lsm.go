package lsm

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm/flush"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
	"github.com/feichai0017/NoKV/wal"
)

// LSM _
type LSM struct {
	lock       sync.RWMutex
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
	wal        *wal.Manager
	flushMgr   *flush.Manager
	flushWG    sync.WaitGroup
	logger     *slog.Logger

	runtimeMu      sync.RWMutex
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

// Options _
type Options struct {
	// FS provides the filesystem implementation for manifest operations.
	FS vfs.FS
	// Logger handles background/storage logs for the LSM subsystem.
	Logger *slog.Logger

	WorkDir        string
	MemTableSize   int64
	MemTableEngine string
	SSTableMaxSz   int64
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64

	// Cache budgets. Zero disables the corresponding user-space cache.
	BlockCacheBytes int64
	IndexCacheBytes int64
	BloomCacheBytes int64

	// compact
	NumCompactors       int
	CompactionPolicy    string
	BaseLevelSize       int64
	LevelSizeMultiplier int // Target size ratio between levels.
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int
	// L0SlowdownWritesTrigger starts write pacing when L0 table count reaches
	// this threshold. Values <= 0 disable L0-based slowdown.
	L0SlowdownWritesTrigger int
	// L0StopWritesTrigger blocks writes when L0 table count reaches this
	// threshold. Values <= 0 disable L0-based hard stop.
	L0StopWritesTrigger int
	// L0ResumeWritesTrigger clears slowdown/stop only when L0 table count drops
	// to this threshold or lower, providing hysteresis and reducing oscillation.
	L0ResumeWritesTrigger int
	// CompactionSlowdownTrigger starts write pacing when max compaction score
	// reaches this value. Values <= 0 disable score-based slowdown.
	CompactionSlowdownTrigger float64
	// CompactionStopTrigger blocks writes when max compaction score reaches this
	// value. Values <= 0 disable score-based hard stop.
	CompactionStopTrigger float64
	// CompactionResumeTrigger clears throttling only when max compaction score
	// drops to this value or lower, providing hysteresis.
	CompactionResumeTrigger float64
	// WriteThrottleMinRate is the target write admission rate in bytes/sec when
	// slowdown pressure approaches the stop threshold.
	WriteThrottleMinRate int64
	// WriteThrottleMaxRate is the target write admission rate in bytes/sec when
	// slowdown first becomes active.
	WriteThrottleMaxRate int64

	IngestCompactBatchSize  int
	IngestBacklogMergeScore float64
	IngestShardParallelism  int

	// CompactionValueWeight increases the priority of levels containing a high
	// proportion of ValueLog-backed payloads. Must be non-negative.
	CompactionValueWeight float64

	// CompactionValueAlertThreshold triggers stats alerts when value density
	// exceeds this ratio.
	CompactionValueAlertThreshold float64

	DiscardStatsCh *chan map[manifest.ValueLogID]int64

	// HotKeyProvider optionally surfaces hottest keys as InternalKey values so
	// compaction can prioritise ranges with heavy access.
	HotKeyProvider func() [][]byte

	// ManifestSync controls whether manifest edits are fsynced immediately.
	ManifestSync bool
	// ManifestRewriteThreshold triggers a manifest rewrite when the manifest
	// grows beyond this size (bytes). Values <= 0 disable rewrites.
	ManifestRewriteThreshold int64

	// WALGCPolicy controls whether old WAL segments can be deleted.
	// Nil defaults to AllowAllWALGCPolicy.
	WALGCPolicy WALGCPolicy
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
	lsm.throttleWrites(WriteThrottleNone, 0, 0)
	if lsm.closer != nil {
		lsm.closer.Close()
	}
	if lsm.flushMgr != nil {
		closeErr = errors.Join(closeErr, lsm.flushMgr.Close())
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

// SetDiscardStatsCh updates the discard stats channel used during compaction.
func (lsm *LSM) SetDiscardStatsCh(ch *chan map[manifest.ValueLogID]int64) {
	if lsm == nil {
		return
	}
	var resolved chan map[manifest.ValueLogID]int64
	if ch != nil {
		resolved = *ch
	}
	lsm.runtimeMu.Lock()
	lsm.discardStatsCh = resolved
	lsm.runtimeMu.Unlock()
}

// SetHotKeyProvider wires a callback that returns currently hot keys as
// InternalKey values so compaction can prioritise hot ranges.
func (lsm *LSM) SetHotKeyProvider(fn func() [][]byte) {
	if lsm == nil {
		return
	}
	if fn == nil {
		return
	}
	if lsm.levels != nil {
		lsm.levels.setHotKeyProvider(fn)
	}
}

// SetWALGCPolicy updates the WAL segment-GC strategy used by LSM recovery.
func (lsm *LSM) SetWALGCPolicy(policy WALGCPolicy) {
	if lsm == nil {
		return
	}
	lsm.runtimeMu.Lock()
	lsm.walGCPolicy = normalizeWALGCPolicy(policy)
	lsm.runtimeMu.Unlock()
}

func (lsm *LSM) getDiscardStatsCh() chan map[manifest.ValueLogID]int64 {
	if lsm == nil {
		return nil
	}
	lsm.runtimeMu.RLock()
	ch := lsm.discardStatsCh
	lsm.runtimeMu.RUnlock()
	return ch
}

func (lsm *LSM) getWALGCPolicy() WALGCPolicy {
	if lsm == nil {
		return AllowAllWALGCPolicy{}
	}
	lsm.runtimeMu.RLock()
	policy := lsm.walGCPolicy
	lsm.runtimeMu.RUnlock()
	return normalizeWALGCPolicy(policy)
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

// SetThrottleCallback registers the DB-layer callback for write admission changes.
func (lsm *LSM) SetThrottleCallback(fn func(WriteThrottleState)) {
	lsm.throttleFn = fn
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
	if lsm == nil || lsm.flushMgr == nil {
		return 0
	}
	return lsm.flushMgr.Stats().Pending
}

// FlushMetrics returns detailed flush manager statistics.
func (lsm *LSM) FlushMetrics() flush.Metrics {
	if lsm == nil || lsm.flushMgr == nil {
		return flush.Metrics{}
	}
	return lsm.flushMgr.Stats()
}

// CompactionStats returns (#pending candidates, max adjusted score).
func (lsm *LSM) CompactionStats() (int64, float64) {
	if lsm == nil || lsm.levels == nil {
		return 0, 0
	}
	return lsm.levels.compactionStats()
}

// CompactionDurations returns the last and max compaction durations (ms) and run count.
func (lsm *LSM) CompactionDurations() (float64, float64, uint64) {
	if lsm == nil || lsm.levels == nil {
		return 0, 0, 0
	}
	return lsm.levels.compactionDurations()
}

// LevelMetrics returns aggregated statistics per LSM level.
func (lsm *LSM) LevelMetrics() []LevelMetrics {
	if lsm == nil || lsm.levels == nil {
		return nil
	}
	return lsm.levels.levelMetricsSnapshot()
}

// CompactionValueWeight returns the current compaction value weighting factor.
func (lsm *LSM) CompactionValueWeight() float64 {
	if lsm == nil || lsm.option == nil {
		return 0
	}
	return lsm.option.CompactionValueWeight
}

// CompactionValueAlertThreshold returns the alert threshold for value density.
func (lsm *LSM) CompactionValueAlertThreshold() float64 {
	if lsm == nil || lsm.option == nil {
		return 0.6
	}
	return lsm.option.CompactionValueAlertThreshold
}

// CacheMetrics returns read-path cache hit statistics.
func (lsm *LSM) CacheMetrics() CacheMetrics {
	if lsm == nil || lsm.levels == nil {
		return CacheMetrics{}
	}
	return lsm.levels.cacheMetrics()
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
	return lsm.levels.LogValueLogHead(ptr)
}

// LogValueLogDelete records removal of a value log segment.
func (lsm *LSM) LogValueLogDelete(bucket uint32, fid uint32) error {
	return lsm.levels.LogValueLogDelete(bucket, fid)
}

// LogValueLogUpdate restores or amends metadata for a value log segment.
func (lsm *LSM) LogValueLogUpdate(meta *manifest.ValueLogMeta) error {
	if lsm.levels == nil {
		return nil
	}
	return lsm.levels.LogValueLogUpdate(meta)
}

// ManifestManager exposes the underlying manifest manager for advanced coordination layers.
func (lsm *LSM) ManifestManager() *manifest.Manager {
	if lsm == nil || lsm.levels == nil {
		return nil
	}
	return lsm.levels.manifestMgr
}

// ValueLogHead returns persisted head pointers keyed by bucket.
func (lsm *LSM) ValueLogHead() map[uint32]kv.ValuePtr {
	heads := lsm.levels.ValueLogHead()
	if len(heads) == 0 {
		return nil
	}
	out := make(map[uint32]kv.ValuePtr, len(heads))
	for bucket, meta := range heads {
		if !meta.Valid {
			continue
		}
		out[bucket] = kv.ValuePtr{
			Bucket: bucket,
			Fid:    meta.FileID,
			Offset: uint32(meta.Offset),
		}
	}
	return out
}

// ValueLogStatus returns manifest tracked value log metadata.
func (lsm *LSM) ValueLogStatus() map[manifest.ValueLogID]manifest.ValueLogMeta {
	if lsm.levels == nil {
		return nil
	}
	return lsm.levels.ValueLogStatus()
}

// CurrentVersion returns a snapshot of manifest version state.
func (lsm *LSM) CurrentVersion() manifest.Version {
	if lsm.levels == nil || lsm.levels.manifestMgr == nil {
		return manifest.Version{}
	}
	return lsm.levels.manifestMgr.Current()
}

// NewLSM constructs the LSM core and returns initialization errors.
func NewLSM(opt *Options, walMgr *wal.Manager) (*LSM, error) {
	if opt == nil {
		return nil, errors.New("lsm: nil options")
	}
	if walMgr == nil {
		return nil, errors.New("lsm: nil wal manager")
	}
	frozen := opt.normalized()
	if frozen == nil {
		return nil, errors.New("lsm: nil cloned options")
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
	lsm.flushMgr = flush.NewManager()
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
		go lsm.levels.compaction.Start(i, lsm.closer.CloseSignal, lsm.closer.Done)
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
			return errors.New("lsm: memtable not initialized")
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
			return errors.New("lsm: memtable not initialized")
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
	tables, release := lsm.GetMemTables()
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
	// query from the level manager
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

// Prefetch warms cache layers for the key by issuing targeted block loads.
// key must be an InternalKey.
func (lsm *LSM) Prefetch(key []byte) {
	if len(key) == 0 {
		return
	}
	if lsm == nil || lsm.levels == nil {
		return
	}
	lsm.levels.prefetch(key)
}

// MemSize returns the current active memtable memory usage.
func (lsm *LSM) MemSize() int64 {
	return lsm.memTable.Size()
}

// MemTableIsNil reports whether the active memtable pointer is unset.
func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memTable == nil
}

// GetSkipListFromMemTable exposes the active memtable skiplist when that engine is used.
func (lsm *LSM) GetSkipListFromMemTable() *utils.Skiplist {
	if lsm == nil || lsm.memTable == nil || lsm.memTable.index == nil {
		return nil
	}
	if sl, ok := lsm.memTable.index.(*utils.Skiplist); ok {
		return sl
	}
	return nil
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

// GetMemTables pins active+immutable memtables and returns an unlock callback.
func (lsm *LSM) GetMemTables() ([]*memTable, func()) {
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
	if _, err := lsm.flushMgr.Submit(&flush.Task{SegmentID: mt.segmentID, Data: mt}); err != nil {
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
				task, ok := lsm.flushMgr.Next()
				if !ok {
					return
				}
				mt, _ := task.Data.(*memTable)
				if mt == nil {
					if err := lsm.flushMgr.Update(task.ID, flush.StageRelease, nil, errors.New("nil memtable")); err != nil {
						lsm.getLogger().Error("flush task update", "error", err)
					}
					continue
				}

				func() {
					defer mt.DecrRef()
					if err := lsm.levels.flush(mt); err != nil {
						if updateErr := lsm.flushMgr.Update(task.ID, flush.StageRelease, nil, err); updateErr != nil {
							lsm.getLogger().Error("flush task update", "error", updateErr)
						}
						return
					}
					if updateErr := lsm.flushMgr.Update(task.ID, flush.StageInstall, nil, nil); updateErr != nil {
						lsm.getLogger().Error("flush task update", "error", updateErr)
					}
					lsm.lock.Lock()
					for idx, imm := range lsm.immutables {
						if imm == mt {
							lsm.immutables = append(lsm.immutables[:idx], lsm.immutables[idx+1:]...)
							break
						}
					}
					lsm.lock.Unlock()
					_ = mt.close()
					if updateErr := lsm.flushMgr.Update(task.ID, flush.StageRelease, nil, nil); updateErr != nil {
						lsm.getLogger().Error("flush task update", "error", updateErr)
					}
				}()
			}
		})
	}
}
