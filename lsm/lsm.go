package lsm

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm/flush"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
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

	throttleFn func(bool)
	throttled  int32

	closed atomic.Bool
}

// Options _
type Options struct {
	WorkDir      string
	MemTableSize int64
	SSTableMaxSz int64
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64

	// Block cache controls.
	BlockCacheSize int
	BloomCacheSize int

	// compact
	NumCompactors       int
	BaseLevelSize       int64
	LevelSizeMultiplier int // 决定level之间期望的size比例
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int

	IngestCompactBatchSize  int
	IngestBacklogMergeScore float64
	IngestShardParallelism  int

	// CompactionValueWeight increases the priority of levels containing a high
	// proportion of ValueLog-backed payloads. Must be non-negative.
	CompactionValueWeight float64

	// CompactionValueAlertThreshold triggers stats alerts when value density
	// exceeds this ratio.
	CompactionValueAlertThreshold float64

	DiscardStatsCh *chan map[uint32]int64

	// HotKeyProvider optionally surfaces the hottest keys so compaction can
	// prioritise ranges with heavy access.
	HotKeyProvider func() [][]byte

	// ManifestSync controls whether manifest edits are fsynced immediately.
	ManifestSync bool
	// ManifestRewriteThreshold triggers a manifest rewrite when the manifest
	// grows beyond this size (bytes). Values <= 0 disable rewrites.
	ManifestRewriteThreshold int64
}

// Close  _
func (lsm *LSM) Close() error {
	if lsm == nil {
		return nil
	}
	if !lsm.closed.CompareAndSwap(false, true) {
		return nil
	}
	// wait for all api calls to finish
	lsm.throttleWrites(false)
	lsm.closer.Close()
	lsm.flushMgr.Close()
	lsm.flushWG.Wait()

	lsm.lock.Lock()
	mem := lsm.memTable
	immutables := append([]*memTable(nil), lsm.immutables...)
	lsm.memTable = nil
	lsm.immutables = nil
	lsm.lock.Unlock()

	if mem != nil {
		if err := mem.close(); err != nil {
			return err
		}
	}
	for _, mt := range immutables {
		if mt == nil {
			continue
		}
		if err := mt.close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	return nil
}

// SetDiscardStatsCh updates the discard stats channel used during compaction.
func (lsm *LSM) SetDiscardStatsCh(ch *chan map[uint32]int64) {
	lsm.option.DiscardStatsCh = ch
	if lsm.levels != nil {
		lsm.levels.opt.DiscardStatsCh = ch
	}
}

// SetHotKeyProvider wires a callback that returns currently hot keys so
// compaction can prioritise hot ranges.
func (lsm *LSM) SetHotKeyProvider(fn func() [][]byte) {
	if fn == nil {
		return
	}
	lsm.option.HotKeyProvider = fn
	if lsm.levels != nil {
		lsm.levels.setHotKeyProvider(fn)
	}
}

// SetThrottleCallback registers a callback used to toggle write throttling at the DB layer.
func (lsm *LSM) SetThrottleCallback(fn func(bool)) {
	lsm.throttleFn = fn
}

func (lsm *LSM) throttleWrites(on bool) {
	fn := lsm.throttleFn
	if fn == nil {
		return
	}
	if on {
		if atomic.CompareAndSwapInt32(&lsm.throttled, 0, 1) {
			fn(true)
		}
		return
	}
	if atomic.CompareAndSwapInt32(&lsm.throttled, 1, 0) {
		fn(false)
	}
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
func (lsm *LSM) LogValueLogDelete(fid uint32) error {
	return lsm.levels.LogValueLogDelete(fid)
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

// ValueLogHead returns the persisted head pointer, if any.
func (lsm *LSM) ValueLogHead() (kv.ValuePtr, bool) {
	meta := lsm.levels.ValueLogHead()
	if !meta.Valid {
		return kv.ValuePtr{}, false
	}
	return kv.ValuePtr{Fid: meta.FileID, Offset: uint32(meta.Offset)}, true
}

// ValueLogStatus returns manifest tracked value log metadata.
func (lsm *LSM) ValueLogStatus() map[uint32]manifest.ValueLogMeta {
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

// NewLSM _
func NewLSM(opt *Options, walMgr *wal.Manager) *LSM {
	if opt.CompactionValueWeight < 0 {
		opt.CompactionValueWeight = 0
	}
	if opt.CompactionValueWeight == 0 {
		opt.CompactionValueWeight = 0.35
	}
	if opt.CompactionValueAlertThreshold <= 0 {
		opt.CompactionValueAlertThreshold = 0.6
	}
	lsm := &LSM{option: opt, wal: walMgr}
	lsm.flushMgr = flush.NewManager()
	// initialize levelManager
	lsm.levels = lsm.initLevelManager(opt)
	// start the db recovery process to load the wal, if there is no recovery content, create a new memtable
	lsm.memTable, lsm.immutables = lsm.recovery()
	lsm.startFlushWorkers(1)
	for _, mt := range lsm.immutables {
		lsm.submitFlush(mt)
	}
	// initialize closer for resource recycling signal control
	lsm.closer = utils.NewCloser()
	return lsm
}

// StartCompacter _
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	if n <= 0 {
		n = 1
	}
	lsm.closer.Add(n)
	for i := 0; i < n; i++ {
		go lsm.levels.compaction.Start(i, lsm.closer.CloseSignal, lsm.closer.Done)
	}
}

// Set _
func (lsm *LSM) Set(entry *kv.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// graceful shutdown
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	// If the current memtable is full, rotate it under write lock; otherwise
	// hold the read lock while writing to prevent concurrent rotation.
	estimate := int64(kv.EstimateEncodeSize(entry))
	for {
		lsm.lock.RLock()
		mt := lsm.memTable
		if mt == nil {
			lsm.lock.RUnlock()
			return errors.New("lsm: memtable not initialized")
		}
		if atomic.LoadInt64(&mt.walSize)+estimate > lsm.option.MemTableSize {
			lsm.lock.RUnlock()
			var old *memTable
			lsm.lock.Lock()
			if lsm.memTable == mt && atomic.LoadInt64(&mt.walSize)+estimate > lsm.option.MemTableSize {
				old = lsm.rotateLocked()
			}
			lsm.lock.Unlock()
			if old != nil {
				lsm.submitFlush(old)
			}
			continue
		}
		err = mt.set(entry)
		lsm.lock.RUnlock()
		return err
	}
}

// Get _
func (lsm *LSM) Get(key []byte) (*kv.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	var (
		entry *kv.Entry
		err   error
	)
	lsm.lock.RLock()
	active := lsm.memTable
	immutables := append([]*memTable(nil), lsm.immutables...)
	lsm.lock.RUnlock()

	if active != nil {
		if entry, err = active.Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}

	for i := len(immutables) - 1; i >= 0; i-- {
		if entry, err = immutables[i].Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}
	// query from the level manager
	return lsm.levels.Get(key)
}

// Prefetch warms cache layers for the key by issuing targeted block loads.
func (lsm *LSM) Prefetch(key []byte, hot bool) {
	if len(key) == 0 {
		return
	}
	if lsm == nil || lsm.levels == nil {
		return
	}
	lsm.levels.prefetch(key, hot)
}

func (lsm *LSM) MemSize() int64 {
	return lsm.memTable.Size()
}

func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memTable == nil
}

func (lsm *LSM) GetSkipListFromMemTable() *utils.Skiplist {
	return lsm.memTable.sl
}

func (lsm *LSM) Rotate() {
	lsm.lock.Lock()
	old := lsm.rotateLocked()
	lsm.lock.Unlock()
	lsm.submitFlush(old)
}

func (lsm *LSM) rotateLocked() *memTable {
	old := lsm.memTable
	lsm.immutables = append(lsm.immutables, old)
	lsm.memTable = lsm.NewMemtable()
	return old
}

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

func (lsm *LSM) submitFlush(mt *memTable) {
	if mt == nil {
		return
	}
	mt.IncrRef()
	if _, err := lsm.flushMgr.Submit(&flush.Task{SegmentID: mt.segmentID, Data: mt}); err != nil {
		mt.DecrRef()
		utils.Panic(err)
	}
}

func (lsm *LSM) startFlushWorkers(n int) {
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		lsm.flushWG.Add(1)
		go func() {
			defer lsm.flushWG.Done()
			for {
				task, ok := lsm.flushMgr.Next()
				if !ok {
					return
				}
				mt, _ := task.Data.(*memTable)
				if mt == nil {
					if err := lsm.flushMgr.Update(task.ID, flush.StageRelease, nil, errors.New("nil memtable")); err != nil {
						_ = utils.Err(err)
					}
					continue
				}

				func() {
					defer mt.DecrRef()
					if err := lsm.levels.flush(mt); err != nil {
						if updateErr := lsm.flushMgr.Update(task.ID, flush.StageRelease, nil, err); updateErr != nil {
							_ = utils.Err(updateErr)
						}
						return
					}
					if updateErr := lsm.flushMgr.Update(task.ID, flush.StageInstall, nil, nil); updateErr != nil {
						_ = utils.Err(updateErr)
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
						_ = utils.Err(updateErr)
					}
				}()
			}
		}()
	}
}
