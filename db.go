// Package NoKV provides the embedded database API and engine wiring.
package NoKV

import (
	stderrors "errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/feichai0017/NoKV/wal"
)

// nonTxnMaxVersion is the sentinel MVCC version used by non-transactional APIs.
// Non-transactional reads/writes must not be mixed with MVCC/Txn writes.
const nonTxnMaxVersion = math.MaxUint64

type (
	// CoreAPI describes the externally exposed NoKV operations.
	CoreAPI interface {
		Set(key, value []byte) error
		Get(key []byte) (*kv.Entry, error)
		Del(key []byte) error
		SetCF(cf kv.ColumnFamily, key, value []byte) error
		GetCF(cf kv.ColumnFamily, key []byte) (*kv.Entry, error)
		DelCF(cf kv.ColumnFamily, key []byte) error
		NewIterator(opt *utils.Options) utils.Iterator
		Info() *Stats
		Close() error
	}

	// DB is the global handle for the engine and owns shared resources.
	DB struct {
		sync.RWMutex
		opt              *Options
		fs               vfs.FS
		dirLock          *utils.DirLock
		lsm              *lsm.LSM
		wal              *wal.Manager
		walWatchdog      *wal.Watchdog
		vlog             *valueLog
		stats            *Stats
		blockWrites      int32
		vheads           map[uint32]kv.ValuePtr
		lastLoggedHeads  map[uint32]kv.ValuePtr
		headLogDelta     uint32
		isClosed         uint32
		closeOnce        sync.Once
		closeErr         error
		orc              *oracle
		hotRead          hotTracker
		hotWrite         hotTracker
		writeMetrics     *metrics.WriteMetrics
		commitQueue      commitQueue
		commitWG         sync.WaitGroup
		commitBatchPool  sync.Pool
		iterPool         *iteratorPool
		prefetchRing     *utils.Ring[prefetchRequest]
		prefetchItems    chan struct{}
		prefetchWG       sync.WaitGroup
		prefetchState    atomic.Pointer[prefetchState]
		prefetchWarm     int32
		prefetchHot      int32
		prefetchCooldown time.Duration
		cfMetrics        []*cfCounters
		hotWriteLimited  uint64
	}

	commitQueue struct {
		ring           *utils.Ring[*commitRequest]
		items          chan struct{}
		spaces         chan struct{}
		closeCh        chan struct{}
		queueLen       int64
		inflight       int64
		pendingBytes   int64
		pendingEntries int64
		closed         uint32
	}

	commitRequest struct {
		req        *request
		entryCount int
		size       int64
		hot        bool
	}

	commitBatch struct {
		reqs        []*commitRequest
		pool        *[]*commitRequest
		requests    []*request
		batchStart  time.Time
		valueLogDur time.Duration
	}
)

type cfCounters struct {
	writes uint64
	reads  uint64
}

// Open DB
func Open(opt *Options) *DB {
	db := &DB{opt: opt, writeMetrics: metrics.NewWriteMetrics()}
	db.fs = vfs.Ensure(opt.FS)
	db.headLogDelta = valueLogHeadLogInterval
	db.initWriteBatchOptions()
	db.commitBatchPool.New = func() any {
		batch := make([]*commitRequest, 0, db.opt.WriteBatchMaxCount)
		return &batch
	}

	if db.opt.BlockCacheSize < 0 {
		db.opt.BlockCacheSize = 0
	}
	if db.opt.BloomCacheSize < 0 {
		db.opt.BloomCacheSize = 0
	}

	lock, err := utils.AcquireDirLock(opt.WorkDir, db.fs)
	utils.Panic(err)
	db.dirLock = lock

	utils.Panic(db.runRecoveryChecks())

	wlog, err := wal.Open(wal.Config{
		Dir:         opt.WorkDir,
		SyncOnWrite: false,
		FS:          db.fs,
	})
	utils.Panic(err)
	db.wal = wlog

	numCompactors := opt.NumCompactors
	if numCompactors <= 0 {
		cpu := runtime.NumCPU()
		if cpu <= 1 {
			numCompactors = 1
		} else {
			numCompactors = min(max(cpu/2, 2), 8)
		}
	}
	numL0Tables := opt.NumLevelZeroTables
	if numL0Tables <= 0 {
		numL0Tables = 15
	}
	ingestBatchSize := opt.IngestCompactBatchSize
	if ingestBatchSize <= 0 {
		ingestBatchSize = 8
	}
	mergeScore := opt.IngestBacklogMergeScore
	if mergeScore <= 0 {
		mergeScore = 2.0
	}
	shardParallel := opt.IngestShardParallelism
	if shardParallel <= 0 {
		shardParallel = max(numCompactors/2, 2)
	}
	baseTableSize := opt.MemTableSize
	if baseTableSize <= 0 {
		baseTableSize = 8 << 20
	}
	if baseTableSize < 8<<20 {
		baseTableSize = 8 << 20
	}
	if opt.SSTableMaxSz > 0 && baseTableSize > opt.SSTableMaxSz {
		baseTableSize = opt.SSTableMaxSz
	}
	baseLevelSize := baseTableSize * 4
	if baseLevelSize < 32<<20 {
		baseLevelSize = 32 << 20
	}
	// Initialize the LSM tree.
	db.lsm = lsm.NewLSM(&lsm.Options{
		FS:                       db.fs,
		WorkDir:                  opt.WorkDir,
		MemTableSize:             opt.MemTableSize,
		MemTableEngine:           string(opt.MemTableEngine),
		SSTableMaxSz:             opt.SSTableMaxSz,
		BlockSize:                8 * 1024,
		BloomFalsePositive:       0.01,
		BaseLevelSize:            baseLevelSize,
		LevelSizeMultiplier:      8,
		BaseTableSize:            baseTableSize,
		TableSizeMultiplier:      2,
		NumLevelZeroTables:       numL0Tables,
		MaxLevelNum:              utils.MaxLevelNum,
		NumCompactors:            numCompactors,
		IngestCompactBatchSize:   ingestBatchSize,
		IngestBacklogMergeScore:  mergeScore,
		IngestShardParallelism:   shardParallel,
		CompactionValueWeight:    db.opt.CompactionValueWeight,
		BlockCacheSize:           db.opt.BlockCacheSize,
		BloomCacheSize:           db.opt.BloomCacheSize,
		ManifestSync:             db.opt.ManifestSync,
		ManifestRewriteThreshold: db.opt.ManifestRewriteThreshold,
	}, wlog)
	db.lsm.SetThrottleCallback(db.applyThrottle)
	recoveredVersion := db.lsm.MaxVersion()
	db.iterPool = newIteratorPool()
	cfCount := int(kv.CFWrite) + 1
	db.cfMetrics = make([]*cfCounters, cfCount)
	for i := range db.cfMetrics {
		db.cfMetrics[i] = &cfCounters{}
	}
	// Initialize the value log.
	db.initVLog()
	db.lsm.SetDiscardStatsCh(&(db.vlog.lfDiscardStats.flushChan))
	// Initialize stats tracking.
	db.stats = newStats(db)

	db.hotRead = newHotTracker(opt)
	db.hotWrite = newHotTrackerForWrite(opt)
	if db.hotRead != nil {
		if opt.HotRingTopK <= 0 {
			opt.HotRingTopK = 16
		}
		db.prefetchWarm = 4
		db.prefetchHot = 16
		if db.prefetchHot <= db.prefetchWarm {
			db.prefetchHot = db.prefetchWarm + 4
		}
		db.prefetchCooldown = 15 * time.Second
		db.prefetchRing = utils.NewRing[prefetchRequest](256)
		db.prefetchItems = make(chan struct{}, db.prefetchRing.Cap())
		db.prefetchState.Store(&prefetchState{
			pend:       make(map[string]struct{}),
			prefetched: make(map[string]time.Time),
		})
		db.prefetchWG.Add(1)
		go db.prefetchLoop()
		db.lsm.SetHotKeyProvider(func() [][]byte {
			if db.hotRead == nil {
				return nil
			}
			top := db.hotRead.TopN(opt.HotRingTopK)
			if len(top) == 0 {
				return nil
			}
			keys := make([][]byte, 0, len(top))
			for _, item := range top {
				if item.Key == "" {
					continue
				}
				keys = append(keys, []byte(item.Key))
			}
			return keys
		})
	}

	db.orc = newOracle(*opt)
	db.orc.initCommitState(recoveredVersion)
	// Start the SSTable compaction loop.
	db.lsm.StartCompacter()
	// Initialize the commit queue and GC plumbing.
	queueCap := max(opt.WriteBatchMaxCount*8, 1024)
	db.commitQueue.init(queueCap)
	db.commitWG.Add(1)
	go db.commitWorker()
	if db.opt.EnableWALWatchdog {
		db.walWatchdog = wal.NewWatchdog(wal.WatchdogConfig{
			Manager:      db.wal,
			Interval:     db.opt.WALAutoGCInterval,
			MinRemovable: db.opt.WALAutoGCMinRemovable,
			MaxBatch:     db.opt.WALAutoGCMaxBatch,
			WarnRatio:    db.opt.WALTypedRecordWarnRatio,
			WarnSegments: db.opt.WALTypedRecordWarnSegments,
			RaftPointers: func() map[uint64]manifest.RaftLogPointer {
				if man := db.Manifest(); man != nil {
					return man.RaftPointerSnapshot()
				}
				return nil
			},
		})
		if db.walWatchdog != nil {
			db.walWatchdog.Start()
		}
	}
	// Start periodic stats collection.
	db.stats.StartStats()
	if db.opt.ValueLogGCInterval > 0 {
		if db.vlog != nil && db.vlog.lfDiscardStats != nil && db.vlog.lfDiscardStats.closer != nil {
			db.vlog.lfDiscardStats.closer.Add(1)
			go db.runValueLogGCPeriodically()
		}
	}
	return db
}

func (db *DB) runRecoveryChecks() error {
	if db == nil || db.opt == nil {
		return fmt.Errorf("recovery checks: options not initialized")
	}
	if err := manifest.Verify(db.opt.WorkDir, db.fs); err != nil {
		if !stderrors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := wal.VerifyDir(db.opt.WorkDir, db.fs); err != nil {
		return err
	}
	vlogDir := filepath.Join(db.opt.WorkDir, "vlog")
	bucketCount := db.opt.ValueLogBucketCount
	if bucketCount <= 1 {
		bucketCount = 1
	}
	for bucket := 0; bucket < bucketCount; bucket++ {
		cfg := vlogpkg.Config{
			Dir:      filepath.Join(vlogDir, fmt.Sprintf("bucket-%03d", bucket)),
			FileMode: utils.DefaultFileMode,
			MaxSize:  int64(db.opt.ValueLogFileSize),
			Bucket:   uint32(bucket),
			FS:       db.fs,
		}
		if err := vlogpkg.VerifyDir(cfg); err != nil {
			if !stderrors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

// Close stops background workers and flushes in-memory state before releasing all resources.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.closeOnce.Do(func() {
		db.closeErr = db.closeInternal()
	})
	return db.closeErr
}

// closeInternal executes DB shutdown exactly once and aggregates non-fatal
// close failures so callers can observe every resource teardown error.
func (db *DB) closeInternal() error {
	if db == nil {
		return nil
	}

	if db.IsClosed() {
		return nil
	}

	if vlog := db.vlog; vlog != nil && vlog.lfDiscardStats != nil && vlog.lfDiscardStats.closer != nil {
		vlog.lfDiscardStats.closer.Close()
	}

	db.stopCommitWorkers()

	var errs []error
	if err := db.stats.close(); err != nil {
		errs = append(errs, fmt.Errorf("stats close: %w", err))
	}

	if db.walWatchdog != nil {
		db.walWatchdog.Stop()
		db.walWatchdog = nil
	}

	if db.hotRead != nil {
		db.hotRead.Close()
	}
	if db.hotWrite != nil {
		db.hotWrite.Close()
	}

	if db.prefetchRing != nil {
		db.prefetchRing.Close()
		if db.prefetchItems != nil {
			select {
			case db.prefetchItems <- struct{}{}:
			default:
			}
		}
		db.prefetchWG.Wait()
		db.prefetchRing = nil
		db.prefetchItems = nil
	}

	if err := db.lsm.Close(); err != nil {
		errs = append(errs, fmt.Errorf("lsm close: %w", err))
	}

	if err := db.vlog.close(); err != nil {
		errs = append(errs, fmt.Errorf("vlog close: %w", err))
	}

	if err := db.wal.Close(); err != nil {
		errs = append(errs, fmt.Errorf("wal close: %w", err))
	}

	if db.dirLock != nil {
		if err := db.dirLock.Release(); err != nil {
			errs = append(errs, fmt.Errorf("dir lock release: %w", err))
		}
		db.dirLock = nil
	}

	atomic.StoreUint32(&db.isClosed, 1)

	if len(errs) > 0 {
		return stderrors.Join(errs...)
	}

	return nil
}

// Del removes a key from the default column family by writing a tombstone.
func (db *DB) Del(key []byte) error {
	return db.DelCF(kv.CFDefault, key)
}

// DelCF deletes a key from the specified column family.
func (db *DB) DelCF(cf kv.ColumnFamily, key []byte) error {
	if len(key) == 0 {
		return utils.ErrEmptyKey
	}
	entry := kv.NewEntryWithCF(cf, key, nil)
	entry.Meta = kv.BitDelete
	defer entry.DecrRef()
	return db.setEntry(entry)
}

// Set writes a key/value pair into the default column family.
func (db *DB) Set(key, value []byte) error {
	return db.SetCF(kv.CFDefault, key, value)
}

// SetCF writes a key/value pair into the specified column family.
func (db *DB) SetCF(cf kv.ColumnFamily, key, value []byte) error {
	// Non-transactional API: do not mix with MVCC/Txn writes.
	if len(key) == 0 {
		return utils.ErrEmptyKey
	}
	data := kv.NewEntryWithCF(cf, key, value)
	if value == nil {
		data.Meta = kv.BitDelete
	}
	defer data.DecrRef()
	return db.setEntry(data)
}

// setEntry persists an entry using the non-transactional write path.
func (db *DB) setEntry(data *kv.Entry) error {
	if data == nil || len(data.Key) == 0 {
		return utils.ErrEmptyKey
	}
	if !data.CF.Valid() {
		data.CF = kv.CFDefault
	}
	if err := db.maybeThrottleWrite(data.CF, data.Key); err != nil {
		return err
	}
	// Internal keys include CF and version; non-transactional writes use max version.
	data.Key = kv.InternalKey(data.CF, data.Key, nonTxnMaxVersion)

	// Delegate to the commit pipeline to leverage batching and VLog offloading.
	data.IncrRef()
	if err := db.batchSet([]*kv.Entry{data}); err != nil {
		data.DecrRef()
		return err
	}
	return nil
}

// SetVersionedEntry writes a value to the specified column family using the
// provided version. It mirrors SetCF but allows callers to control the MVCC
// timestamp embedded in the internal key.
func (db *DB) SetVersionedEntry(cf kv.ColumnFamily, key []byte, version uint64, value []byte, meta byte) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if len(key) == 0 {
		return utils.ErrEmptyKey
	}
	entry := kv.NewEntryWithCF(cf, kv.SafeCopy(nil, key), kv.SafeCopy(nil, value))
	entry.Meta = meta
	defer entry.DecrRef()

	if err := db.maybeThrottleWrite(entry.CF, entry.Key); err != nil {
		return err
	}

	entry.Key = kv.InternalKey(entry.CF, entry.Key, version)

	// Delegate to the commit pipeline to leverage batching and VLog offloading.
	entry.IncrRef()
	if err := db.batchSet([]*kv.Entry{entry}); err != nil {
		entry.DecrRef()
		return err
	}
	return nil
}

// DeleteVersionedEntry marks the specified version as deleted by writing a
// tombstone record.
func (db *DB) DeleteVersionedEntry(cf kv.ColumnFamily, key []byte, version uint64) error {
	return db.SetVersionedEntry(cf, key, version, nil, kv.BitDelete)
}

// GetVersionedEntry retrieves the value stored at the provided MVCC version.
// The returned entry is detached from internal pools and does not require DecrRef.
func (db *DB) GetVersionedEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	internalKey := kv.InternalKey(cf, key, version)
	entry, err := db.loadBorrowedEntry(internalKey)
	if err != nil {
		return nil, err
	}
	defer entry.DecrRef()
	return cloneEntry(entry, cf), nil
}

// Get reads the latest visible value for key from the default column family.
func (db *DB) Get(key []byte) (*kv.Entry, error) {
	return db.GetCF(kv.CFDefault, key)
}

// GetCF reads a key from the specified column family.
// The returned entry is detached from internal pools and does not require DecrRef.
func (db *DB) GetCF(cf kv.ColumnFamily, key []byte) (*kv.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	// Non-transactional API: use the max sentinel timestamp (not for MVCC).
	internalKey := kv.InternalKey(cf, key, nonTxnMaxVersion)
	entry, err := db.loadBorrowedEntry(internalKey)
	if err != nil {
		return nil, err
	}
	defer entry.DecrRef()
	if isDeletedOrExpired(entry.Meta, entry.ExpiresAt) {
		return nil, utils.ErrKeyNotFound
	}
	out := cloneEntry(entry, cf)
	db.recordCFRead(out.CF, 1)
	db.recordRead(out.Key)
	return out, nil
}

// loadBorrowedEntry fetches one internal-key record from LSM and resolves value-log
// indirection before returning it to the caller.
//
// Ownership contract:
//   - The returned entry is a borrowed, pool-managed object.
//   - The caller MUST call DecrRef exactly once when finished.
//
// Error behavior:
//   - Returns ErrKeyNotFound when no record exists.
//   - If vlog pointer resolution fails, this function releases the borrowed entry
//     before returning the error to avoid leaking ref-counted entries.
func (db *DB) loadBorrowedEntry(internalKey []byte) (*kv.Entry, error) {
	entry, err := db.lsm.Get(internalKey)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, utils.ErrKeyNotFound
	}
	if !kv.IsValuePtr(entry) {
		return entry, nil
	}
	var vp kv.ValuePtr
	vp.Decode(entry.Value)
	result, cb, readErr := db.vlog.read(&vp)
	if cb != nil {
		defer kv.RunCallback(cb)
	}
	if readErr != nil {
		entry.DecrRef()
		return nil, readErr
	}
	entry.Value = kv.SafeCopy(nil, result)
	entry.Meta &^= kv.BitValuePointer
	return entry, nil
}

// cloneEntry converts an internal/buffered entry into a detached public value object.
//
// It deep-copies key/value bytes so the returned entry is independent from pooled
// memory, parses internal key layout (CF/user-key/version), and fills external-facing
// metadata. The returned entry does not participate in ref-count lifecycle and does
// not require DecrRef from API callers.
func cloneEntry(src *kv.Entry, fallbackCF kv.ColumnFamily) *kv.Entry {
	if src == nil {
		return nil
	}
	cf := fallbackCF
	if !cf.Valid() {
		cf = kv.CFDefault
	}
	userKeySrc := src.Key
	version := src.Version
	if storedCF, parsedUserKey, ts := kv.SplitInternalKey(src.Key); storedCF.Valid() {
		cf = storedCF
		userKeySrc = parsedUserKey
		if ts != 0 {
			version = ts
		}
	}
	return &kv.Entry{
		Key:          kv.SafeCopy(nil, userKeySrc),
		Value:        kv.SafeCopy(nil, src.Value),
		ExpiresAt:    src.ExpiresAt,
		CF:           cf,
		Meta:         src.Meta,
		Version:      version,
		Offset:       src.Offset,
		Hlen:         src.Hlen,
		ValThreshold: src.ValThreshold,
	}
}

func isDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&kv.BitDelete > 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

// Info returns the live stats collector for snapshot/diagnostic access.
func (db *DB) Info() *Stats {
	// Return the current stats snapshot.
	return db.stats
}

// RunValueLogGC triggers a value log garbage collection.
func (db *DB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return utils.ErrInvalidRequest
	}
	heads := db.lsm.ValueLogHead()
	if len(heads) == 0 {
		db.RLock()
		if len(db.vheads) > 0 {
			heads = make(map[uint32]kv.ValuePtr, len(db.vheads))
			maps.Copy(heads, db.vheads)
		}
		db.RUnlock()
	}
	if len(heads) == 0 && db.vlog != nil {
		heads = make(map[uint32]kv.ValuePtr)
		for bucket, mgr := range db.vlog.managers {
			if mgr == nil {
				continue
			}
			heads[uint32(bucket)] = mgr.Head()
		}
	}
	// Pick a log file and run GC
	if err := db.vlog.runGC(discardRatio, heads); err != nil {
		if stderrors.Is(err, utils.ErrEmptyKey) {
			return nil
		}
		return err
	}
	return nil
}

func (db *DB) runValueLogGCPeriodically() {
	if db.vlog == nil || db.vlog.lfDiscardStats == nil || db.vlog.lfDiscardStats.closer == nil {
		return
	}
	defer db.vlog.lfDiscardStats.closer.Done()

	ticker := time.NewTicker(db.opt.ValueLogGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := db.RunValueLogGC(db.opt.ValueLogGCDiscardRatio)
			if err != nil {
				if err == utils.ErrNoRewrite {
					db.vlog.logf("No rewrite on GC.")
				} else {
					_ = utils.Err(err)
				}
			}
		case <-db.vlog.lfDiscardStats.closer.CloseSignal:
			return
		}
	}
}

func (db *DB) shouldWriteValueToLSM(e *kv.Entry) bool {
	return int64(len(e.Value)) < db.opt.ValueThreshold
}

func (db *DB) valueThreshold() int64 {
	return atomic.LoadInt64(&db.opt.ValueThreshold)
}

// SetRegionMetrics attaches region metrics recorder so Stats snapshot and expvar
// include region state counts.
func (db *DB) SetRegionMetrics(rm *metrics.RegionMetrics) {
	if db == nil {
		return
	}
	if db.stats != nil {
		db.stats.SetRegionMetrics(rm)
	}
}

// WAL exposes the underlying WAL manager.
func (db *DB) WAL() *wal.Manager {
	if db == nil {
		return nil
	}
	return db.wal
}

// Manifest exposes the manifest manager for coordination components.
func (db *DB) Manifest() *manifest.Manager {
	if db == nil || db.lsm == nil {
		return nil
	}
	return db.lsm.ManifestManager()
}

// IsClosed reports whether Close has finished and the DB no longer accepts work.
func (db *DB) IsClosed() bool {
	return atomic.LoadUint32(&db.isClosed) == 1
}

func (db *DB) cfCounter(cf kv.ColumnFamily) *cfCounters {
	if db == nil {
		return nil
	}
	if !cf.Valid() {
		cf = kv.CFDefault
	}
	idx := int(cf)
	if idx < 0 || idx >= len(db.cfMetrics) {
		idx = int(kv.CFDefault)
	}
	if db.cfMetrics[idx] == nil {
		db.cfMetrics[idx] = &cfCounters{}
	}
	return db.cfMetrics[idx]
}

func (db *DB) recordCFWrite(cf kv.ColumnFamily, delta uint64) {
	if cnt := db.cfCounter(cf); cnt != nil {
		atomic.AddUint64(&cnt.writes, delta)
	}
}

func (db *DB) recordCFRead(cf kv.ColumnFamily, delta uint64) {
	if cnt := db.cfCounter(cf); cnt != nil {
		atomic.AddUint64(&cnt.reads, delta)
	}
}

func (db *DB) columnFamilyStats() map[string]ColumnFamilySnapshot {
	stats := make(map[string]ColumnFamilySnapshot)
	if db == nil {
		return stats
	}
	limit := int(kv.CFWrite) + 1
	for idx := 0; idx < limit && idx < len(db.cfMetrics); idx++ {
		cnt := db.cfMetrics[idx]
		if cnt == nil {
			continue
		}
		writes := atomic.LoadUint64(&cnt.writes)
		reads := atomic.LoadUint64(&cnt.reads)
		if writes == 0 && reads == 0 {
			continue
		}
		cfName := kv.ColumnFamily(idx).String()
		stats[cfName] = ColumnFamilySnapshot{Writes: writes, Reads: reads}
	}
	return stats
}
