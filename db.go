// Package NoKV provides the embedded database API and engine wiring.
package NoKV

import (
	stderrors "errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/hotring"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/utils"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/feichai0017/NoKV/wal"
)

type (
	// NoKV对外提供的功能集合
	CoreAPI interface {
		Set(data *kv.Entry) error
		Get(key []byte) (*kv.Entry, error)
		Del(key []byte) error
		SetCF(cf kv.ColumnFamily, key, value []byte) error
		GetCF(cf kv.ColumnFamily, key []byte) (*kv.Entry, error)
		DelCF(cf kv.ColumnFamily, key []byte) error
		NewIterator(opt *utils.Options) utils.Iterator
		Info() *Stats
		Close() error
	}

	// DB 对外暴露的接口对象 全局唯一，持有各种资源句柄
	DB struct {
		sync.RWMutex
		opt              *Options
		dirLock          *utils.DirLock
		lsm              *lsm.LSM
		wal              *wal.Manager
		walWatchdog      *wal.Watchdog
		vlog             *valueLog
		vwriter          valueLogWriter
		stats            *Stats
		blockWrites      int32
		vhead            *kv.ValuePtr
		lastLoggedHead   kv.ValuePtr
		headLogDelta     uint32
		isClosed         uint32
		orc              *oracle
		hot              *hotring.HotRing
		writeMetrics     *metrics.WriteMetrics
		commitQueue      commitQueue
		commitWG         sync.WaitGroup
		commitBatchPool  sync.Pool
		iterPool         *iteratorPool
		prefetchRing     *utils.Ring[prefetchRequest]
		prefetchWG       sync.WaitGroup
		prefetchState    atomic.Pointer[prefetchState]
		prefetchClamp    int32
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

var (
	head = []byte("!NoKV!head") // For storing value offset for replay.
)

// Open DB
func Open(opt *Options) *DB {
	db := &DB{opt: opt, writeMetrics: metrics.NewWriteMetrics()}
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

	lock, err := utils.AcquireDirLock(opt.WorkDir)
	utils.Panic(err)
	db.dirLock = lock

	utils.Panic(db.runRecoveryChecks())

	wlog, err := wal.Open(wal.Config{
		Dir:         opt.WorkDir,
		SyncOnWrite: false,
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
	// 初始化LSM结构
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:                  opt.WorkDir,
		MemTableSize:             opt.MemTableSize,
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
	// 初始化vlog结构
	db.initVLog()
	db.lsm.SetDiscardStatsCh(&(db.vlog.lfDiscardStats.flushChan))
	// 初始化统计信息
	db.stats = newStats(db)

	if opt.HotRingEnabled {
		db.hot = hotring.NewHotRing(opt.HotRingBits, nil)
		if opt.HotRingWindowSlots > 0 && opt.HotRingWindowSlotDuration > 0 {
			db.hot.EnableSlidingWindow(opt.HotRingWindowSlots, opt.HotRingWindowSlotDuration)
		}
		if opt.HotRingDecayInterval > 0 && opt.HotRingDecayShift > 0 {
			db.hot.EnableDecay(opt.HotRingDecayInterval, opt.HotRingDecayShift)
		}
		if opt.HotRingTopK <= 0 {
			opt.HotRingTopK = 16
		}
		db.prefetchClamp = 64
		db.prefetchWarm = 4
		db.prefetchHot = 16
		if db.prefetchHot <= db.prefetchWarm {
			db.prefetchHot = db.prefetchWarm + 4
		}
		db.prefetchCooldown = 15 * time.Second
		db.prefetchRing = utils.NewRing[prefetchRequest](256)
		db.prefetchState.Store(&prefetchState{
			pend:       make(map[string]struct{}),
			prefetched: make(map[string]time.Time),
		})
		db.prefetchWG.Add(1)
		go db.prefetchLoop()
		db.lsm.SetHotKeyProvider(func() [][]byte {
			if db.hot == nil {
				return nil
			}
			top := db.hot.TopN(opt.HotRingTopK)
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
	// 启动 sstable 的合并压缩过程
	db.lsm.StartCompacter()
	// 准备vlog gc
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
	// 启动 info 统计过程
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
	if err := manifest.Verify(db.opt.WorkDir); err != nil {
		if !stderrors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := wal.VerifyDir(db.opt.WorkDir); err != nil {
		return err
	}
	vlogDir := filepath.Join(db.opt.WorkDir, "vlog")
	cfg := vlogpkg.Config{
		Dir:      vlogDir,
		FileMode: utils.DefaultFileMode,
		MaxSize:  int64(db.opt.ValueLogFileSize),
	}
	if err := vlogpkg.VerifyDir(cfg); err != nil {
		if !stderrors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (db *DB) Close() error {
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

	if err := db.stats.close(); err != nil {
		return err
	}

	if db.walWatchdog != nil {
		db.walWatchdog.Stop()
		db.walWatchdog = nil
	}

	if db.hot != nil {
		db.hot.Close()
	}

	if db.prefetchRing != nil {
		db.prefetchRing.Close()
		db.prefetchWG.Wait()
		db.prefetchRing = nil
	}

	if err := db.lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.close(); err != nil {
		return err
	}
	if err := db.wal.Close(); err != nil {
		return err
	}

	if db.dirLock != nil {
		if err := db.dirLock.Release(); err != nil {
			return err
		}
		db.dirLock = nil
	}
	atomic.StoreUint32(&db.isClosed, 1)
	return nil
}

func (db *DB) Del(key []byte) error {
	return db.DelCF(kv.CFDefault, key)
}

// DelCF deletes a key from the specified column family.
func (db *DB) DelCF(cf kv.ColumnFamily, key []byte) error {
	// 写入一个值为nil的entry 作为墓碑消息实现删除
	e := kv.NewEntryWithCF(cf, key, nil)
	e.Meta = kv.BitDelete
	err := db.Set(e)
	e.DecrRef()
	return err
}

// SetCF writes a key/value pair into the specified column family.
func (db *DB) SetCF(cf kv.ColumnFamily, key, value []byte) error {
	e := kv.NewEntryWithCF(cf, key, value)
	err := db.Set(e)
	e.DecrRef()
	return err
}

func (db *DB) Set(data *kv.Entry) error {
	if data == nil || len(data.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// 做一些必要性的检查
	// 如果value 大于一个阈值 则创建值指针，并将其写入vlog中
	var (
		vp  *kv.ValuePtr
		err error
	)
	if !data.CF.Valid() {
		data.CF = kv.CFDefault
	}
	if err := db.maybeThrottleWrite(data.CF, data.Key); err != nil {
		return err
	}
	data.Key = kv.InternalKey(data.CF, data.Key, math.MaxUint32)
	// 如果value不应该直接写入LSM 则先写入 vlog文件，这时必须保证vlog具有重放功能
	// 以便于崩溃后恢复数据
	if !db.shouldWriteValueToLSM(data) {
		if vp, err = db.vlog.newValuePtr(data); err != nil {
			return err
		}
		data.Meta |= kv.BitValuePointer
		data.Value = vp.Encode()
	}
	if err := db.lsm.Set(data); err != nil {
		return err
	}
	db.recordCFWrite(data.CF, 1)
	if db.opt.SyncWrites {
		if err := db.wal.Sync(); err != nil {
			return err
		}
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
	if meta&kv.BitDelete == 0 && len(entry.Value) > 0 && !db.shouldWriteValueToLSM(entry) {
		vp, err := db.vlog.newValuePtr(entry)
		if err != nil {
			return err
		}
		entry.Meta |= kv.BitValuePointer
		entry.Value = vp.Encode()
	}
	if err := db.lsm.Set(entry); err != nil {
		return err
	}
	db.recordCFWrite(cf, 1)
	if db.opt.SyncWrites {
		if err := db.wal.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// DeleteVersionedEntry marks the specified version as deleted by writing a
// tombstone record.
func (db *DB) DeleteVersionedEntry(cf kv.ColumnFamily, key []byte, version uint64) error {
	return db.SetVersionedEntry(cf, key, version, nil, kv.BitDelete)
}

// GetVersionedEntry retrieves the value stored at the provided MVCC version.
// The caller is responsible for releasing the returned entry via DecrRef.
func (db *DB) GetVersionedEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	internalKey := kv.InternalKey(cf, key, version)
	entry, err := db.lsm.Get(internalKey)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, utils.ErrKeyNotFound
	}
	if kv.IsValuePtr(entry) {
		var vp kv.ValuePtr
		vp.Decode(entry.Value)
		result, cb, err := db.vlog.read(&vp)
		if err != nil {
			if cb != nil {
				kv.RunCallback(cb)
			}
			entry.DecrRef()
			return nil, err
		}
		entry.Value = kv.SafeCopy(nil, result)
		if cb != nil {
			kv.RunCallback(cb)
		}
	}
	cfStored, userKey, ts := kv.SplitInternalKey(entry.Key)
	entry.CF = cfStored
	entry.Key = kv.SafeCopy(nil, userKey)
	entry.Version = ts
	return entry, nil
}
func (db *DB) Get(key []byte) (*kv.Entry, error) {
	return db.GetCF(kv.CFDefault, key)
}

// GetCF reads a key from the specified column family.
func (db *DB) GetCF(cf kv.ColumnFamily, key []byte) (*kv.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}

	originKey := key
	// 添加时间戳用于查询
	internalKey := kv.InternalKey(cf, key, math.MaxUint32)

	var (
		entry *kv.Entry
		err   error
	)
	// 从LSM中查询entry，这时不确定entry是不是值指针
	if entry, err = db.lsm.Get(internalKey); err != nil {
		return entry, err
	}
	// 检查从lsm拿到的value是否是value ptr,是则从vlog中拿值
	if entry != nil && kv.IsValuePtr(entry) {
		var vp kv.ValuePtr
		vp.Decode(entry.Value)
		result, cb, err := db.vlog.read(&vp)
		defer kv.RunCallback(cb)
		if err != nil {
			return nil, err
		}
		entry.Value = kv.SafeCopy(nil, result)
	}

	if isDeletedOrExpired(entry.Meta, entry.ExpiresAt) {
		return nil, utils.ErrKeyNotFound
	}
	storedCF, _, ts := kv.SplitInternalKey(entry.Key)
	if storedCF.Valid() {
		entry.CF = storedCF
	} else {
		entry.CF = cf
	}
	if ts != 0 {
		entry.Version = ts
	}
	entry.Key = originKey
	db.recordCFRead(entry.CF, 1)
	db.recordRead(originKey)
	return entry, nil
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

func (db *DB) Info() *Stats {
	// 读取stats结构，打包数据并返回
	return db.stats
}

// RunValueLogGC triggers a value log garbage collection.
func (db *DB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return utils.ErrInvalidRequest
	}
	// Find head on disk
	headKey := kv.InternalKey(kv.CFDefault, head, math.MaxUint64)
	val, err := db.lsm.Get(headKey)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			val = kv.NewEntry(headKey, []byte{})
		} else {
			return fmt.Errorf("retrieving head from on-disk LSM: %w", err)
		}
	}
	defer val.DecrRef()

	// 内部key head 一定是value ptr 不需要检查内容
	var head kv.ValuePtr
	if len(val.Value) > 0 {
		head.Decode(val.Value)
	}

	// Pick a log file and run GC
	if err := db.vlog.runGC(discardRatio, &head); err != nil {
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
func (db *DB) SetRegionMetrics(rm *storepkg.RegionMetrics) {
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
