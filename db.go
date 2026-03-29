// Package NoKV provides the embedded database API and engine wiring.
package NoKV

import (
	"bytes"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/hotring"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/raftstore/engine"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/feichai0017/NoKV/wal"
)

// nonTxnMaxVersion is the read upper-bound sentinel used by non-transactional APIs.
// Non-transactional writes use monotonic versions <= this sentinel.
const nonTxnMaxVersion = kv.MaxVersion

type (
	// BatchSetItem represents one non-transactional write in the default CF.
	//
	// Ownership note: key is copied into the internal-key encoding; value is
	// referenced directly until the write path finishes.
	BatchSetItem struct {
		Key   []byte
		Value []byte
	}

	// MVCCStore defines MVCC/internal operations consumed by percolator and raftstore.
	MVCCStore interface {
		ApplyInternalEntries(entries []*kv.Entry) error
		// GetInternalEntry returns a borrowed internal entry without cloning/copying.
		// entry.Key remains in internal encoding (cf+user_key+ts). Callers must
		// DecrRef exactly once.
		GetInternalEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error)
		NewInternalIterator(opt *utils.Options) utils.Iterator
	}

	// RaftLog opens raft peer storage without exposing the underlying WAL manager.
	RaftLog interface {
		Open(groupID uint64, meta *raftmeta.Store) (engine.PeerStorage, error)
	}

	// DB is the global handle for the engine and owns shared resources.
	DB struct {
		sync.RWMutex
		opt             *Options
		fs              vfs.FS
		dirLock         io.Closer
		lsm             *lsm.LSM
		wal             *wal.Manager
		walWatchdog     *wal.Watchdog
		vlog            *valueLog
		stats           *Stats
		nonTxnVersion   atomic.Uint64
		blockWrites     atomic.Int32
		slowWrites      atomic.Int32
		discardStatsCh  chan map[manifest.ValueLogID]int64
		vheads          map[uint32]kv.ValuePtr
		lastLoggedHeads map[uint32]kv.ValuePtr
		headLogDelta    uint32
		isClosed        atomic.Uint32
		closeOnce       sync.Once
		closeErr        error
		throttleMu      sync.Mutex
		throttleCh      chan struct{}
		hotWrite        *hotring.RotatingHotRing
		writeMetrics    *metrics.WriteMetrics
		commitQueue     commitQueue
		commitWG        sync.WaitGroup
		commitBatchPool sync.Pool
		syncQueue       chan *syncBatch
		syncWG          sync.WaitGroup
		iterPool        *iteratorPool
		hotWriteLimited atomic.Uint64
		policyMatcher   atomic.Pointer[kv.ValueSeparationPolicyMatcher]
	}

	commitQueue struct {
		q              *utils.MPSCQueue[*commitRequest]
		pendingBytes   atomic.Int64
		pendingEntries atomic.Int64
	}

	commitRequest struct {
		req        *request
		entryCount int
		size       int64
	}

	commitBatch struct {
		reqs        []*commitRequest
		pool        *[]*commitRequest
		requests    []*request
		batchStart  time.Time
		valueLogDur time.Duration
	}

	// syncBatch carries committed-but-unsynced requests from commitWorker to
	// syncWorker. The syncWorker calls wal.Sync() once per batch of syncBatch
	// items before ack-ing the enclosed requests.
	syncBatch struct {
		reqs      []*commitRequest
		pool      *[]*commitRequest
		requests  []*request // apply-order slice for perReqErr
		failedAt  int
		applyDone time.Time // timestamp after apply, for metrics
	}
)

type dbRaftLog struct {
	db *DB
}

func newDB(opt *Options) *DB {
	db := &DB{opt: opt, writeMetrics: metrics.NewWriteMetrics()}
	db.fs = vfs.Ensure(opt.FS)
	db.headLogDelta = valueLogHeadLogInterval
	db.throttleCh = make(chan struct{})
	db.hotWrite = newHotWriteRing(opt)
	db.discardStatsCh = make(chan map[manifest.ValueLogID]int64, 16)
	db.commitBatchPool.New = func() any {
		batch := make([]*commitRequest, 0, db.opt.WriteBatchMaxCount)
		return &batch
	}
	return db
}

func (db *DB) openDurability() error {
	if err := db.fs.MkdirAll(db.opt.WorkDir, os.ModePerm); err != nil {
		return fmt.Errorf("open db: create workdir %q: %w", db.opt.WorkDir, err)
	}
	lock, err := db.fs.Lock(filepath.Join(db.opt.WorkDir, "LOCK"))
	if err != nil {
		return fmt.Errorf("open db: acquire workdir lock %q: %w", db.opt.WorkDir, err)
	}
	db.dirLock = lock

	if err := db.runRecoveryChecks(); err != nil {
		return fmt.Errorf("open db: recovery checks: %w", err)
	}

	wlog, err := wal.Open(wal.Config{
		Dir:         db.opt.WorkDir,
		SyncOnWrite: false,
		BufferSize:  db.opt.WALBufferSize,
		FS:          db.fs,
	})
	if err != nil {
		return fmt.Errorf("open db: wal open: %w", err)
	}
	db.wal = wlog
	return nil
}

func (db *DB) openEngine() error {
	baseTableSize, baseLevelSize := db.levelSizes()
	lsmCore, err := lsm.NewLSM(&lsm.Options{
		FS:                            db.fs,
		WorkDir:                       db.opt.WorkDir,
		MemTableSize:                  db.opt.MemTableSize,
		MemTableEngine:                string(db.opt.MemTableEngine),
		SSTableMaxSz:                  db.opt.SSTableMaxSz,
		BlockSize:                     8 * 1024,
		BloomFalsePositive:            0.01,
		BaseLevelSize:                 baseLevelSize,
		LevelSizeMultiplier:           8,
		BaseTableSize:                 baseTableSize,
		TableSizeMultiplier:           2,
		NumLevelZeroTables:            db.opt.NumLevelZeroTables,
		L0SlowdownWritesTrigger:       db.opt.L0SlowdownWritesTrigger,
		L0StopWritesTrigger:           db.opt.L0StopWritesTrigger,
		L0ResumeWritesTrigger:         db.opt.L0ResumeWritesTrigger,
		CompactionSlowdownTrigger:     db.opt.CompactionSlowdownTrigger,
		CompactionStopTrigger:         db.opt.CompactionStopTrigger,
		CompactionResumeTrigger:       db.opt.CompactionResumeTrigger,
		MaxLevelNum:                   utils.MaxLevelNum,
		NumCompactors:                 db.opt.NumCompactors,
		CompactionPolicy:              string(db.opt.CompactionPolicy),
		IngestCompactBatchSize:        db.opt.IngestCompactBatchSize,
		IngestBacklogMergeScore:       db.opt.IngestBacklogMergeScore,
		IngestShardParallelism:        db.opt.IngestShardParallelism,
		WriteThrottleMinRate:          db.opt.WriteThrottleMinRate,
		WriteThrottleMaxRate:          db.opt.WriteThrottleMaxRate,
		CompactionValueWeight:         db.opt.CompactionValueWeight,
		CompactionValueAlertThreshold: db.opt.CompactionValueAlertThreshold,
		BlockCacheBytes:               db.opt.BlockCacheBytes,
		IndexCacheBytes:               db.opt.IndexCacheBytes,
		DiscardStatsCh:                &db.discardStatsCh,
		ManifestSync:                  db.opt.ManifestSync,
		ManifestRewriteThreshold:      db.opt.ManifestRewriteThreshold,
		WALGCPolicy:                   newDBWALGCPolicy(db),
		ThrottleCallback:              db.applyThrottle,
	}, db.wal)
	if err != nil {
		return fmt.Errorf("open db: lsm init: %w", err)
	}
	db.lsm = lsmCore
	db.nonTxnVersion.Store(db.lsm.Diagnostics().MaxVersion)
	db.iterPool = newIteratorPool()
	db.initVLog()
	db.stats = newStats(db)
	if len(db.opt.ValueSeparationPolicies) > 0 {
		db.policyMatcher.Store(kv.NewValueSeparationPolicyMatcher(db.opt.ValueSeparationPolicies))
	}
	return nil
}

func (db *DB) startWriteRuntime() {
	queueCap := max(db.opt.WriteBatchMaxCount*8, 1024)
	db.commitQueue.init(queueCap)
	if db.opt.SyncWrites && db.opt.SyncPipeline {
		db.syncQueue = make(chan *syncBatch, 128)
		db.syncWG.Add(1)
		go db.syncWorker()
	}
	db.commitWG.Add(1)
	go db.commitWorker()
}

func (db *DB) startServices() {
	db.lsm.StartCompacter()
	if db.opt.EnableWALWatchdog {
		db.walWatchdog = wal.NewWatchdog(wal.WatchdogConfig{
			Manager:      db.wal,
			Interval:     db.opt.WALAutoGCInterval,
			MinRemovable: db.opt.WALAutoGCMinRemovable,
			MaxBatch:     db.opt.WALAutoGCMaxBatch,
			WarnRatio:    db.opt.WALTypedRecordWarnRatio,
			WarnSegments: db.opt.WALTypedRecordWarnSegments,
			RaftPointers: db.opt.RaftPointerSnapshot,
		})
		if db.walWatchdog != nil {
			db.walWatchdog.Start()
		}
	}
	db.stats.StartStats()
	if db.opt.ValueLogGCInterval > 0 {
		if db.vlog != nil && db.vlog.lfDiscardStats != nil && db.vlog.lfDiscardStats.closer != nil {
			db.vlog.lfDiscardStats.closer.Add(1)
			go db.runValueLogGCPeriodically()
		}
	}
}

func (db *DB) levelSizes() (int64, int64) {
	baseTableSize := db.opt.MemTableSize
	if baseTableSize <= 0 {
		baseTableSize = 8 << 20
	}
	if baseTableSize < 8<<20 {
		baseTableSize = 8 << 20
	}
	if db.opt.SSTableMaxSz > 0 && baseTableSize > db.opt.SSTableMaxSz {
		baseTableSize = db.opt.SSTableMaxSz
	}
	baseLevelSize := max(baseTableSize*4, 32<<20)
	return baseTableSize, baseLevelSize
}

func (l dbRaftLog) Open(groupID uint64, meta *raftmeta.Store) (engine.PeerStorage, error) {
	return engine.OpenWALStorage(engine.WALStorageConfig{
		GroupID:   groupID,
		WAL:       l.db.wal,
		LocalMeta: meta,
	})
}

// RaftLog returns the raft peer-storage capability backed by the DB WAL.
func (db *DB) RaftLog() RaftLog {
	if db == nil || db.wal == nil {
		return nil
	}
	return dbRaftLog{db: db}
}

// Open constructs the database and returns initialization errors instead of panicking.
func Open(opt *Options) (_ *DB, err error) {
	if opt == nil {
		return nil, stderrors.New("open db: nil options")
	}
	frozen := *opt
	frozen.normalizeInPlace()
	db := newDB(&frozen)
	defer func() {
		if err != nil {
			err = stderrors.Join(err, db.closeInternal())
		}
	}()
	if err := db.openDurability(); err != nil {
		return nil, err
	}
	if err := db.openEngine(); err != nil {
		return nil, err
	}
	db.startWriteRuntime()
	db.startServices()
	return db, nil
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
	bucketCount := max(db.opt.ValueLogBucketCount, 1)
	for bucket := range bucketCount {
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
	if db.stats != nil {
		if err := db.stats.close(); err != nil {
			errs = append(errs, fmt.Errorf("stats close: %w", err))
		}
		db.stats = nil
	}

	if db.walWatchdog != nil {
		db.walWatchdog.Stop()
		db.walWatchdog = nil
	}

	if db.hotWrite != nil {
		db.hotWrite.Close()
	}

	if db.lsm != nil {
		if err := db.lsm.Close(); err != nil {
			errs = append(errs, fmt.Errorf("lsm close: %w", err))
		}
		db.lsm = nil
	}

	if db.vlog != nil {
		if err := db.vlog.close(); err != nil {
			errs = append(errs, fmt.Errorf("vlog close: %w", err))
		}
		db.vlog = nil
	}

	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			errs = append(errs, fmt.Errorf("wal close: %w", err))
		}
		db.wal = nil
	}

	if db.dirLock != nil {
		if err := db.dirLock.Close(); err != nil {
			errs = append(errs, fmt.Errorf("dir lock release: %w", err))
		}
		db.dirLock = nil
	}

	db.isClosed.Store(1)

	if len(errs) > 0 {
		return stderrors.Join(errs...)
	}

	return nil
}

// Del removes a key from the default column family by writing a tombstone.
func (db *DB) Del(key []byte) error {
	if len(key) == 0 {
		return utils.ErrEmptyKey
	}
	entry := kv.NewInternalEntry(kv.CFDefault, key, db.nextNonTxnVersion(), nil, kv.BitDelete, 0)
	defer entry.DecrRef()
	return db.ApplyInternalEntries([]*kv.Entry{entry})
}

// DeleteRange removes all keys in [start, end) from the default column family.
func (db *DB) DeleteRange(start, end []byte) error {
	if len(start) == 0 || len(end) == 0 {
		return utils.ErrEmptyKey
	}
	if bytes.Compare(start, end) >= 0 {
		return utils.ErrInvalidRequest
	}
	entry := kv.NewInternalEntry(kv.CFDefault, start, db.nextNonTxnVersion(), end, kv.BitRangeDelete, 0)
	defer entry.DecrRef()
	return db.ApplyInternalEntries([]*kv.Entry{entry})
}

// Set writes a key/value pair into the default column family.
// Use Del for explicit deletion; nil values are rejected.
func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return utils.ErrEmptyKey
	}
	if value == nil {
		return utils.ErrNilValue
	}
	entry := kv.NewInternalEntry(kv.CFDefault, key, db.nextNonTxnVersion(), value, 0, 0)
	defer entry.DecrRef()
	return db.ApplyInternalEntries([]*kv.Entry{entry})
}

// SetBatch writes multiple key/value pairs into the default column family.
//
// Semantics:
//   - Non-transactional API: each entry receives a monotonically increasing
//     internal version.
//   - The batch is submitted through the regular write pipeline and commit queue.
//
// Validation:
//   - Empty batch is a no-op.
//   - Every item must have a non-empty key and non-nil value.
//
// Ownership:
//   - key bytes are encoded into internal keys.
//   - value slices are referenced directly until this call returns; callers must
//     keep them immutable for the duration of this call.
func (db *DB) SetBatch(items []BatchSetItem) error {
	if len(items) == 0 {
		return nil
	}
	entries := make([]*kv.Entry, 0, len(items))
	release := func() {
		for _, entry := range entries {
			if entry != nil {
				entry.DecrRef()
			}
		}
	}
	for _, item := range items {
		if len(item.Key) == 0 {
			release()
			return utils.ErrEmptyKey
		}
		if item.Value == nil {
			release()
			return utils.ErrNilValue
		}
		entries = append(entries, kv.NewInternalEntry(kv.CFDefault, item.Key, db.nextNonTxnVersion(), item.Value, 0, 0))
	}
	defer release()
	return db.ApplyInternalEntries(entries)
}

// SetWithTTL writes a key/value pair into the default column family with TTL.
// Use Del for explicit deletion; nil values are rejected.
//
// Ownership note: key is encoded into a new internal-key buffer, while value is
// referenced directly (no deep copy). Callers must keep value immutable until
// this method returns.
func (db *DB) SetWithTTL(key, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return utils.ErrEmptyKey
	}
	if value == nil {
		return utils.ErrNilValue
	}
	entry := kv.NewInternalEntry(kv.CFDefault, key, db.nextNonTxnVersion(), value, 0, 0)
	entry.WithTTL(ttl)
	defer entry.DecrRef()
	return db.ApplyInternalEntries([]*kv.Entry{entry})
}

// nextNonTxnVersion allocates the next monotonic version for non-transactional writes.
func (db *DB) nextNonTxnVersion() uint64 {
	next := db.nonTxnVersion.Add(1)
	if next == 0 {
		panic("NoKV: non-transactional version overflow (legacy max-sentinel data requires migration)")
	}
	return next
}

// ApplyInternalEntries writes pre-built internal-key entries through the regular write
// pipeline.
//
// The caller must provide entries with internal keys. The entry slices must not
// be mutated until this call returns.
func (db *DB) ApplyInternalEntries(entries []*kv.Entry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if len(entries) == 0 {
		return nil
	}
	for _, entry := range entries {
		if entry == nil || len(entry.Key) == 0 {
			return utils.ErrEmptyKey
		}
		// ApplyInternalEntries is for pre-built internal keys only.
		parsedCF, userKey, parsedVersion, ok := kv.SplitInternalKey(entry.Key)
		if !ok || len(userKey) == 0 {
			return utils.ErrInvalidRequest
		}
		entry.CF = parsedCF
		entry.Version = parsedVersion
		if err := db.maybeThrottleWrite(parsedCF, userKey); err != nil {
			return err
		}
	}
	for _, entry := range entries {
		entry.IncrRef()
	}
	return db.batchSet(entries)
}

// GetInternalEntry retrieves one internal-key record for the provided version.
//
// The returned entry is borrowed from internal pools and returned as-is
// (no clone/no copy). entry.Key remains in internal encoding
// (cf+user_key+ts). Callers MUST call DecrRef exactly once when finished.
func (db *DB) GetInternalEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
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
	return entry, nil
}

// Get reads the latest visible value for key from the default column family.
func (db *DB) Get(key []byte) (*kv.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	// Non-transactional API: use the max sentinel timestamp (not for MVCC).
	internalKey := kv.InternalKey(kv.CFDefault, key, nonTxnMaxVersion)
	entry, err := db.lsm.Get(internalKey)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, utils.ErrKeyNotFound
	}
	defer entry.DecrRef()
	if entry.IsRangeDelete() {
		return nil, utils.ErrKeyNotFound
	}
	if entry.IsDeletedOrExpired() {
		return nil, utils.ErrKeyNotFound
	}
	var out *kv.Entry
	if kv.IsValuePtr(entry) {
		out, err = db.detachValuePointerEntry(entry)
		if err != nil {
			return nil, err
		}
	} else {
		out = cloneEntry(entry)
	}
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
	if entry.IsRangeDelete() {
		entry.DecrRef()
		return nil, utils.ErrKeyNotFound
	}

	// Range tombstone coverage is checked in lsm.Get() while memtables are pinned.

	if !kv.IsValuePtr(entry) {
		if !entry.PopulateInternalMeta() {
			entry.DecrRef()
			return nil, utils.ErrInvalidRequest
		}
		return entry, nil
	}
	var vp kv.ValuePtr
	vp.Decode(entry.Value)
	result, cb, readErr := db.vlog.read(&vp)
	if cb != nil {
		defer cb()
	}
	if readErr != nil {
		entry.DecrRef()
		return nil, readErr
	}
	entry.Value = kv.SafeCopy(nil, result)
	entry.Meta &^= kv.BitValuePointer
	if !entry.PopulateInternalMeta() {
		entry.DecrRef()
		return nil, utils.ErrInvalidRequest
	}
	return entry, nil
}

// cloneEntry converts an internal/buffered entry into a detached public value object.
//
// It deep-copies key/value bytes so the returned entry is independent from pooled
// memory, parses internal key layout (CF/user-key/version), and fills external-facing
// metadata. The returned entry does not participate in internal ref-count lifecycle;
// API callers must not call DecrRef on it.
func cloneEntry(src *kv.Entry) *kv.Entry {
	if src == nil {
		return nil
	}
	cf, userKeySrc, ts, ok := kv.SplitInternalKey(src.Key)
	utils.CondPanicFunc(!ok, func() error {
		return fmt.Errorf("cloneEntry expects internal key: %x", src.Key)
	})
	version := src.Version
	if ts != 0 {
		version = ts
	}
	buf := make([]byte, len(userKeySrc)+len(src.Value))
	keyCopy := buf[:len(userKeySrc)]
	copy(keyCopy, userKeySrc)
	valueCopy := buf[len(userKeySrc):]
	copy(valueCopy, src.Value)
	return &kv.Entry{
		Key:          keyCopy,
		Value:        valueCopy,
		ExpiresAt:    src.ExpiresAt,
		CF:           cf,
		Meta:         src.Meta,
		Version:      version,
		Offset:       src.Offset,
		Hlen:         src.Hlen,
		ValThreshold: src.ValThreshold,
	}
}

func (db *DB) detachValuePointerEntry(src *kv.Entry) (*kv.Entry, error) {
	if src == nil {
		return nil, utils.ErrKeyNotFound
	}
	if !kv.IsValuePtr(src) {
		return cloneEntry(src), nil
	}
	cf, userKeySrc, ts, ok := kv.SplitInternalKey(src.Key)
	if !ok {
		return nil, utils.ErrInvalidRequest
	}
	version := src.Version
	if ts != 0 {
		version = ts
	}
	var vp kv.ValuePtr
	vp.Decode(src.Value)
	result, cb, err := db.vlog.read(&vp)
	if cb != nil {
		defer cb()
	}
	if err != nil {
		return nil, err
	}
	keyCopy := kv.SafeCopy(nil, userKeySrc)
	value := result
	if cb != nil {
		buf := make([]byte, len(userKeySrc)+len(result))
		keyCopy = buf[:len(userKeySrc)]
		copy(keyCopy, userKeySrc)
		value = buf[len(userKeySrc):]
		copy(value, result)
	}
	return &kv.Entry{
		Key:          keyCopy,
		Value:        value,
		ExpiresAt:    src.ExpiresAt,
		CF:           cf,
		Meta:         src.Meta &^ kv.BitValuePointer,
		Version:      version,
		Offset:       src.Offset,
		Hlen:         src.Hlen,
		ValThreshold: src.ValThreshold,
	}, nil
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
	heads := db.lsm.ValueLogHeadSnapshot()
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
					slog.Default().Warn("value log gc", "error", err)
				}
			}
		case <-db.vlog.lfDiscardStats.closer.Closed():
			return
		}
	}
}

func (db *DB) shouldWriteValueToLSM(e *kv.Entry) bool {
	// Range deletes always stay in LSM
	if e.IsRangeDelete() {
		return true
	}

	// Check if we have policy-based separation enabled
	matcher := db.policyMatcher.Load()
	if matcher != nil {
		if policy := matcher.MatchPolicy(e); policy != nil {
			switch policy.Strategy {
			case kv.AlwaysInline:
				return true
			case kv.AlwaysOffload:
				return false
			case kv.ThresholdBased:
				return int64(len(e.Value)) < policy.Threshold
			}
		}
	}

	// Fall back to global threshold
	return int64(len(e.Value)) < db.opt.ValueThreshold
}

// GetValueSeparationPolicyStats returns the current value separation policy statistics.
// Returns nil if no policies are configured.
func (db *DB) GetValueSeparationPolicyStats() map[string]int64 {
	matcher := db.policyMatcher.Load()
	if matcher == nil {
		return nil
	}
	return matcher.GetStats()
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

// WorkDir returns the database working directory.
func (db *DB) WorkDir() string {
	if db == nil || db.opt == nil {
		return ""
	}
	return db.opt.WorkDir
}

// IsClosed reports whether Close has finished and the DB no longer accepts work.
func (db *DB) IsClosed() bool {
	return db.isClosed.Load() == 1
}
