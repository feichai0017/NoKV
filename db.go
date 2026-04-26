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

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
	vlogpkg "github.com/feichai0017/NoKV/engine/vlog"
	"github.com/feichai0017/NoKV/engine/wal"
	dbruntime "github.com/feichai0017/NoKV/internal/runtime"
	"github.com/feichai0017/NoKV/metrics"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/thermos"
	"github.com/feichai0017/NoKV/utils"
	pkgerrors "github.com/pkg/errors"
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
		NewInternalIterator(opt *index.Options) index.Iterator
	}

	// RaftLog opens raft peer storage without exposing the underlying WAL manager.
	RaftLog interface {
		Open(groupID uint64, meta *localmeta.Store) (raftlog.PeerStorage, error)
	}

	// DB is the global handle for the engine and owns shared resources.
	DB struct {
		sync.RWMutex
		opt             *Options
		fs              vfs.FS
		dirLock         io.Closer
		lsm             *lsm.LSM
		wal             *wal.Manager
		vlog            *valueLog
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
		hotWrite        *thermos.RotatingThermos
		writeMetrics    *metrics.WriteMetrics
		commitQueue     dbruntime.CommitQueue
		commitWG        sync.WaitGroup
		commitBatchPool sync.Pool
		syncQueue       chan *dbruntime.SyncBatch
		syncWG          sync.WaitGroup
		iterPool        *dbruntime.IteratorPool
		hotWriteLimited atomic.Uint64
		policyMatcher   atomic.Pointer[kv.ValueSeparationPolicyMatcher]
		background      dbruntime.BackgroundServices
		runtimeModules  dbruntime.Registry
	}
)

type dbRaftLog struct {
	db *DB
}

func newDB(opt *Options) *DB {
	cfg := opt
	if cfg == nil {
		cfg = &Options{}
	}
	db := &DB{opt: cfg, writeMetrics: metrics.NewWriteMetrics()}
	db.fs = vfs.Ensure(cfg.FS)
	db.headLogDelta = valueLogHeadLogInterval
	db.throttleCh = make(chan struct{})
	db.hotWrite = dbruntime.NewHotWriteRing(dbruntime.HotWriteConfig{
		Enabled:          cfg.ThermosEnabled && cfg.WriteHotKeyLimit > 0,
		Bits:             cfg.ThermosBits,
		WindowSlots:      cfg.ThermosWindowSlots,
		WindowSlotPeriod: cfg.ThermosWindowSlotDuration,
		DecayInterval:    cfg.ThermosDecayInterval,
		DecayShift:       cfg.ThermosDecayShift,
		NodeCap:          cfg.ThermosNodeCap,
		NodeSampleBits:   cfg.ThermosNodeSampleBits,
		RotationInterval: cfg.ThermosRotationInterval,
	})
	db.discardStatsCh = make(chan map[manifest.ValueLogID]int64, 16)
	db.commitBatchPool.New = func() any {
		batch := make([]*dbruntime.CommitRequest, 0, db.opt.WriteBatchMaxCount)
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
		Dir:        db.opt.WorkDir,
		BufferSize: db.opt.WALBufferSize,
		FS:         db.fs,
	})
	if err != nil {
		return fmt.Errorf("open db: wal open: %w", err)
	}
	db.wal = wlog
	if db.opt.RaftPointerSnapshot != nil {
		if err := db.wal.RegisterRetention("raft", func() wal.RetentionMark {
			return raftRetentionMark(db.opt.RaftPointerSnapshot())
		}); err != nil {
			return fmt.Errorf("open db: wal raft retention: %w", err)
		}
	}
	return nil
}

func raftRetentionMark(ptrs map[uint64]localmeta.RaftLogPointer) wal.RetentionMark {
	var first uint32
	for _, ptr := range ptrs {
		if ptr.Segment > 0 && (first == 0 || ptr.Segment < first) {
			first = ptr.Segment
		}
		if ptr.SegmentIndex > 0 {
			seg := uint32(ptr.SegmentIndex)
			if first == 0 || seg < first {
				first = seg
			}
		}
	}
	return wal.RetentionMark{FirstSegment: first}
}

func (db *DB) checkWorkDirMode() error {
	if db == nil || db.opt == nil {
		return fmt.Errorf("open db: options not initialized")
	}
	mode, err := raftmode.ReadOnlyMode(db.opt.WorkDir)
	if err != nil {
		return fmt.Errorf("open db: read workdir mode: %w", err)
	}
	if raftmode.Allowed(db.opt.AllowedModes, mode) {
		return nil
	}
	if len(db.opt.AllowedModes) == 0 {
		return fmt.Errorf("open db: workdir mode %q requires explicit distributed open intent", mode)
	}
	return fmt.Errorf("open db: workdir mode %q is not allowed for this open intent", mode)
}

func (db *DB) openEngine() error {
	lsmCore, err := lsm.NewLSM(db.runtimeLSMOptions(), db.wal)
	if err != nil {
		return fmt.Errorf("open db: lsm init: %w", err)
	}
	db.lsm = lsmCore
	db.nonTxnVersion.Store(db.lsm.Diagnostics().MaxVersion)
	db.iterPool = dbruntime.NewIteratorPool()
	db.initVLog()
	db.background.Init(newStats(db))
	if len(db.opt.ValueSeparationPolicies) > 0 {
		db.policyMatcher.Store(kv.NewValueSeparationPolicyMatcher(db.opt.ValueSeparationPolicies))
	}
	return nil
}

func (db *DB) startWriteRuntime() {
	queueCap := max(db.opt.WriteBatchMaxCount*8, 1024)
	db.commitQueue.Init(queueCap)
	if db.opt.SyncWrites && db.opt.SyncPipeline {
		db.syncQueue = make(chan *dbruntime.SyncBatch, 128)
		db.syncWG.Add(1)
		go db.syncWorker()
	}
	db.commitWG.Add(1)
	go db.commitWorker()
}

func (db *DB) runtimeLSMOptions() *lsm.Options {
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
	cfg := &lsm.Options{
		FS:                       db.fs,
		WorkDir:                  db.opt.WorkDir,
		MemTableSize:             db.opt.MemTableSize,
		MemTableEngine:           string(db.opt.MemTableEngine),
		SSTableMaxSz:             db.opt.SSTableMaxSz,
		BlockSize:                lsm.DefaultBlockSize,
		BloomFalsePositive:       lsm.DefaultBloomFalsePositive,
		BaseLevelSize:            baseLevelSize,
		LevelSizeMultiplier:      lsm.DefaultLevelSizeMultiplier,
		BaseTableSize:            baseTableSize,
		TableSizeMultiplier:      lsm.DefaultTableSizeMultiplier,
		MaxLevelNum:              utils.MaxLevelNum,
		CompactionPolicy:         string(db.opt.CompactionPolicy),
		DiscardStatsCh:           &db.discardStatsCh,
		ManifestSync:             db.opt.ManifestSync,
		ManifestRewriteThreshold: db.opt.ManifestRewriteThreshold,
		WALGCPolicy: dbruntime.NewWALGCPolicy(dbruntime.WALGCPolicyConfig{
			RaftPointers: db.opt.RaftPointerSnapshot,
			SegmentMetrics: func(segmentID uint32) wal.RecordMetrics {
				if db.wal == nil {
					return wal.RecordMetrics{}
				}
				return db.wal.SegmentRecordMetrics(segmentID)
			},
			Warn: func(msg string, args ...any) {
				slog.Default().Warn(msg, args...)
			},
		}),
		ThrottleCallback: db.applyThrottle,
	}
	db.opt.applyLSMSharedOptions(cfg)
	return cfg
}

func (l dbRaftLog) Open(groupID uint64, meta *localmeta.Store) (raftlog.PeerStorage, error) {
	return raftlog.OpenWALStorage(raftlog.WALStorageConfig{
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
	frozen.resolveOpenDefaults()
	db := newDB(&frozen)
	defer func() {
		if err != nil {
			err = stderrors.Join(err, db.closeInternal())
		}
	}()
	if err := db.checkWorkDirMode(); err != nil {
		return nil, err
	}
	if err := db.openDurability(); err != nil {
		return nil, err
	}
	if err := db.openEngine(); err != nil {
		return nil, err
	}
	db.startWriteRuntime()
	db.background.Start(dbruntime.BackgroundConfig{
		StartCompacter:    db.lsm.StartCompacter,
		EnableWALWatchdog: db.opt.EnableWALWatchdog,
		WALWatchdogConfig: wal.WatchdogConfig{
			Manager:      db.wal,
			Interval:     db.opt.WALAutoGCInterval,
			MinRemovable: db.opt.WALAutoGCMinRemovable,
			MaxBatch:     db.opt.WALAutoGCMaxBatch,
			WarnRatio:    db.opt.WALTypedRecordWarnRatio,
			WarnSegments: db.opt.WALTypedRecordWarnSegments,
			RaftPointers: db.opt.RaftPointerSnapshot,
		},
		StartValueLogGC: func() {
			if db.opt.ValueLogGCInterval > 0 {
				if db.vlog != nil && db.vlog.lfDiscardStats != nil && db.vlog.lfDiscardStats.closer != nil {
					db.vlog.lfDiscardStats.closer.Add(1)
					go db.runValueLogGCPeriodically()
				}
			}
		},
	})
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
	db.runtimeModules.CloseAll()

	var errs []error
	if err := db.background.Close(); err != nil {
		errs = append(errs, err)
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

// MaterializeInternalEntry converts a borrowed internal entry into a detached
// internal entry suitable for export or replay. The returned entry preserves
// canonical internal-key layout and resolves any value-log indirection into
// inline value bytes.
func (db *DB) MaterializeInternalEntry(src *kv.Entry) (*kv.Entry, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if src == nil {
		return nil, utils.ErrKeyNotFound
	}
	value, meta, err := db.resolveDetachedValue(src)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, len(src.Key)+len(value))
	keyCopy := buf[:len(src.Key)]
	copy(keyCopy, src.Key)
	valueCopy := buf[len(src.Key):]
	copy(valueCopy, value)
	return &kv.Entry{
		Key:          keyCopy,
		Value:        valueCopy,
		ExpiresAt:    src.ExpiresAt,
		CF:           src.CF,
		Meta:         meta,
		Version:      src.Version,
		Offset:       src.Offset,
		Hlen:         src.Hlen,
		ValThreshold: src.ValThreshold,
	}, nil
}

func (db *DB) resolveDetachedValue(src *kv.Entry) ([]byte, byte, error) {
	meta := src.Meta
	if !kv.IsValuePtr(src) {
		return src.Value, meta, nil
	}
	var vp kv.ValuePtr
	vp.Decode(src.Value)
	result, cb, err := db.vlog.read(&vp)
	if cb != nil {
		defer cb()
	}
	if err != nil {
		return nil, meta, err
	}
	return result, meta &^ kv.BitValuePointer, nil
}

// Get reads the latest visible value for key from the default column family.
//
// The returned Entry is DETACHED: caller owns Entry.Value bytes and must
// NOT call DecrRef. This differs from GetInternalEntry, which returns
// a BORROWED entry that must be released with DecrRef exactly once.
// Mixing the two contracts typically surfaces as a crash far from the
// bug site.
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
	cf, userKey, ts, ok := kv.SplitInternalKey(entry.Key)
	if !ok {
		return nil, utils.ErrInvalidRequest
	}
	version := entry.Version
	if ts != 0 {
		version = ts
	}
	value, meta, err := db.resolveDetachedValue(entry)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, len(userKey)+len(value))
	keyCopy := buf[:len(userKey)]
	copy(keyCopy, userKey)
	valueCopy := buf[len(userKey):]
	copy(valueCopy, value)
	return &kv.Entry{
		Key:          keyCopy,
		Value:        valueCopy,
		ExpiresAt:    entry.ExpiresAt,
		CF:           cf,
		Meta:         meta,
		Version:      version,
		Offset:       entry.Offset,
		Hlen:         entry.Hlen,
		ValThreshold: entry.ValThreshold,
	}, nil
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

// Info returns the live stats collector for snapshot/diagnostic access.
func (db *DB) Info() *Stats {
	if db == nil {
		return nil
	}
	stats, _ := db.background.StatsCollector().(*Stats)
	return stats
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
	db.background.SetRegionMetrics(rm)
}

func (db *DB) SyncWAL() error {
	if db == nil || db.wal == nil {
		return fmt.Errorf("db: wal is unavailable")
	}
	return db.wal.Sync()
}

func (db *DB) ReplayWAL(fn func(info wal.EntryInfo, payload []byte) error) error {
	if db == nil || db.wal == nil {
		return fmt.Errorf("db: wal is unavailable")
	}
	return db.wal.Replay(fn)
}

// IsClosed reports whether Close has finished and the DB no longer accepts work.
func (db *DB) IsClosed() bool {
	return db.isClosed.Load() == 1
}

func (db *DB) applyThrottle(state lsm.WriteThrottleState) {
	state = dbruntime.NormalizeWriteThrottleState(state)
	stop := int32(0)
	slow := int32(0)
	switch state {
	case lsm.WriteThrottleStop:
		stop = 1
	case lsm.WriteThrottleSlowdown:
		slow = 1
	}
	prevStop := db.blockWrites.Swap(stop)
	prevSlow := db.slowWrites.Swap(slow)
	if prevStop == stop && prevSlow == slow {
		return
	}
	db.throttleMu.Lock()
	ch := db.throttleCh
	db.throttleCh = make(chan struct{})
	db.throttleMu.Unlock()
	close(ch)
	switch state {
	case lsm.WriteThrottleStop:
		slog.Default().Warn("write stop enabled due to compaction backlog")
	case lsm.WriteThrottleSlowdown:
		slog.Default().Info("write slowdown enabled due to compaction backlog")
	default:
		slog.Default().Info("write throttling cleared")
	}
}

func (db *DB) sendToWriteCh(entries []*kv.Entry, waitOnThrottle bool) (*dbruntime.Request, error) {
	var size int64
	count := int64(len(entries))
	for _, e := range entries {
		size += int64(e.EstimateSize(int(db.opt.ValueThreshold)))
	}
	limitCount, limitSize := db.opt.MaxBatchCount, db.opt.MaxBatchSize
	if count >= limitCount || size >= limitSize {
		return nil, utils.ErrTxnTooBig
	}
	if db.slowWrites.Load() == 1 {
		if db.lsm != nil {
			if d := dbruntime.SlowdownDelay(size, db.lsm.ThrottleRateBytesPerSec()); d > 0 {
				time.Sleep(d)
			}
		}
	}
	for db.blockWrites.Load() == 1 {
		if !waitOnThrottle {
			return nil, utils.ErrBlockedWrites
		}
		if db.isClosed.Load() == 1 || db.commitQueue.Closed() {
			return nil, utils.ErrBlockedWrites
		}
		db.throttleMu.Lock()
		ch := db.throttleCh
		db.throttleMu.Unlock()
		if db.blockWrites.Load() == 0 {
			break
		}
		select {
		case <-ch:
		case <-db.commitQueue.CloseCh():
			return nil, utils.ErrBlockedWrites
		}
	}

	req := dbruntime.RequestPool.Get().(*dbruntime.Request)
	req.Reset()
	req.Entries = entries
	if db.writeMetrics != nil {
		req.EnqueueAt = time.Now()
	}
	req.WG.Add(1)
	req.IncrRef()

	cr := dbruntime.CommitRequestPool.Get().(*dbruntime.CommitRequest)
	cr.Req = req
	cr.EntryCount = int(count)
	cr.Size = size

	if err := db.enqueueCommitRequest(cr); err != nil {
		req.WG.Done()
		req.Entries = nil
		req.DecrRef()
		dbruntime.CommitRequestPool.Put(cr)
		return nil, err
	}

	return req, nil
}

func (db *DB) maybeThrottleWrite(cf kv.ColumnFamily, key []byte) error {
	if db == nil || db.opt == nil {
		return nil
	}
	if !dbruntime.ShouldThrottleHotWrite(db.hotWrite, db.opt.WriteHotKeyLimit, cf, key) {
		return nil
	}
	db.hotWriteLimited.Add(1)
	return utils.ErrHotKeyWriteThrottle
}

func (db *DB) batchSet(entries []*kv.Entry) error {
	req, err := db.sendToWriteCh(entries, true)
	if err != nil {
		for _, entry := range entries {
			if entry != nil {
				entry.DecrRef()
			}
		}
		return err
	}
	return req.Wait()
}

func (db *DB) enqueueCommitRequest(cr *dbruntime.CommitRequest) error {
	if cr == nil {
		return nil
	}
	cq := &db.commitQueue
	if cq.Closed() && cq.CloseCh() == nil {
		return utils.ErrBlockedWrites
	}
	if cq.Closed() {
		return utils.ErrBlockedWrites
	}
	cq.AddPending(int64(cr.EntryCount), cr.Size)
	queued := false
	defer func() {
		if !queued {
			cq.AddPending(-int64(cr.EntryCount), -cr.Size)
		}
	}()
	if !cq.Push(cr) {
		return utils.ErrBlockedWrites
	}
	queued = true
	qLen := cq.Len()
	qEntries := cq.PendingEntries()
	qBytes := cq.PendingBytes()
	db.writeMetrics.UpdateQueue(qLen, int(qEntries), qBytes)
	return nil
}

func (db *DB) nextCommitBatch(consumer *utils.MPSCConsumer[*dbruntime.CommitRequest]) *dbruntime.CommitBatch {
	cq := &db.commitQueue
	first := cq.Pop(consumer)
	if first == nil {
		return nil
	}

	batchPtr := db.commitBatchPool.Get().(*[]*dbruntime.CommitRequest)
	batch := (*batchPtr)[:0]
	pendingEntries := int64(0)
	pendingBytes := int64(0)
	coalesceWait := db.opt.WriteBatchWait

	limitCount, limitSize := db.opt.WriteBatchMaxCount, db.opt.WriteBatchMaxSize
	backlog := cq.Len()
	if backlog > limitCount && limitCount > 0 {
		factor := min(max(backlog/limitCount, 1), 4)
		if scaled := limitCount * factor; scaled > 0 {
			limitCount = min(scaled, backlog)
		}
		if scaled := limitSize * int64(factor); scaled > 0 {
			limitSize = scaled
		}
	}

	addToBatch := func(cr *dbruntime.CommitRequest) {
		batch = append(batch, cr)
		pendingEntries += int64(cr.EntryCount)
		pendingBytes += cr.Size
	}

	addToBatch(first)
	if coalesceWait > 0 && cq.Len() == 0 && len(batch) < limitCount && pendingBytes < limitSize {
		time.Sleep(coalesceWait)
	}
	remaining := limitCount - len(batch)
	if remaining > 0 && pendingBytes < limitSize {
		cq.DrainReady(consumer, remaining, func(cr *dbruntime.CommitRequest) bool {
			addToBatch(cr)
			return pendingBytes < limitSize
		})
	}

	cq.AddPending(-pendingEntries, -pendingBytes)
	qLen := cq.Len()
	qEntries := cq.PendingEntries()
	qBytes := cq.PendingBytes()
	db.writeMetrics.UpdateQueue(qLen, int(qEntries), qBytes)
	return &dbruntime.CommitBatch{Reqs: batch, Pool: batchPtr}
}

func (db *DB) commitWorker() {
	defer db.commitWG.Done()
	consumer := db.commitQueue.Consumer()
	if consumer == nil {
		return
	}
	defer consumer.Close()
	for {
		batch := db.nextCommitBatch(consumer)
		if batch == nil {
			return
		}
		batch.BatchStart = time.Now()
		requests, totalEntries, totalSize, waitSum := db.collectCommitRequests(batch.Reqs, batch.BatchStart)
		if len(requests) == 0 {
			db.ackCommitBatch(batch.Reqs, batch.Pool, nil, -1, nil)
			continue
		}
		batch.Requests = requests
		if db.writeMetrics != nil {
			db.writeMetrics.RecordBatch(len(requests), totalEntries, totalSize, waitSum)
		}

		err := db.vlog.write(requests)
		if err != nil {
			db.ackCommitBatch(batch.Reqs, batch.Pool, requests, -1, err)
			continue
		}
		if db.writeMetrics != nil {
			batch.ValueLogDur = max(time.Since(batch.BatchStart), 0)
			if batch.ValueLogDur > 0 {
				db.writeMetrics.RecordValueLog(batch.ValueLogDur)
			}
		}

		failedAt, err := db.applyRequests(batch.Requests)
		if err == nil && db.syncQueue != nil {
			sb := &dbruntime.SyncBatch{
				Reqs:      batch.Reqs,
				Pool:      batch.Pool,
				Requests:  batch.Requests,
				FailedAt:  failedAt,
				ApplyDone: time.Now(),
			}
			batch.Reqs = nil
			batch.Pool = nil
			db.releaseCommitBatch(batch)
			db.syncQueue <- sb
			continue
		}

		if err == nil && db.opt.SyncWrites {
			syncStart := time.Now()
			err = db.wal.Sync()
			if db.writeMetrics != nil {
				db.writeMetrics.RecordSync(time.Since(syncStart), 1)
			}
		}

		if db.writeMetrics != nil {
			totalDur := max(time.Since(batch.BatchStart), 0)
			applyDur := max(totalDur-batch.ValueLogDur, 0)
			if applyDur > 0 {
				db.writeMetrics.RecordApply(applyDur)
			}
		}

		db.ackCommitBatch(batch.Reqs, batch.Pool, batch.Requests, failedAt, err)
	}
}

func (db *DB) syncWorker() {
	defer db.syncWG.Done()
	pending := make([]*dbruntime.SyncBatch, 0, 64)
	for first := range db.syncQueue {
		pending = append(pending, first)
	drain:
		for {
			select {
			case sb, ok := <-db.syncQueue:
				if !ok {
					break drain
				}
				pending = append(pending, sb)
			default:
				break drain
			}
		}

		syncStart := time.Now()
		syncErr := db.wal.Sync()
		if db.writeMetrics != nil {
			db.writeMetrics.RecordSync(time.Since(syncStart), len(pending))
		}
		for _, sb := range pending {
			db.ackCommitBatch(sb.Reqs, sb.Pool, sb.Requests, sb.FailedAt, syncErr)
		}
		pending = pending[:0]
	}
}

func (db *DB) ackCommitBatch(reqs []*dbruntime.CommitRequest, pool *[]*dbruntime.CommitRequest, requests []*dbruntime.Request, failedAt int, defaultErr error) {
	if defaultErr != nil && failedAt >= 0 && failedAt < len(requests) {
		perReqErr := make(map[*dbruntime.Request]error, len(requests)-failedAt)
		for i := failedAt; i < len(requests); i++ {
			if requests[i] != nil {
				perReqErr[requests[i]] = defaultErr
			}
		}
		db.finishCommitRequests(reqs, nil, perReqErr)
	} else {
		db.finishCommitRequests(reqs, defaultErr, nil)
	}
	if pool != nil {
		for i := range reqs {
			reqs[i] = nil
		}
		*pool = reqs[:0]
		db.commitBatchPool.Put(pool)
	}
}

func (db *DB) stopCommitWorkers() {
	db.commitQueue.Close()
	db.commitWG.Wait()
	if db.syncQueue != nil {
		close(db.syncQueue)
		db.syncWG.Wait()
	}
}

func (db *DB) collectCommitRequests(reqs []*dbruntime.CommitRequest, now time.Time) ([]*dbruntime.Request, int, int64, int64) {
	requests := make([]*dbruntime.Request, 0, len(reqs))
	var (
		totalEntries int
		totalSize    int64
		waitSum      int64
	)
	for _, cr := range reqs {
		if cr == nil || cr.Req == nil {
			continue
		}
		r := cr.Req
		requests = append(requests, r)
		totalEntries += len(r.Entries)
		totalSize += cr.Size
		if !r.EnqueueAt.IsZero() {
			waitSum += now.Sub(r.EnqueueAt).Nanoseconds()
			r.EnqueueAt = time.Time{}
		}
	}
	return requests, totalEntries, totalSize, waitSum
}

func (db *DB) releaseCommitBatch(batch *dbruntime.CommitBatch) {
	if batch == nil || batch.Pool == nil {
		return
	}
	batch.Requests = nil
	batch.BatchStart = time.Time{}
	batch.ValueLogDur = 0
	reqs := batch.Reqs
	for i := range reqs {
		reqs[i] = nil
	}
	*batch.Pool = reqs[:0]
	db.commitBatchPool.Put(batch.Pool)
}

func (db *DB) applyRequests(reqs []*dbruntime.Request) (int, error) {
	for i, r := range reqs {
		if r == nil || len(r.Entries) == 0 {
			continue
		}
		if err := db.writeToLSM(r); err != nil {
			return i, pkgerrors.Wrap(err, "writeRequests")
		}
		if len(r.PtrBuckets) == 0 {
			continue
		}
		db.Lock()
		db.updateHeadBuckets(r.PtrBuckets)
		db.Unlock()
	}
	return -1, nil
}

func (db *DB) finishCommitRequests(reqs []*dbruntime.CommitRequest, defaultErr error, perReqErr map[*dbruntime.Request]error) {
	for _, cr := range reqs {
		if cr == nil || cr.Req == nil {
			continue
		}
		if perReqErr != nil {
			if reqErr, ok := perReqErr[cr.Req]; ok {
				cr.Req.Err = reqErr
			} else {
				cr.Req.Err = defaultErr
			}
		} else {
			cr.Req.Err = defaultErr
		}
		cr.Req.WG.Done()
		cr.Req = nil
		cr.EntryCount = 0
		cr.Size = 0
		dbruntime.CommitRequestPool.Put(cr)
	}
}

func (db *DB) writeToLSM(b *dbruntime.Request) error {
	if len(b.PtrIdxs) == 0 {
		if len(b.Ptrs) != 0 && len(b.Ptrs) != len(b.Entries) {
			return pkgerrors.Errorf("Ptrs and Entries don't match: %+v", b)
		}
		return db.lsm.SetBatch(b.Entries)
	}
	if len(b.Ptrs) != len(b.Entries) {
		return pkgerrors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for _, idx := range b.PtrIdxs {
		entry := b.Entries[idx]
		entry.Meta = entry.Meta | kv.BitValuePointer
		entry.Value = b.Ptrs[idx].Encode()
	}
	if err := db.lsm.SetBatch(b.Entries); err != nil {
		return err
	}
	return nil
}

func (db *DB) requireOpenLSM() (*lsm.LSM, error) {
	if db == nil || db.IsClosed() || db.lsm == nil {
		return nil, fmt.Errorf("db: snapshot bridge requires open db")
	}
	return db.lsm, nil
}

func (db *DB) ExternalSSTOptions() *lsm.Options {
	lsmCore, err := db.requireOpenLSM()
	if err != nil {
		return nil
	}
	return lsmCore.ExternalSSTOptions()
}

func (db *DB) ImportExternalSST(paths []string) (*lsm.ExternalSSTImportResult, error) {
	lsmCore, err := db.requireOpenLSM()
	if err != nil {
		return nil, err
	}
	return lsmCore.ImportExternalSST(paths)
}

func (db *DB) RollbackExternalSST(fileIDs []uint64) error {
	lsmCore, err := db.requireOpenLSM()
	if err != nil {
		return err
	}
	return lsmCore.RollbackExternalSST(fileIDs)
}

func (db *DB) ExportSnapshotDir(dir string, region localmeta.RegionMeta) (*snapshotpkg.ExportResult, error) {
	if _, err := db.requireOpenLSM(); err != nil {
		return nil, err
	}
	return snapshotpkg.ExportDir(db, dir, region, nil)
}

func (db *DB) ImportSnapshotDir(dir string) (*snapshotpkg.ImportResult, error) {
	if _, err := db.requireOpenLSM(); err != nil {
		return nil, err
	}
	return snapshotpkg.ImportDir(db, dir, nil)
}

func (db *DB) ExportSnapshot(region localmeta.RegionMeta) ([]byte, error) {
	if _, err := db.requireOpenLSM(); err != nil {
		return nil, err
	}
	payload, _, err := snapshotpkg.ExportPayload(db, db.opt.WorkDir, region, nil)
	return payload, err
}

func (db *DB) ExportSnapshotTo(w io.Writer, region localmeta.RegionMeta) (snapshotpkg.Meta, error) {
	if _, err := db.requireOpenLSM(); err != nil {
		return snapshotpkg.Meta{}, err
	}
	return snapshotpkg.ExportPayloadTo(w, db, db.opt.WorkDir, region, nil)
}

func (db *DB) ImportSnapshot(payload []byte) (*snapshotpkg.ImportResult, error) {
	if _, err := db.requireOpenLSM(); err != nil {
		return nil, err
	}
	return snapshotpkg.ImportPayload(db, db.opt.WorkDir, payload, nil)
}

func (db *DB) ImportSnapshotFrom(r io.Reader) (*snapshotpkg.ImportResult, error) {
	if _, err := db.requireOpenLSM(); err != nil {
		return nil, err
	}
	return snapshotpkg.ImportPayloadFrom(db, db.opt.WorkDir, r, nil)
}
