// Package NoKV provides the embedded database API and engine wiring.
package NoKV

import (
	"bytes"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
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
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/metrics"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	dbruntime "github.com/feichai0017/NoKV/runtime"
	"github.com/feichai0017/NoKV/runtime/commit"
	iterpkg "github.com/feichai0017/NoKV/runtime/iterator"
	"github.com/feichai0017/NoKV/runtime/stats"
	"github.com/feichai0017/NoKV/thermos"
	"github.com/feichai0017/NoKV/utils"
)

// nonTxnMaxVersion is the read upper-bound sentinel used by non-transactional APIs.
// Non-transactional writes use monotonic versions <= this sentinel.
const nonTxnMaxVersion = kv.MaxVersion

// defaultRaftWALShards controls the number of WAL Manager instances that
// back the raft control-plane fan-out. Each shard is one fd + one fsync
// worker + one bufio.Writer, so the count is a tradeoff between fd cost
// and per-Manager.mu contention. Must be a power of two — raftWALShard
// uses `& (N-1)` for placement.
//
// Total Manager budget under the LSM data-plane sharding plan:
// 4 raft + 4 LSM data = 8 Managers. There is no separate control-plane
// Manager — db.wal is dissolved into the LSM shards.
const defaultRaftWALShards = 4

type (
	// BatchSetItem represents one non-transactional write in the default CF.
	//
	// Ownership note: key is copied into the internal-key encoding; value is
	// referenced directly until the write path finishes.
	BatchSetItem struct {
		Key   []byte
		Value []byte
	}

	// RaftLog opens raft peer storage without exposing the underlying raft WAL shards.
	RaftLog interface {
		Open(groupID uint64, meta *localmeta.Store) (raftlog.PeerStorage, error)
	}

	mvccMaintenanceSnapshotSource func() storemvcc.MaintenanceSnapshot

	// DB is the global handle for the engine and owns shared resources.
	DB struct {
		sync.RWMutex
		opt     *Options
		fs      vfs.FS
		dirLock io.Closer
		lsm     *lsm.LSM
		// lsmWALs holds one WAL Manager per LSM data-plane shard. The
		// number of entries is db.opt.LSMShardCount (resolved at Open).
		// Each Manager has its own fd, fsync worker, and bufio.Writer so
		// commit workers do not contend on a single Manager.mu.
		lsmWALs       []*wal.Manager
		lsmWatchdogs  []*wal.Watchdog
		raftWALMu     sync.Mutex
		raftWALs      [defaultRaftWALShards]*wal.Manager
		raftWatchdogs [defaultRaftWALShards]*wal.Watchdog
		nonTxnVersion atomic.Uint64
		blockWrites   atomic.Int32
		slowWrites    atomic.Int32
		isClosed      atomic.Uint32
		closeOnce     sync.Once
		closeErr      error
		throttleMu    sync.Mutex
		throttleCh    chan struct{}
		hotWrite      *thermos.RotatingThermos
		writeMetrics  *metrics.WriteMetrics
		// pipeline owns the commit queue, per-shard dispatch channels,
		// processors, and the optional sync worker. See runtime/commit.
		pipeline        *commit.Pipeline
		iterPool        *iterpkg.IteratorPool
		raftMode        raftmode.Mode
		mvccGCPlan      *storemvcc.GCPlanner
		mvccMaintenance atomic.Value
		hotWriteLimited atomic.Uint64
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

	n := db.opt.LSMShardCount
	if n <= 0 {
		return fmt.Errorf("open db: LSMShardCount must be > 0")
	}
	db.lsmWALs = make([]*wal.Manager, n)
	for shard := range n {
		dir := db.lsmWALDir(shard)
		if err := db.fs.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("open db: ensure lsm wal dir %d: %w", shard, err)
		}
		mgr, err := wal.Open(wal.Config{
			Dir:        dir,
			BufferSize: db.opt.WALBufferSize,
			FS:         db.fs,
		})
		if err != nil {
			return fmt.Errorf("open db: lsm wal shard %d: %w", shard, err)
		}
		db.lsmWALs[shard] = mgr
	}
	return nil
}

// lsmWALDir returns the per-shard WAL directory for the LSM data plane.
func (db *DB) lsmWALDir(shard int) string {
	return filepath.Join(db.opt.WorkDir, fmt.Sprintf("lsm-wal-%02d", shard))
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
		db.raftMode = mode
		return nil
	}
	if len(db.opt.AllowedModes) == 0 {
		return fmt.Errorf("open db: workdir mode %q requires explicit distributed open intent", mode)
	}
	return fmt.Errorf("open db: workdir mode %q is not allowed for this open intent", mode)
}

func (db *DB) openEngine() error {
	lsmCore, err := lsm.NewLSM(db.runtimeLSMOptions(), db.lsmWALs)
	if err != nil {
		return fmt.Errorf("open db: lsm init: %w", err)
	}
	db.lsm = lsmCore
	db.nonTxnVersion.Store(db.lsm.Diagnostics().MaxVersion)
	db.iterPool = iterpkg.NewIteratorPool()
	db.background.Init(stats.New(db, 0))
	return nil
}

func (db *DB) startWriteRuntime() {
	// One commit processor per LSM data-plane shard. Each processor
	// owns its shard's WAL Manager — no cross-shard sharing means no
	// Manager.mu contention on the hot write path. The dispatcher fans
	// batches out by per-key affinity so each batch lives on exactly
	// one shard (preserving SetBatch atomicity).
	workers := db.opt.LSMShardCount
	if workers <= 0 {
		workers = 1
	}
	db.pipeline = commit.New(commit.Config{
		ShardCount:         workers,
		SyncWrites:         db.opt.SyncWrites,
		SyncPipeline:       db.opt.SyncPipeline,
		MaxBatchCount:      db.opt.MaxBatchCount,
		MaxBatchSize:       db.opt.MaxBatchSize,
		WriteBatchMaxCount: db.opt.WriteBatchMaxCount,
		WriteBatchMaxSize:  db.opt.WriteBatchMaxSize,
		WriteBatchWait:     db.opt.WriteBatchWait,
	}, db)
	db.pipeline.Start()
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
		ManifestSync:             db.opt.ManifestSync,
		ManifestRewriteThreshold: db.opt.ManifestRewriteThreshold,
		ThrottleCallback:         db.ApplyThrottle,
	}
	db.opt.applyLSMSharedOptions(cfg)
	return cfg
}

func (l dbRaftLog) Open(groupID uint64, meta *localmeta.Store) (raftlog.PeerStorage, error) {
	walMgr, err := l.db.raftWALFor(groupID)
	if err != nil {
		return nil, err
	}
	return raftlog.OpenWALStorage(raftlog.WALStorageConfig{
		GroupID:   groupID,
		WAL:       walMgr,
		LocalMeta: meta,
	})
}

func (db *DB) raftWALFor(groupID uint64) (*wal.Manager, error) {
	if db == nil || db.opt == nil {
		return nil, fmt.Errorf("db raft wal: nil db")
	}
	shard := raftWALShard(groupID)
	db.raftWALMu.Lock()
	defer db.raftWALMu.Unlock()
	if mgr := db.raftWALs[shard]; mgr != nil {
		return mgr, nil
	}
	mgr, err := wal.Open(wal.Config{
		Dir:        db.raftWALDir(shard),
		BufferSize: db.opt.WALBufferSize,
		FS:         db.fs,
	})
	if err != nil {
		return nil, err
	}
	if db.opt.RaftPointerSnapshot != nil {
		if err := mgr.RegisterRetention("raft", func() wal.RetentionMark {
			return raftRetentionMarkForShard(db.opt.RaftPointerSnapshot(), shard)
		}); err != nil {
			_ = mgr.Close()
			return nil, err
		}
	}
	if db.opt.EnableWALWatchdog {
		wd := wal.NewWatchdog(wal.WatchdogConfig{
			Manager:      mgr,
			Interval:     db.opt.WALAutoGCInterval,
			MinRemovable: db.opt.WALAutoGCMinRemovable,
			MaxBatch:     db.opt.WALAutoGCMaxBatch,
			WarnRatio:    db.opt.WALTypedRecordWarnRatio,
			WarnSegments: db.opt.WALTypedRecordWarnSegments,
		})
		if wd != nil {
			wd.Start()
			db.raftWatchdogs[shard] = wd
		}
	}
	db.raftWALs[shard] = mgr
	return mgr, nil
}

func (db *DB) raftWALDir(shard int) string {
	return filepath.Join(db.opt.WorkDir, fmt.Sprintf("raft-wal-%02d", shard))
}

func raftWALShard(groupID uint64) int {
	const mix = 11400714819323198485
	return int((groupID * mix) & (defaultRaftWALShards - 1))
}

func raftRetentionMarkForShard(ptrs map[uint64]localmeta.RaftLogPointer, shard int) wal.RetentionMark {
	filtered := make(map[uint64]localmeta.RaftLogPointer)
	for groupID, ptr := range ptrs {
		if raftWALShard(groupID) == shard {
			filtered[groupID] = ptr
		}
	}
	return raftRetentionMark(filtered)
}

// RaftLog returns the raft peer-storage capability backed by sharded raft WALs.
func (db *DB) RaftLog() RaftLog {
	if db == nil || len(db.lsmWALs) == 0 {
		return nil
	}
	return dbRaftLog{db: db}
}

// RaftMode returns the persisted lifecycle mode observed when the DB was opened.
func (db *DB) RaftMode() raftmode.Mode {
	if db == nil {
		return ""
	}
	return db.raftMode
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
	watchdogConfigs := make([]wal.WatchdogConfig, 0, len(db.lsmWALs))
	for _, mgr := range db.lsmWALs {
		if mgr == nil {
			continue
		}
		watchdogConfigs = append(watchdogConfigs, wal.WatchdogConfig{
			Manager:      mgr,
			Interval:     db.opt.WALAutoGCInterval,
			MinRemovable: db.opt.WALAutoGCMinRemovable,
			MaxBatch:     db.opt.WALAutoGCMaxBatch,
			WarnRatio:    db.opt.WALTypedRecordWarnRatio,
			WarnSegments: db.opt.WALTypedRecordWarnSegments,
		})
	}
	var periodicTasks []dbruntime.PeriodicTaskConfig
	if task, state, ok := storemvcc.NewGCPlanTask(storemvcc.GCPlanConfig{
		MVCCStore: db,
		Interval:  db.opt.MVCCGCPlanInterval,
		SafePoint: db.opt.MVCCGCSafePointFn,
		Retention: db.opt.MVCCGCSnapshotRetentionFn,
		Mount:     fsmeta.StringMountResolver,
	}); ok {
		db.mvccGCPlan = state
		periodicTasks = append(periodicTasks, task)
	}
	db.background.Start(dbruntime.BackgroundConfig{
		StartCompacter:     db.lsm.StartCompacter,
		EnableWALWatchdog:  db.opt.EnableWALWatchdog,
		WALWatchdogConfigs: watchdogConfigs,
		PeriodicTasks:      periodicTasks,
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
	for shard := range defaultRaftWALShards {
		if err := wal.VerifyDir(filepath.Join(db.opt.WorkDir, fmt.Sprintf("raft-wal-%02d", shard)), db.fs); err != nil {
			return err
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

	if db.pipeline != nil {
		db.pipeline.Close()
	}
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

	if err := db.closeRaftWALs(); err != nil {
		errs = append(errs, err)
	}

	for shard, wd := range db.lsmWatchdogs {
		if wd != nil {
			wd.Stop()
			db.lsmWatchdogs[shard] = nil
		}
	}
	for shard, mgr := range db.lsmWALs {
		if mgr == nil {
			continue
		}
		if err := mgr.Close(); err != nil {
			errs = append(errs, fmt.Errorf("lsm wal shard %d close: %w", shard, err))
		}
		db.lsmWALs[shard] = nil
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

func (db *DB) closeRaftWALs() error {
	db.raftWALMu.Lock()
	defer db.raftWALMu.Unlock()
	var errs []error
	for shard, wd := range db.raftWatchdogs {
		if wd != nil {
			wd.Stop()
			db.raftWatchdogs[shard] = nil
		}
	}
	for shard, mgr := range db.raftWALs {
		if mgr == nil {
			continue
		}
		if err := mgr.Close(); err != nil {
			errs = append(errs, fmt.Errorf("raft wal shard %d close: %w", shard, err))
		}
		db.raftWALs[shard] = nil
	}
	return stderrors.Join(errs...)
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
// canonical internal-key layout. Legacy value-log pointers are rejected because
// new storage records keep values inline.
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
	return nil, meta, utils.ErrUnsupportedValueLog
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

// loadBorrowedEntry fetches one internal-key record from LSM.
//
// Ownership contract:
//   - The returned entry is a borrowed, pool-managed object.
//   - The caller MUST call DecrRef exactly once when finished.
//
// Error behavior:
//   - Returns ErrKeyNotFound when no record exists.
//   - If a legacy value-log pointer is encountered, this function releases the
//     borrowed entry and returns ErrUnsupportedValueLog.
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
	entry.DecrRef()
	return nil, utils.ErrUnsupportedValueLog
}

// NewIterator creates a DB-level iterator over user keys in the default
// column family. The state machine + Item materialization live in
// runtime/iterator; this method wires DB internals (lsm, iterPool)
// into iterpkg.New as a thin facade.
func (db *DB) NewIterator(opt *index.Options) index.Iterator {
	return iterpkg.New(iterpkg.Deps{
		Storage: db.lsm,
		Pool:    db.iterPool,
	}, opt)
}

// NewInternalIterator returns an iterator over internal keys (CF marker +
// user key + timestamp). Callers should decode kv.Entry.Key via
// kv.SplitInternalKey and handle ok=false.
func (db *DB) NewInternalIterator(opt *index.Options) index.Iterator {
	return iterpkg.NewInternal(db.lsm, opt)
}

// Info returns the live stats collector for snapshot/diagnostic access.
func (db *DB) Info() *stats.Stats {
	if db == nil {
		return nil
	}
	s, _ := db.background.StatsCollector().(*stats.Stats)
	return s
}

// stats.Host implementation: read-only accessors the stats subsystem
// uses to assemble a StatsSnapshot. They are intentionally a thin lift
// over DB struct fields so the snapshot logic can live in runtime/stats
// without importing the root NoKV package.

func (db *DB) LSM() stats.LSMSource                 { return db.lsm }
func (db *DB) LSMWALs() []*wal.Manager              { return db.lsmWALs }
func (db *DB) BackgroundWatchdogs() []*wal.Watchdog { return db.background.WALWatchdogs() }
func (db *DB) MVCCGCPlanSnapshot() storemvcc.GCPlanSnapshot {
	if db == nil || db.mvccGCPlan == nil {
		return storemvcc.GCPlanSnapshot{}
	}
	return db.mvccGCPlan.Snapshot(db.background.PeriodicTaskSnapshot(storemvcc.GCPlanTaskName))
}

// SetMVCCMaintenanceSnapshotSource attaches the raftstore-owned replicated
// MVCC maintenance observer used by runtime stats. The source is read-only:
// DB only calls it while building snapshots and never owns the worker lifecycle.
func (db *DB) SetMVCCMaintenanceSnapshotSource(source func() storemvcc.MaintenanceSnapshot) {
	if db == nil || source == nil {
		return
	}
	db.mvccMaintenance.Store(mvccMaintenanceSnapshotSource(source))
}

func (db *DB) MVCCMaintenanceSnapshot() storemvcc.MaintenanceSnapshot {
	if db == nil {
		return storemvcc.MaintenanceSnapshot{}
	}
	v := db.mvccMaintenance.Load()
	if v == nil {
		return storemvcc.MaintenanceSnapshot{}
	}
	source, ok := v.(mvccMaintenanceSnapshotSource)
	if !ok || source == nil {
		return storemvcc.MaintenanceSnapshot{}
	}
	return source()
}

func (db *DB) HotWrite() *thermos.RotatingThermos  { return db.hotWrite }
func (db *DB) IteratorReused() uint64              { return db.iterPool.Reused() }
func (db *DB) WriteMetrics() *metrics.WriteMetrics { return db.writeMetrics }
func (db *DB) BlockWritesActive() bool             { return db.blockWrites.Load() == 1 }
func (db *DB) SlowWritesActive() bool              { return db.slowWrites.Load() == 1 }
func (db *DB) HotWriteLimited() uint64             { return db.hotWriteLimited.Load() }
func (db *DB) RaftLagWarnSegments() int64          { return db.opt.RaftLagWarnSegments }
func (db *DB) WALTypedRecordWarnRatio() float64    { return db.opt.WALTypedRecordWarnRatio }
func (db *DB) WALTypedRecordWarnSegments() int64   { return db.opt.WALTypedRecordWarnSegments }
func (db *DB) ThermosTopK() int                    { return db.opt.ThermosTopK }

func (db *DB) RaftPointerSnapshot() func() map[uint64]localmeta.RaftLogPointer {
	if db == nil || db.opt == nil {
		return nil
	}
	return db.opt.RaftPointerSnapshot
}

func (db *DB) RaftWALsLocked(fn func(wals []*wal.Manager)) {
	db.raftWALMu.Lock()
	defer db.raftWALMu.Unlock()
	fn(db.raftWALs[:])
}

// commit.Host implementation: read-only accessors the commit Pipeline uses
// without importing the root NoKV package.

func (db *DB) ThrottleSignal() <-chan struct{} {
	db.throttleMu.Lock()
	ch := db.throttleCh
	db.throttleMu.Unlock()
	return ch
}

func (db *DB) CommitLSM() commit.LSM { return db.lsm }

// SetRegionMetrics attaches region metrics recorder so Stats snapshot and expvar
// include region state counts.
func (db *DB) SetRegionMetrics(rm *metrics.RegionMetrics) {
	if db == nil {
		return
	}
	db.background.SetRegionMetrics(rm)
}

// SyncWAL fans an fsync across every LSM data-plane WAL Manager. Used by
// administrative durability calls; the hot write path syncs only the
// shard the commit processor is pinned to.
func (db *DB) SyncWAL() error {
	if db == nil || len(db.lsmWALs) == 0 {
		return fmt.Errorf("db: wal is unavailable")
	}
	var errs []error
	for shard, mgr := range db.lsmWALs {
		if mgr == nil {
			continue
		}
		if err := mgr.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("lsm wal shard %d sync: %w", shard, err))
		}
	}
	return stderrors.Join(errs...)
}

// ReplayWAL replays every LSM data-plane WAL Manager in shard order.
// Cross-shard ordering is not preserved — callers must rely on internal-key
// MVCC timestamps for record ordering.
func (db *DB) ReplayWAL(fn func(info wal.EntryInfo, payload []byte) error) error {
	if db == nil || len(db.lsmWALs) == 0 {
		return fmt.Errorf("db: wal is unavailable")
	}
	for shard, mgr := range db.lsmWALs {
		if mgr == nil {
			continue
		}
		if err := mgr.Replay(fn); err != nil {
			return fmt.Errorf("lsm wal shard %d replay: %w", shard, err)
		}
	}
	return nil
}

// IsClosed reports whether Close has finished and the DB no longer accepts work.
func (db *DB) IsClosed() bool {
	return db.isClosed.Load() == 1
}

func (db *DB) ApplyThrottle(state lsm.WriteThrottleState) {
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

// sendToWriteCh delegates to the commit Pipeline. Kept as a thin facade
// so internal batch helpers (batchSet) keep their current names without
// reaching into runtime/commit directly.
func (db *DB) sendToWriteCh(entries []*kv.Entry, waitOnThrottle bool) (*dbruntime.Request, error) {
	return db.pipeline.Send(entries, waitOnThrottle)
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
