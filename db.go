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
	"github.com/feichai0017/NoKV/metrics"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
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
// Total Manager budget under the LSM data-plane sharding plan
// (see docs/notes/2026-04-27-sharded-wal-memtable.md):
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

	// MVCCStore defines MVCC/internal operations consumed by percolator and raftstore.
	MVCCStore interface {
		ApplyInternalEntries(entries []*kv.Entry) error
		// GetInternalEntry returns a borrowed internal entry without cloning/copying.
		// entry.Key remains in internal encoding (cf+user_key+ts). Callers must
		// DecrRef exactly once.
		GetInternalEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error)
		NewInternalIterator(opt *index.Options) index.Iterator
	}

	// RaftLog opens raft peer storage without exposing the underlying raft WAL shards.
	RaftLog interface {
		Open(groupID uint64, meta *localmeta.Store) (raftlog.PeerStorage, error)
	}

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
		lsmWALs         []*wal.Manager
		lsmWatchdogs    []*wal.Watchdog
		raftWALMu       sync.Mutex
		raftWALs        [defaultRaftWALShards]*wal.Manager
		raftWatchdogs   [defaultRaftWALShards]*wal.Watchdog
		vlog            *vlogpkg.Consumer
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
		// pipeline owns the commit queue, per-shard dispatch channels,
		// processors, and the optional sync worker. See runtime/commit.
		pipeline        *commit.Pipeline
		iterPool        *iterpkg.IteratorPool
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
	db.headLogDelta = vlogpkg.HeadLogInterval
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
	if db.opt.EnableValueLog {
		db.initVLog()
	} else {
		// Operator diagnostic: when vlog is disabled but the manifest
		// still references value-log segments, every Get/iterator hit
		// on a value pointer will fail with a clear error. Surface the
		// mismatch here too so operators see it at Open time without
		// waiting for the first read failure.
		if status := db.lsm.ValueLogStatusSnapshot(); len(status) > 0 {
			slog.Default().Warn("value log disabled but manifest references existing vlog segments",
				"segments", len(status),
				"action", "set Options.EnableValueLog=true to read the existing data, or migrate values out")
		}
	}
	db.background.Init(stats.New(db, 0))
	if len(db.opt.ValueSeparationPolicies) > 0 {
		db.policyMatcher.Store(kv.NewValueSeparationPolicyMatcher(db.opt.ValueSeparationPolicies))
	}
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
		ValueThreshold:     int(db.opt.ValueThreshold),
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
		DiscardStatsCh:           &db.discardStatsCh,
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
	db.background.Start(dbruntime.BackgroundConfig{
		StartCompacter:     db.lsm.StartCompacter,
		EnableWALWatchdog:  db.opt.EnableWALWatchdog,
		WALWatchdogConfigs: watchdogConfigs,
		StartValueLogGC: func() {
			if db.opt.ValueLogGCInterval > 0 {
				if closer := db.vlogCloser(); closer != nil {
					closer.Add(1)
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
	for shard := range defaultRaftWALShards {
		if err := wal.VerifyDir(filepath.Join(db.opt.WorkDir, fmt.Sprintf("raft-wal-%02d", shard)), db.fs); err != nil {
			return err
		}
	}
	if db.opt.EnableValueLog {
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

	if closer := db.vlogCloser(); closer != nil {
		closer.Close()
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

	if db.vlog != nil {
		if err := db.vlog.Close(); err != nil {
			errs = append(errs, fmt.Errorf("vlog close: %w", err))
		}
		db.vlog = nil
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
	if db.vlog == nil {
		return nil, meta, fmt.Errorf("value pointer encountered but EnableValueLog is false; LSM still references vlog data, re-enable EnableValueLog to read it")
	}
	var vp kv.ValuePtr
	vp.Decode(src.Value)
	result, cb, err := db.vlog.Read(&vp)
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
	if db.vlog == nil {
		entry.DecrRef()
		return nil, fmt.Errorf("value pointer encountered but EnableValueLog is false; LSM still references vlog data, re-enable EnableValueLog to read it")
	}
	var vp kv.ValuePtr
	vp.Decode(entry.Value)
	result, cb, readErr := db.vlog.Read(&vp)
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

// NewIterator creates a DB-level iterator over user keys in the default
// column family. The state machine + Item materialization live in
// runtime/iterator; this method wires DB internals (lsm, vlog, iterPool)
// into iterpkg.New as a thin facade.
func (db *DB) NewIterator(opt *index.Options) index.Iterator {
	return iterpkg.New(iterpkg.Deps{
		Storage: db.lsm,
		Vlog:    db.vlog,
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
func (db *DB) Vlog() stats.VlogSource               { return db.vlog }
func (db *DB) LSMWALs() []*wal.Manager              { return db.lsmWALs }
func (db *DB) BackgroundWatchdogs() []*wal.Watchdog { return db.background.WALWatchdogs() }
func (db *DB) HotWrite() *thermos.RotatingThermos   { return db.hotWrite }
func (db *DB) IteratorReused() uint64               { return db.iterPool.Reused() }
func (db *DB) WriteMetrics() *metrics.WriteMetrics  { return db.writeMetrics }
func (db *DB) BlockWritesActive() bool              { return db.blockWrites.Load() == 1 }
func (db *DB) SlowWritesActive() bool               { return db.slowWrites.Load() == 1 }
func (db *DB) HotWriteLimited() uint64              { return db.hotWriteLimited.Load() }
func (db *DB) ValueLogDisabledOrphans() int {
	if db == nil || db.lsm == nil || db.opt.EnableValueLog {
		return 0
	}
	return len(db.lsm.ValueLogStatusSnapshot())
}
func (db *DB) RaftLagWarnSegments() int64        { return db.opt.RaftLagWarnSegments }
func (db *DB) WALTypedRecordWarnRatio() float64  { return db.opt.WALTypedRecordWarnRatio }
func (db *DB) WALTypedRecordWarnSegments() int64 { return db.opt.WALTypedRecordWarnSegments }
func (db *DB) ThermosTopK() int                  { return db.opt.ThermosTopK }

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

// commit.Host implementation: read-only accessors plus UpdateHeadBuckets
// the commit Pipeline uses to wire DB-side state without importing the
// root NoKV package.

func (db *DB) ThrottleSignal() <-chan struct{} {
	db.throttleMu.Lock()
	ch := db.throttleCh
	db.throttleMu.Unlock()
	return ch
}

// commit.Host LSM/Vlog use narrower interfaces than stats.Host LSM/Vlog;
// expose them via differently-named accessors that satisfy the commit
// surface without colliding with stats.Host method names.
func (db *DB) CommitLSM() commit.LSM   { return db.lsm }
func (db *DB) CommitVlog() commit.Vlog { return db.vlog }

func (db *DB) UpdateHeadBuckets(buckets []uint32) {
	db.Lock()
	db.updateHeadBuckets(buckets)
	db.Unlock()
}

// RunValueLogGC triggers a value log garbage collection. No-op (returns
// nil) when EnableValueLog is false — callers can wire this into a
// scheduler unconditionally.
func (db *DB) RunValueLogGC(discardRatio float64) error {
	if db == nil || db.vlog == nil {
		return nil
	}
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
		for bucket, mgr := range db.vlog.Managers() {
			if mgr == nil {
				continue
			}
			heads[uint32(bucket)] = mgr.Head()
		}
	}
	// Pick a log file and run GC
	if err := db.vlog.RunGC(discardRatio, heads); err != nil {
		if stderrors.Is(err, utils.ErrEmptyKey) {
			return nil
		}
		return err
	}
	return nil
}

func (db *DB) runValueLogGCPeriodically() {
	closer := db.vlogCloser()
	if closer == nil {
		return
	}
	defer closer.Done()

	ticker := time.NewTicker(db.opt.ValueLogGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := db.RunValueLogGC(db.opt.ValueLogGCDiscardRatio)
			if err != nil {
				if err == utils.ErrNoRewrite {
					db.vlog.Logf("No rewrite on GC.")
				} else {
					slog.Default().Warn("value log gc", "error", err)
				}
			}
		case <-closer.Closed():
			return
		}
	}
}

// vlogCloser exposes the value-log Consumer's lifecycle Closer (or nil
// when vlog is disabled) so DB-side background loops can register on
// the same shutdown signal as the discard-stats flush worker.
func (db *DB) vlogCloser() *utils.Closer {
	if db == nil || db.vlog == nil {
		return nil
	}
	return db.vlog.BackgroundCloser()
}

// initVLog opens the value-log Consumer with all the DB-side hooks the
// engine/vlog package needs (LSM access, batch writeback, value-policy
// matcher, etc.). Heads observed during replay are installed into
// db.vheads / db.lastLoggedHeads here so the Consumer never reaches
// back into the DB struct.
func (db *DB) initVLog() {
	heads := db.getHeads()
	vlogDir := filepath.Join(db.opt.WorkDir, "vlog")

	cfg := vlogpkg.ConsumerConfig{
		Dir:                        vlogDir,
		BucketCount:                max(db.opt.ValueLogBucketCount, 1),
		GCParallelism:              db.opt.ValueLogGCParallelism,
		NumCompactors:              db.opt.NumCompactors,
		FileSize:                   db.opt.ValueLogFileSize,
		Verbose:                    db.opt.ValueLogVerbose,
		SyncWrites:                 db.opt.SyncWrites,
		DiscardStatsFlushThreshold: db.opt.DiscardStatsFlushThreshold,
		GCReduceScore:              db.opt.ValueLogGCReduceScore,
		GCSkipScore:                db.opt.ValueLogGCSkipScore,
		GCReduceBacklog:            db.opt.ValueLogGCReduceBacklog,
		GCSkipBacklog:              db.opt.ValueLogGCSkipBacklog,
		GCSampleSizeRatio:          db.opt.ValueLogGCSampleSizeRatio,
		GCSampleCountRatio:         db.opt.ValueLogGCSampleCountRatio,
		GCSampleFromHead:           db.opt.ValueLogGCSampleFromHead,
		GCMaxEntries:               db.opt.ValueLogMaxEntries,
		MaxBatchCount:              db.opt.MaxBatchCount,
		MaxBatchSize:               db.opt.MaxBatchSize,
		DiscardStatsCh:             db.discardStatsCh,
		FS:                         db.fs,
	}
	deps := vlogpkg.Deps{
		LSM:              db.lsm,
		GetInternalEntry: db.GetInternalEntry,
		BatchSet:         db.batchSet,
		SendToWriteCh: func(entries []*kv.Entry, waitOnThrottle bool) (vlogpkg.Waiter, error) {
			req, err := db.sendToWriteCh(entries, waitOnThrottle)
			if err != nil {
				return nil, err
			}
			return req, nil
		},
		PeekShouldWriteValueToLSM: db.peekShouldWriteValueToLSM,
	}

	// lastLoggedHeads tracks what is actually persisted in the manifest,
	// so it must be primed *only* from the manifest snapshot — not from
	// the manager-initial heads we discover during replay. Otherwise the
	// first updateHeadBuckets would short-circuit shouldPersistHead and
	// the bucket head would never reach the manifest.
	db.lastLoggedHeads = make(map[uint32]kv.ValuePtr, len(heads))
	maps.Copy(db.lastLoggedHeads, heads)

	vlog, observed, err := vlogpkg.OpenConsumer(cfg, deps, heads, nil)
	utils.Panic(err)
	db.vlog = vlog
	if db.vheads == nil {
		db.vheads = make(map[uint32]kv.ValuePtr)
	}
	for bucket, head := range observed {
		if existing, ok := db.vheads[bucket]; ok && !existing.IsZero() {
			continue
		}
		db.vheads[bucket] = head
	}
}

// getHeads returns the value-log head snapshot held by the LSM manifest.
// Empty result means no vlog heads have ever been logged.
func (db *DB) getHeads() map[uint32]kv.ValuePtr {
	heads := db.lsm.ValueLogHeadSnapshot()
	if len(heads) == 0 {
		return make(map[uint32]kv.ValuePtr)
	}
	return heads
}

// valueLogStatusSnapshot returns the manifest's view of every value-log
// segment (bucket, fid, valid). Nil when LSM is closed.
func (db *DB) valueLogStatusSnapshot() map[manifest.ValueLogID]manifest.ValueLogMeta {
	if db == nil || db.lsm == nil {
		return nil
	}
	return db.lsm.ValueLogStatusSnapshot()
}

// updateHeadBuckets advances db.vheads / db.lastLoggedHeads after the
// commit pipeline appended new value-log entries. For each bucket we
// fetch the live Manager head, panic on regression (impossible unless a
// concurrent rewind happened), and persist to the manifest only when the
// head moved past the headLogDelta threshold.
func (db *DB) updateHeadBuckets(buckets []uint32) {
	if len(buckets) == 0 {
		return
	}
	if db.vlog == nil {
		return
	}
	if db.vheads == nil {
		db.vheads = make(map[uint32]kv.ValuePtr)
	}
	if db.lastLoggedHeads == nil {
		db.lastLoggedHeads = make(map[uint32]kv.ValuePtr)
	}
	for _, bucket := range buckets {
		mgr, err := db.vlog.ManagerFor(bucket)
		if err != nil {
			continue
		}
		head := mgr.Head()
		if head.IsZero() {
			continue
		}
		next := &kv.ValuePtr{Bucket: bucket, Fid: head.Fid, Offset: head.Offset, Len: head.Len}
		if prev, ok := db.vheads[bucket]; ok && next.Less(&prev) {
			utils.CondPanicFunc(true, func() error {
				return fmt.Errorf("value log head regression: bucket=%d prev=%+v next=%+v", bucket, prev, next)
			})
		}
		db.vheads[bucket] = *next
		if !db.shouldPersistHead(next, bucket) {
			continue
		}
		if err := db.lsm.LogValueLogHead(next); err != nil {
			slog.Default().Error("log value log head", "bucket", bucket, "error", err)
			continue
		}
		metrics.DefaultValueLogGCCollector().IncHeadUpdates()
		db.lastLoggedHeads[bucket] = *next
	}
}

// shouldPersistHead gates manifest writes by db.headLogDelta: skip the
// log entry unless the new head moved at least headLogDelta bytes past
// the last logged offset (or the FID rolled over).
func (db *DB) shouldPersistHead(next *kv.ValuePtr, bucket uint32) bool {
	if db == nil || next == nil || next.IsZero() {
		return false
	}
	if db.headLogDelta == 0 {
		return true
	}
	last := db.lastLoggedHeads[bucket]
	if last.IsZero() {
		return true
	}
	if next.Fid != last.Fid {
		return true
	}
	if next.Offset < last.Offset {
		return true
	}
	if next.Offset-last.Offset >= db.headLogDelta {
		return true
	}
	return false
}

// RequestsHaveVlogWork reports whether any entry in any request needs to be
// offloaded to the value log. When this returns false the commit pipeline can
// skip db.vlog.write entirely — saving a function call, the heads/touched
// map allocations, the per-request bucketEntries map, and the rewind closure
// preparation. This is the metadata-profile fast path: when every value is
// inline-eligible (below ValueThreshold or pinned inline by a policy), the
// commit pipeline runs as pure WAL → memtable → SST with no vlog code on the
// hot path. See docs/notes/2026-04-27-slab-substrate.md §4 (Phase 1).
//
// This call counts a policy decision per entry (via shouldWriteValueToLSM →
// MatchPolicy). vlog.write subsequently uses peekShouldWriteValueToLSM so the
// per-entry decision is recorded exactly once across the pipeline.
//
// When EnableValueLog is false (the default) this returns false
// immediately — the metadata-profile deployment never enters vlog code.
func (db *DB) RequestsHaveVlogWork(reqs []*dbruntime.Request) bool {
	if db == nil || db.vlog == nil {
		return false
	}
	for _, req := range reqs {
		if req == nil {
			continue
		}
		for _, e := range req.Entries {
			if !db.shouldWriteValueToLSM(e) {
				return true
			}
		}
	}
	return false
}

// peekShouldWriteValueToLSM mirrors shouldWriteValueToLSM but does not
// record a policy-matcher decision. Used by vlog.write to avoid double-
// counting the per-entry decision that the commit pipeline's pre-scan
// already recorded via RequestsHaveVlogWork.
func (db *DB) peekShouldWriteValueToLSM(e *kv.Entry) bool {
	if e.IsRangeDelete() {
		return true
	}
	matcher := db.policyMatcher.Load()
	if matcher != nil {
		if policy := matcher.PeekPolicy(e); policy != nil {
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
	return int64(len(e.Value)) < db.opt.ValueThreshold
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
// so legacy call sites (vlog SendToWriteCh dep, db_bench_test.go) and
// internal batch helpers (batchSet) keep their current names without
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
