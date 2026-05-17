// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package local provides the embedded single-node database API and engine wiring.
package local

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
	"github.com/feichai0017/NoKV/local/internal/commit"
	iterpkg "github.com/feichai0017/NoKV/local/internal/iterator"
	"github.com/feichai0017/NoKV/local/stats"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/thermos"
	"github.com/feichai0017/NoKV/utils"
)

// nonTxnMaxVersion is the read upper-bound sentinel used by non-transactional APIs.
// Non-transactional writes use monotonic versions <= this sentinel.
const nonTxnMaxVersion = kv.MaxVersion

// defaultControlWALShards controls the number of WAL Manager instances that
// back replicated control-log fan-out. Each shard is one fd + one fsync
// worker + one bufio.Writer, so the count is a tradeoff between fd cost
// and per-Manager.mu contention. Must be a power of two — controlWALShard
// uses `& (N-1)` for placement.
//
// Total Manager budget under the LSM data-plane sharding plan:
// 4 control-log + 4 LSM data = 8 Managers. There is no separate control-plane
// Manager — db.wal is dissolved into the LSM shards.
const defaultControlWALShards = 4

type (
	// BatchSetItem represents one non-transactional write in the default CF.
	//
	// Ownership note: key is copied into the internal-key encoding; value is
	// referenced directly until the write path finishes.
	BatchSetItem struct {
		Key   []byte
		Value []byte
	}

	mvccGCStatsSnapshotSource func() stats.MVCCGCStatsSnapshot
	transportMetricsSource    func() metrics.GRPCTransportMetrics

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
		lsmWALs          []*wal.Manager
		lsmWatchdogs     []*wal.Watchdog
		controlWALMu     sync.Mutex
		controlWALs      [defaultControlWALShards]*wal.Manager
		controlWatchdogs [defaultControlWALShards]*wal.Watchdog
		nonTxnVersion    atomic.Uint64
		blockWrites      atomic.Int32
		slowWrites       atomic.Int32
		isClosed         atomic.Uint32
		closeOnce        sync.Once
		closeErr         error
		throttleMu       sync.Mutex
		throttleCh       chan struct{}
		hotWrite         *thermos.RotatingThermos
		writeMetrics     *metrics.WriteMetrics
		// pipeline owns the commit queue, per-shard dispatch channels,
		// processors, and the optional sync worker.
		pipeline        *commit.Pipeline
		iterPool        *iterpkg.IteratorPool
		workdirMode     workdirmode.Mode
		mvccGCStats     atomic.Value
		transportStats  atomic.Value
		hotWriteLimited atomic.Uint64
		background      backgroundServices
	}
)

func newDB(opt *Options) *DB {
	cfg := opt
	if cfg == nil {
		cfg = &Options{}
	}
	db := &DB{opt: cfg, writeMetrics: metrics.NewWriteMetrics()}
	db.fs = vfs.Ensure(cfg.FS)
	db.throttleCh = make(chan struct{})
	db.hotWrite = commit.NewHotWriteRing(commit.HotWriteConfig{
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

func controlRetentionMark(ptrs map[uint64]stats.ControlLogPointer) wal.RetentionMark {
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
	mode, err := workdirmode.ReadOnlyMode(db.opt.WorkDir)
	if err != nil {
		return fmt.Errorf("open db: read workdir mode: %w", err)
	}
	if workdirmode.Allowed(db.opt.AllowedModes, mode) {
		db.workdirMode = mode
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
		UserKeyShardKey:    db.userKeyShardKey,
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

func (db *DB) controlWALFor(groupID uint64) (*wal.Manager, error) {
	if db == nil || db.opt == nil {
		return nil, fmt.Errorf("db control wal: nil db")
	}
	if db.IsClosed() {
		return nil, fmt.Errorf("db control wal: closed db")
	}
	shard := controlWALShard(groupID)
	db.controlWALMu.Lock()
	defer db.controlWALMu.Unlock()
	if mgr := db.controlWALs[shard]; mgr != nil {
		return mgr, nil
	}
	mgr, err := wal.Open(wal.Config{
		Dir:        db.controlWALDir(shard),
		BufferSize: db.opt.WALBufferSize,
		FS:         db.fs,
	})
	if err != nil {
		return nil, err
	}
	if db.opt.ControlLogPointerSnapshot != nil {
		if err := mgr.RegisterRetention("control-log", func() wal.RetentionMark {
			return controlRetentionMarkForShard(db.opt.ControlLogPointerSnapshot(), shard)
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
			db.controlWatchdogs[shard] = wd
		}
	}
	db.controlWALs[shard] = mgr
	return mgr, nil
}

// OpenControlWAL returns one sharded WAL manager for replicated control-log
// adapters. The DB owns durability and retention; callers define the log
// semantics above this storage boundary.
func (db *DB) OpenControlWAL(groupID uint64) (*wal.Manager, error) {
	return db.controlWALFor(groupID)
}

func (db *DB) controlWALDir(shard int) string {
	return filepath.Join(db.opt.WorkDir, fmt.Sprintf("control-wal-%02d", shard))
}

func controlWALShard(groupID uint64) int {
	const mix = 11400714819323198485
	return int((groupID * mix) & (defaultControlWALShards - 1))
}

func controlRetentionMarkForShard(ptrs map[uint64]stats.ControlLogPointer, shard int) wal.RetentionMark {
	filtered := make(map[uint64]stats.ControlLogPointer)
	for groupID, ptr := range ptrs {
		if controlWALShard(groupID) == shard {
			filtered[groupID] = ptr
		}
	}
	return controlRetentionMark(filtered)
}

// WorkdirMode returns the persisted lifecycle mode observed when the DB was opened.
func (db *DB) WorkdirMode() workdirmode.Mode {
	if db == nil {
		return ""
	}
	return db.workdirMode
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
	db.background.Start(backgroundConfig{
		StartCompacter:     db.lsm.StartCompacter,
		EnableWALWatchdog:  db.opt.EnableWALWatchdog,
		WALWatchdogConfigs: watchdogConfigs,
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
	for shard := range defaultControlWALShards {
		if err := wal.VerifyDir(db.controlWALDir(shard), db.fs); err != nil {
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

	if err := db.closeControlWALs(); err != nil {
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

func (db *DB) closeControlWALs() error {
	db.controlWALMu.Lock()
	defer db.controlWALMu.Unlock()
	var errs []error
	for shard, wd := range db.controlWatchdogs {
		if wd != nil {
			wd.Stop()
			db.controlWatchdogs[shard] = nil
		}
	}
	for shard, mgr := range db.controlWALs {
		if mgr == nil {
			continue
		}
		if err := mgr.Close(); err != nil {
			errs = append(errs, fmt.Errorf("control wal shard %d close: %w", shard, err))
		}
		db.controlWALs[shard] = nil
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
		panic("NoKV: non-transactional version overflow")
	}
	return next
}

// ApplyInternalEntries writes pre-built internal-key entries through the
// regular write pipeline.
//
// The caller must provide entries with internal keys. The entry slices must not
// be mutated until this call returns.
//
// Multi-key internal batches are regrouped by the same key-affinity router used
// by the commit pipeline before they reach the sharded LSM. That keeps every
// write/delete for one user key on one shard, which is required for
// same-version MVCC tombstones such as Percolator lock removal.
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
	return db.batchSetInternalEntries(entries)
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
// canonical internal-key layout and inline value payloads.
func (db *DB) MaterializeInternalEntry(src *kv.Entry) (*kv.Entry, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if src == nil {
		return nil, utils.ErrKeyNotFound
	}
	buf := make([]byte, len(src.Key)+len(src.Value))
	keyCopy := buf[:len(src.Key)]
	copy(keyCopy, src.Key)
	valueCopy := buf[len(src.Key):]
	copy(valueCopy, src.Value)
	return &kv.Entry{
		Key:          keyCopy,
		Value:        valueCopy,
		ExpiresAt:    src.ExpiresAt,
		CF:           src.CF,
		Meta:         src.Meta,
		Version:      src.Version,
		Offset:       src.Offset,
		Hlen:         src.Hlen,
		ValThreshold: src.ValThreshold,
	}, nil
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
	buf := make([]byte, len(userKey)+len(entry.Value))
	keyCopy := buf[:len(userKey)]
	copy(keyCopy, userKey)
	valueCopy := buf[len(userKey):]
	copy(valueCopy, entry.Value)
	return &kv.Entry{
		Key:          keyCopy,
		Value:        valueCopy,
		ExpiresAt:    entry.ExpiresAt,
		CF:           cf,
		Meta:         entry.Meta,
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
//   - Returns ErrInvalidRequest when the loaded key is not in internal-key form.
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

	if !entry.PopulateInternalMeta() {
		entry.DecrRef()
		return nil, utils.ErrInvalidRequest
	}
	return entry, nil
}

// NewIterator creates a DB-level iterator over user keys in the default
// column family. The state machine + Item materialization live in
// local/internal/iterator; this method wires DB internals (lsm, iterPool)
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
// over DB struct fields so the snapshot logic can live in local/stats without
// importing the local DB implementation.

func (db *DB) LSM() stats.LSMSource                 { return db.lsm }
func (db *DB) LSMWALs() []*wal.Manager              { return db.lsmWALs }
func (db *DB) BackgroundWatchdogs() []*wal.Watchdog { return db.background.WALWatchdogs() }
func (db *DB) SetMVCCGCStatsSnapshotSource(source func() stats.MVCCGCStatsSnapshot) {
	if db == nil || source == nil {
		return
	}
	db.mvccGCStats.Store(mvccGCStatsSnapshotSource(source))
}

func (db *DB) MVCCGCStatsSnapshot() stats.MVCCGCStatsSnapshot {
	if db == nil {
		return stats.MVCCGCStatsSnapshot{}
	}
	v := db.mvccGCStats.Load()
	if v == nil {
		return stats.MVCCGCStatsSnapshot{}
	}
	source, ok := v.(mvccGCStatsSnapshotSource)
	if !ok || source == nil {
		return stats.MVCCGCStatsSnapshot{}
	}
	return source()
}

func (db *DB) HotWrite() *thermos.RotatingThermos  { return db.hotWrite }
func (db *DB) IteratorReused() uint64              { return db.iterPool.Reused() }
func (db *DB) WriteMetrics() *metrics.WriteMetrics { return db.writeMetrics }
func (db *DB) BlockWritesActive() bool             { return db.blockWrites.Load() == 1 }
func (db *DB) SlowWritesActive() bool              { return db.slowWrites.Load() == 1 }
func (db *DB) HotWriteLimited() uint64             { return db.hotWriteLimited.Load() }
func (db *DB) ControlLogLagWarnSegments() int64    { return db.opt.ControlLogLagWarnSegments }
func (db *DB) WALTypedRecordWarnRatio() float64    { return db.opt.WALTypedRecordWarnRatio }
func (db *DB) WALTypedRecordWarnSegments() int64   { return db.opt.WALTypedRecordWarnSegments }
func (db *DB) ThermosTopK() int                    { return db.opt.ThermosTopK }

// WriteBatchLimits returns the per-batch entry-count and byte-size caps the
// commit pipeline enforces. Callers that build batched-install paths use
// these to chunk their writes so the pipeline's Send() never rejects a
// group as too large.
func (db *DB) WriteBatchLimits() (count int64, size int64) {
	if db == nil || db.opt == nil {
		return 0, 0
	}
	return db.opt.MaxBatchCount, db.opt.MaxBatchSize
}

func (db *DB) ControlLogPointerSnapshot() func() map[uint64]stats.ControlLogPointer {
	if db == nil || db.opt == nil {
		return nil
	}
	return db.opt.ControlLogPointerSnapshot
}

func (db *DB) SetTransportMetricsSource(source func() metrics.GRPCTransportMetrics) {
	if db == nil || source == nil {
		return
	}
	db.transportStats.Store(transportMetricsSource(source))
}

func (db *DB) TransportMetrics() metrics.GRPCTransportMetrics {
	if db == nil {
		return metrics.GRPCTransportMetrics{}
	}
	v := db.transportStats.Load()
	if v == nil {
		return metrics.GRPCTransportMetrics{}
	}
	source, ok := v.(transportMetricsSource)
	if !ok || source == nil {
		return metrics.GRPCTransportMetrics{}
	}
	return source()
}

func (db *DB) ControlWALsLocked(fn func(wals []*wal.Manager)) {
	db.controlWALMu.Lock()
	defer db.controlWALMu.Unlock()
	fn(db.controlWALs[:])
}

// commit.Host implementation: read-only accessors the commit Pipeline uses
// without importing the local DB implementation.

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
	state = commit.NormalizeWriteThrottleState(state)
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
// spreading commit-pipeline details across the DB facade.
func (db *DB) sendToWriteCh(entries []*kv.Entry, waitOnThrottle bool) (*commit.Request, error) {
	return db.pipeline.Send(entries, waitOnThrottle)
}

func (db *DB) maybeThrottleWrite(cf kv.ColumnFamily, key []byte) error {
	if db == nil || db.opt == nil {
		return nil
	}
	if !commit.ShouldThrottleHotWrite(db.hotWrite, db.opt.WriteHotKeyLimit, cf, key) {
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

func (db *DB) batchSetInternalEntries(entries []*kv.Entry) error {
	groups := db.groupInternalEntriesByShard(entries)
	for i, group := range groups {
		if len(group) == 0 {
			continue
		}
		if err := db.batchSet(group); err != nil {
			// Entries for later shard groups were ref-counted by
			// ApplyInternalEntries but have not been handed to the commit
			// pipeline yet. The failing group is cleaned up by batchSet on
			// enqueue failure, or owned by the pipeline if it reached apply.
			releaseEntryGroups(groups[i+1:])
			return err
		}
	}
	return nil
}

// CanApplyInternalEntriesAtomically reports whether ApplyInternalEntries will
// persist entries as one local LSM apply group. The check intentionally uses
// the same shard grouping path as ApplyInternalEntries; callers must not
// duplicate this routing logic outside the DB boundary.
func (db *DB) CanApplyInternalEntriesAtomically(entries []*kv.Entry) bool {
	groups := db.groupInternalEntriesByShard(entries)
	nonEmpty := 0
	for _, group := range groups {
		if len(group) == 0 {
			continue
		}
		nonEmpty++
		if nonEmpty > 1 {
			return false
		}
	}
	return true
}

func (db *DB) groupInternalEntriesByShard(entries []*kv.Entry) [][]*kv.Entry {
	if len(entries) <= 1 {
		return [][]*kv.Entry{entries}
	}
	shardCount := 1
	if db != nil && db.opt != nil && db.opt.LSMShardCount > 1 {
		shardCount = db.opt.LSMShardCount
	}
	if shardCount <= 1 {
		return [][]*kv.Entry{entries}
	}
	buckets := make([][]*kv.Entry, shardCount)
	order := make([]int, 0, shardCount)
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		shardID := db.shardForInternalKey(entry.Key, shardCount)
		if len(buckets[shardID]) == 0 {
			order = append(order, shardID)
		}
		buckets[shardID] = append(buckets[shardID], entry)
	}
	groups := make([][]*kv.Entry, 0, len(order))
	for _, shardID := range order {
		groups = append(groups, buckets[shardID])
	}
	return groups
}

func (db *DB) shardForInternalKey(internalKey []byte, shardCount int) int {
	_, userKey, _, ok := kv.SplitInternalKey(internalKey)
	if !ok || len(userKey) == 0 {
		return 0
	}
	return utils.ShardForUserKey(db.userKeyShardKeyOrUserKey(userKey), shardCount)
}

func (db *DB) userKeyShardKey(userKey []byte) []byte {
	if db == nil || db.opt == nil || db.opt.UserKeyShapeExtractor == nil {
		return nil
	}
	return db.opt.UserKeyShapeExtractor(userKey).shardKey()
}

func (db *DB) userKeyShardKeyOrUserKey(userKey []byte) []byte {
	if key := db.userKeyShardKey(userKey); len(key) > 0 {
		return key
	}
	return userKey
}

func releaseEntryGroups(groups [][]*kv.Entry) {
	for _, group := range groups {
		for _, entry := range group {
			if entry != nil {
				entry.DecrRef()
			}
		}
	}
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

func (db *DB) WorkDir() string {
	if db == nil || db.opt == nil {
		return ""
	}
	return db.opt.WorkDir
}
