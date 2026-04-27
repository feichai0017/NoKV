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
		// commitDispatch holds one channel per LSM data-plane shard. The
		// dispatcher routes each batch to commitDispatch[shardID]; the
		// pinned processor drains it. cap=32 lets the dispatcher run
		// ahead of slow processors so the burst coalesce loop in
		// runCommitBurst sees multiple batches and merges them into one
		// WAL append. We tried a custom SPSC ring (utils.SPSCQueue) here;
		// benchmarks showed cap=32 buffered chan was 30-40% faster on
		// YCSB-A — the chan buffer already amortizes scheduler hops and
		// the atomic traffic on a user-space ring outweighs the savings.
		commitDispatch  []chan *dbruntime.CommitBatch
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
	db.iterPool = dbruntime.NewIteratorPool()
	if db.opt.EnableValueLog {
		db.initVLog()
	}
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
	// One commit processor per LSM data-plane shard. Each processor owns
	// its shard's WAL Manager — no cross-shard sharing means no Manager.mu
	// contention on the hot write path. Dispatcher fans batches out
	// round-robin so each batch lives on exactly one shard
	// (commit-worker affinity preserves SetBatch atomicity).
	workers := db.opt.LSMShardCount
	if workers <= 0 {
		workers = 1
	}
	db.commitDispatch = make([]chan *dbruntime.CommitBatch, workers)
	for i := 0; i < workers; i++ {
		db.commitDispatch[i] = make(chan *dbruntime.CommitBatch, 32)
	}

	db.commitWG.Add(1)
	go db.commitDispatcher()
	for i := 0; i < workers; i++ {
		db.commitWG.Add(1)
		go db.commitProcessor(i)
	}
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
		ThrottleCallback:         db.applyThrottle,
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
	if db.vlog == nil {
		entry.DecrRef()
		return nil, fmt.Errorf("value pointer encountered but EnableValueLog is false; LSM still references vlog data, re-enable EnableValueLog to read it")
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

// requestsHaveVlogWork reports whether any entry in any request needs to be
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
func (db *DB) requestsHaveVlogWork(reqs []*dbruntime.Request) bool {
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
// already recorded via requestsHaveVlogWork.
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

// commitDispatcher owns the MPSC queue's single consumer slot. It pulls
// batches off the queue and routes each to one shard's processor channel
// using round-robin. The dispatcher does no per-batch work itself, so it
// never bottlenecks on vlog/WAL/applyRequests latency.
//
// On queue close, the dispatcher closes every per-shard channel which is
// the signal for processors to drain remaining batches and return.
func (db *DB) commitDispatcher() {
	defer db.commitWG.Done()
	consumer := db.commitQueue.Consumer()
	closeAll := func() {
		for _, ch := range db.commitDispatch {
			close(ch)
		}
	}
	if consumer == nil {
		closeAll()
		return
	}
	defer consumer.Close()
	n := len(db.commitDispatch)
	var rrFallback int
	for {
		batch := db.nextCommitBatch(consumer)
		if batch == nil {
			closeAll()
			return
		}
		shardID := db.shardForBatch(batch, n, &rrFallback)
		batch.ShardID = shardID
		db.commitDispatch[shardID] <- batch
	}
}

// shardForBatch picks the destination shard for a commit batch using
// per-key affinity: hash the user key of the batch's first entry and mod
// by the shard count. This keeps every write to the same key on the
// same shard so percolator's same-startTS lock-on/lock-off and any other
// "later same-version write wins" pattern stays correct.
//
// The whole batch is still pinned to one shard end-to-end (preserves
// SetBatch atomicity at the LSM level). When a batch carries entries
// for multiple keys, the shard is chosen by the first key — peers in
// the same batch land there too. They will be re-routed to their own
// shard on later writes (when those keys arrive in their own batches),
// which is harmless because subsequent reads merge across all shards.
//
// Empty batches (no key to hash) round-robin via *rr to keep load even.
func (db *DB) shardForBatch(batch *dbruntime.CommitBatch, n int, rr *int) int {
	if n <= 1 {
		return 0
	}
	if batch != nil {
		for _, cr := range batch.Reqs {
			if cr == nil || cr.Req == nil {
				continue
			}
			for _, e := range cr.Req.Entries {
				if e == nil || len(e.Key) == 0 {
					continue
				}
				_, userKey, _, ok := kv.SplitInternalKey(e.Key)
				if !ok || len(userKey) == 0 {
					continue
				}
				return int(fnv1a32(userKey)) & (n - 1)
			}
		}
	}
	id := *rr
	*rr++
	if *rr >= n {
		*rr = 0
	}
	return id
}

// fnv1a32 is the inline FNV-1a 32-bit hash used by shard routing. It
// avoids the hash/fnv allocation per call.
func fnv1a32(b []byte) uint32 {
	var h uint32 = 2166136261
	for _, c := range b {
		h ^= uint32(c)
		h *= 16777619
	}
	return h
}

// commitProcessor runs the per-batch CPU pipeline (collect -> vlog write ->
// applyRequests -> ack) for batches pinned to one LSM shard. The processor
// owns its shard end-to-end: WAL append, memtable apply, and (when sync
// is inline) WAL fsync all hit one Manager, so there is no Manager.mu
// contention across processors.
//
// Burst coalescing: when the dispatcher delivers small batches faster
// than the processor can drain them, the processor pulls every batch
// already sitting in its channel and merges them into a single
// vlog.write + lsm.SetBatchGroup + (optional) Sync. That collapses N
// fsync/flush syscalls into one per burst — the bufio.Flush hotspot
// pprof identified at 47% under N=4. SetBatch atomicity is preserved
// because each batch's groups are still atomic (LSM processes them in
// order with per-group atomicity); failedAt from the merged apply is
// fanned back out to the originating batches.
func (db *DB) commitProcessor(shardID int) {
	defer db.commitWG.Done()
	walMgr := db.lsmWALs[shardID]
	burst := make([]*dbruntime.CommitBatch, 0, 8)
	for first := range db.commitDispatch[shardID] {
		burst = burst[:0]
		burst = append(burst, first)
		// Drain any extras already sitting in the channel.
	drain:
		for {
			select {
			case b, ok := <-db.commitDispatch[shardID]:
				if !ok {
					break drain
				}
				burst = append(burst, b)
			default:
				break drain
			}
		}
		db.runCommitBurst(walMgr, shardID, burst)
	}
}

// runCommitBurst processes a burst of batches as a single WAL append +
// memtable apply, then fans the result back to each batch for ack.
func (db *DB) runCommitBurst(walMgr *wal.Manager, shardID int, burst []*dbruntime.CommitBatch) {
	if len(burst) == 0 {
		return
	}
	// Fast path: when only one batch is ready, skip the merge bookkeeping
	// (boundaries / perBatchFailedAt allocations, request-list copy). At
	// high N the dispatcher rarely buffers more than one batch per shard
	// and the merge cost dominates the savings.
	if len(burst) == 1 {
		db.runSingleCommit(walMgr, shardID, burst[0])
		return
	}
	burstStart := time.Now()
	// Per-batch metadata + flattened request list. boundaries[i] is the
	// start index of batch i's requests in mergedRequests.
	boundaries := make([]int, len(burst)+1)
	var mergedRequests []*dbruntime.Request
	for i, batch := range burst {
		batch.BatchStart = burstStart
		requests, totalEntries, totalSize, waitSum := db.collectCommitRequests(batch.Reqs, burstStart)
		batch.Requests = requests
		if db.writeMetrics != nil {
			db.writeMetrics.RecordBatch(len(requests), totalEntries, totalSize, waitSum)
		}
		boundaries[i] = len(mergedRequests)
		mergedRequests = append(mergedRequests, requests...)
	}
	boundaries[len(burst)] = len(mergedRequests)

	if len(mergedRequests) == 0 {
		for _, batch := range burst {
			db.ackCommitBatch(batch.Reqs, batch.Pool, nil, -1, nil)
		}
		return
	}

	// Metadata-profile fast path: skip vlog.write entirely when no entry in
	// the burst needs offloading. Saves the heads/touched/bucketEntries map
	// allocations and the rewind closure preparation that vlog.write does
	// even when every entry stays inline. See docs/notes/2026-04-27-slab-substrate.md §4.
	var vlogDur time.Duration
	if db.requestsHaveVlogWork(mergedRequests) {
		if err := db.vlog.write(mergedRequests); err != nil {
			// Whole burst failed before reaching LSM. Each batch's whole
			// request set is unapplied — failedAt = -1 means "ack with
			// the default error for every request".
			for _, batch := range burst {
				db.ackCommitBatch(batch.Reqs, batch.Pool, batch.Requests, -1, err)
			}
			return
		}
		vlogDur = max(time.Since(burstStart), 0)
		if db.writeMetrics != nil && vlogDur > 0 {
			db.writeMetrics.RecordValueLog(vlogDur)
		}
	}
	for _, batch := range burst {
		batch.ValueLogDur = vlogDur
	}

	mergedFailedAt, applyErr := db.applyRequests(mergedRequests, shardID)
	// Fan mergedFailedAt back to per-batch failedAt:
	// - if mergedFailedAt == -1: every batch succeeded (-1).
	// - else find batch k containing it; batches < k succeeded (-1),
	//   batch k partially failed at (mergedFailedAt - boundaries[k]),
	//   batches > k were not attempted (failedAt = 0).
	perBatchFailedAt := make([]int, len(burst))
	if mergedFailedAt < 0 {
		for i := range perBatchFailedAt {
			perBatchFailedAt[i] = -1
		}
	} else {
		// boundaries length is len(burst)+1; binary or linear search.
		for i := range burst {
			switch {
			case mergedFailedAt >= boundaries[i+1]:
				perBatchFailedAt[i] = -1
			case mergedFailedAt < boundaries[i]:
				perBatchFailedAt[i] = 0
			default:
				perBatchFailedAt[i] = mergedFailedAt - boundaries[i]
			}
		}
	}

	if applyErr == nil && db.syncQueue != nil {
		// Hand each batch off to the sync worker individually so
		// per-batch ack ordering is preserved.
		for i, batch := range burst {
			sb := &dbruntime.SyncBatch{
				Reqs:      batch.Reqs,
				Pool:      batch.Pool,
				Requests:  batch.Requests,
				ShardID:   shardID,
				FailedAt:  perBatchFailedAt[i],
				ApplyDone: time.Now(),
			}
			batch.Reqs = nil
			batch.Pool = nil
			db.releaseCommitBatch(batch)
			db.syncQueue <- sb
		}
		return
	}

	syncErr := applyErr
	if applyErr == nil && db.opt.SyncWrites {
		syncStart := time.Now()
		syncErr = walMgr.Sync()
		if db.writeMetrics != nil {
			db.writeMetrics.RecordSync(time.Since(syncStart), len(burst))
		}
	}

	if db.writeMetrics != nil {
		totalDur := max(time.Since(burstStart), 0)
		applyDur := max(totalDur-vlogDur, 0)
		if applyDur > 0 {
			db.writeMetrics.RecordApply(applyDur)
		}
	}

	for i, batch := range burst {
		db.ackCommitBatch(batch.Reqs, batch.Pool, batch.Requests, perBatchFailedAt[i], syncErr)
	}
}

// runSingleCommit is the burst-size-1 fast path. It mirrors the Phase-2
// per-batch pipeline (no merge / boundaries / per-batch failedAt fan-out)
// to eliminate coalesce overhead when there is no extra batch to drain.
func (db *DB) runSingleCommit(walMgr *wal.Manager, shardID int, batch *dbruntime.CommitBatch) {
	batch.BatchStart = time.Now()
	requests, totalEntries, totalSize, waitSum := db.collectCommitRequests(batch.Reqs, batch.BatchStart)
	if len(requests) == 0 {
		db.ackCommitBatch(batch.Reqs, batch.Pool, nil, -1, nil)
		return
	}
	batch.Requests = requests
	if db.writeMetrics != nil {
		db.writeMetrics.RecordBatch(len(requests), totalEntries, totalSize, waitSum)
	}

	// Metadata-profile fast path — see runCommitBurst for rationale.
	if db.requestsHaveVlogWork(requests) {
		if err := db.vlog.write(requests); err != nil {
			db.ackCommitBatch(batch.Reqs, batch.Pool, requests, -1, err)
			return
		}
		if db.writeMetrics != nil {
			batch.ValueLogDur = max(time.Since(batch.BatchStart), 0)
			if batch.ValueLogDur > 0 {
				db.writeMetrics.RecordValueLog(batch.ValueLogDur)
			}
		}
	}

	failedAt, err := db.applyRequests(batch.Requests, shardID)
	if err == nil && db.syncQueue != nil {
		sb := &dbruntime.SyncBatch{
			Reqs:      batch.Reqs,
			Pool:      batch.Pool,
			Requests:  batch.Requests,
			ShardID:   shardID,
			FailedAt:  failedAt,
			ApplyDone: time.Now(),
		}
		batch.Reqs = nil
		batch.Pool = nil
		db.releaseCommitBatch(batch)
		db.syncQueue <- sb
		return
	}

	if err == nil && db.opt.SyncWrites {
		syncStart := time.Now()
		err = walMgr.Sync()
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

func (db *DB) syncWorker() {
	defer db.syncWG.Done()
	pending := make([]*dbruntime.SyncBatch, 0, 64)
	// per-shard buckets so one fsync covers many batches on the same WAL
	buckets := make(map[int][]*dbruntime.SyncBatch, len(db.lsmWALs))
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

		for _, sb := range pending {
			buckets[sb.ShardID] = append(buckets[sb.ShardID], sb)
		}
		syncStart := time.Now()
		errsByShard := make(map[int]error, len(buckets))
		for shardID, sbs := range buckets {
			if len(sbs) == 0 {
				continue
			}
			mgr := db.lsmWALs[shardID]
			if mgr == nil {
				errsByShard[shardID] = fmt.Errorf("db: lsm wal shard %d not initialized", shardID)
				continue
			}
			errsByShard[shardID] = mgr.Sync()
		}
		if db.writeMetrics != nil {
			db.writeMetrics.RecordSync(time.Since(syncStart), len(pending))
		}
		for _, sb := range pending {
			db.ackCommitBatch(sb.Reqs, sb.Pool, sb.Requests, sb.FailedAt, errsByShard[sb.ShardID])
		}
		pending = pending[:0]
		for k := range buckets {
			buckets[k] = buckets[k][:0]
		}
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

func (db *DB) applyRequests(reqs []*dbruntime.Request, shardID int) (int, error) {
	failedAt, err := db.writeRequestsToLSM(reqs, shardID)
	if err != nil {
		return failedAt, pkgerrors.Wrap(err, "writeRequests")
	}
	for _, r := range reqs {
		if r == nil || len(r.Entries) == 0 {
			continue
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

func (db *DB) writeRequestsToLSM(reqs []*dbruntime.Request, shardID int) (int, error) {
	groups := make([][]*kv.Entry, 0, len(reqs))
	groupToReq := make([]int, 0, len(reqs))
	for i, r := range reqs {
		if r == nil || len(r.Entries) == 0 {
			continue
		}
		if err := db.prepareLSMRequest(r); err != nil {
			// Nothing has reached the LSM yet, so the whole commit batch must fail.
			return 0, err
		}
		groups = append(groups, r.Entries)
		groupToReq = append(groupToReq, i)
	}
	if len(groups) == 0 {
		return -1, nil
	}
	failedGroup, err := db.lsm.SetBatchGroup(shardID, groups)
	if err != nil {
		if failedGroup >= 0 && failedGroup < len(groupToReq) {
			return groupToReq[failedGroup], err
		}
		return 0, err
	}
	return -1, nil
}

func (db *DB) prepareLSMRequest(b *dbruntime.Request) error {
	if len(b.PtrIdxs) == 0 {
		if len(b.Ptrs) != 0 && len(b.Ptrs) != len(b.Entries) {
			return pkgerrors.Errorf("Ptrs and Entries don't match: %+v", b)
		}
		return nil
	}
	if len(b.Ptrs) != len(b.Entries) {
		return pkgerrors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for _, idx := range b.PtrIdxs {
		entry := b.Entries[idx]
		entry.Meta = entry.Meta | kv.BitValuePointer
		entry.Value = b.Ptrs[idx].Encode()
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
