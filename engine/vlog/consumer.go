package vlog

// Consumer is the high-level value-log substrate consumer that sits on top
// of the per-bucket Manager fanout. It owns the multi-bucket layout, the
// discard-stats flush goroutine, the GC scheduler, and the rewind-on-write
// failure semantics. The mmap segment substrate (open/read/write/seal/
// rotate/remove/close) lives in engine/slab; the per-bucket logical cursor
// + AppendEntries lives in engine/vlog/manager.go.
//
// Consumer talks back into its host (DB + LSM) only through the small
// LSM interface and the function-pointer Deps bag — engine/vlog never
// imports the root NoKV package, and DB-side concerns (head bookkeeping,
// gc loop driver, periodic ticker) stay in the root package.

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/metrics"
	dbruntime "github.com/feichai0017/NoKV/runtime"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

// HeadLogInterval is the persistence interval for value-log heads (1 MiB).
const HeadLogInterval = uint32(1 << 20)

// SmallCopyThreshold is the inline-copy ceiling for Read; below this we
// copy the value into the caller's slice instead of returning a borrowed
// mmap view + release callback.
const SmallCopyThreshold = 4 << 10

// DefaultDiscardStatsFlushThreshold is the per-flush row-count trigger
// when Config.DiscardStatsFlushThreshold is unset.
const DefaultDiscardStatsFlushThreshold = 100

// DiscardStatsKey is the special internal key under which the encoded
// per-segment discard counters are persisted into the LSM, so that a
// fresh Consumer can warm them at Open time via PopulateDiscardStats.
var DiscardStatsKey = []byte("!NoKV!discard")

// ConsumerConfig carries the vlog tuning knobs the host snapshots at
// Open time. The single-bucket counterpart for Manager.Open is named
// Config (see manager.go).
type ConsumerConfig struct {
	Dir                        string
	BucketCount                int
	GCParallelism              int
	NumCompactors              int
	FileSize                   int
	Verbose                    bool
	SyncWrites                 bool
	DiscardStatsFlushThreshold int
	GCReduceScore              float64
	GCSkipScore                float64
	GCReduceBacklog            int
	GCSkipBacklog              int
	GCSampleSizeRatio          float64
	GCSampleCountRatio         float64
	GCSampleFromHead           bool
	GCMaxEntries               uint32
	MaxBatchCount              int64
	MaxBatchSize               int64
	DiscardStatsCh             chan map[manifest.ValueLogID]int64
	FS                         vfs.FS
}

// LSM is the narrow LSM surface vlog needs. The engine/lsm package
// satisfies it directly — this interface exists only so engine/vlog does
// not depend on a particular LSM implementation in tests.
type LSM interface {
	Get(internalKey []byte) (*kv.Entry, error)
	Diagnostics() lsm.Diagnostics
	ValueLogStatusSnapshot() map[manifest.ValueLogID]manifest.ValueLogMeta
	ValueLogHeadSnapshot() map[uint32]kv.ValuePtr
	LogValueLogDelete(bucket, fid uint32) error
	LogValueLogUpdate(meta *manifest.ValueLogMeta) error
}

// Waiter is the request-handle the host returns from SendToWriteCh; vlog
// only uses it to block until the write is durable.
type Waiter interface {
	Wait() error
}

// Deps wires the vlog Consumer back to its host. PeekShouldWriteValueToLSM
// is split out from the policy matcher so the commit pipeline's pre-scan
// can record the per-entry decision exactly once.
type Deps struct {
	LSM                       LSM
	GetInternalEntry          func(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error)
	BatchSet                  func(entries []*kv.Entry) error
	SendToWriteCh             func(entries []*kv.Entry, waitOnThrottle bool) (Waiter, error)
	PeekShouldWriteValueToLSM func(*kv.Entry) bool
}

// Consumer owns the multi-bucket Manager fanout, the discard-stats flush
// goroutine, and the GC scheduler. Construction goes through OpenConsumer.
type Consumer struct {
	cfg  ConsumerConfig
	deps Deps

	bucketCount uint32
	managers    []*Manager

	filesToDeleteLock  sync.Mutex
	filesToBeDeleted   []manifest.ValueLogID
	numActiveIterators atomic.Int32

	gcTokens      chan struct{}
	gcParallelism int
	gcBucketBusy  []atomic.Uint32
	gcPickSeed    atomic.Uint64
	garbageCh     chan struct{}

	lfDiscardStats *discardStats
	closer         *utils.Closer
}

// OpenConsumer creates one Manager per bucket, replays them against
// replayHeads (calling replayFn for each entry), reconciles against the
// manifest snapshot, kicks off the discard-stats flush goroutine, and
// returns the heads observed at the end of replay so the caller can
// install them in its own bookkeeping.
func OpenConsumer(cfg ConsumerConfig, deps Deps, replayHeads map[uint32]kv.ValuePtr, replayFn kv.LogEntry) (*Consumer, map[uint32]kv.ValuePtr, error) {
	if cfg.BucketCount <= 0 {
		cfg.BucketCount = 1
	}
	if cfg.FS == nil {
		cfg.FS = vfs.Ensure(nil)
	}
	if cfg.GCParallelism <= 0 {
		cfg.GCParallelism = max(cfg.NumCompactors/2, 1)
	}
	if cfg.GCParallelism > cfg.BucketCount {
		cfg.GCParallelism = cfg.BucketCount
	}

	managers := make([]*Manager, cfg.BucketCount)
	for bucket := range cfg.BucketCount {
		bucketDir := filepath.Join(cfg.Dir, fmt.Sprintf("bucket-%03d", bucket))
		mgr, err := Open(Config{
			Dir:      bucketDir,
			FileMode: utils.DefaultFileMode,
			MaxSize:  int64(cfg.FileSize),
			Bucket:   uint32(bucket),
			FS:       cfg.FS,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("vlog: open bucket %d: %w", bucket, err)
		}
		managers[bucket] = mgr
	}

	c := &Consumer{
		cfg:              cfg,
		deps:             deps,
		bucketCount:      uint32(cfg.BucketCount),
		managers:         managers,
		filesToBeDeleted: make([]manifest.ValueLogID, 0),
		lfDiscardStats:   newDiscardStats(cfg.DiscardStatsCh, cfg.DiscardStatsFlushThreshold),
		gcTokens:         make(chan struct{}, cfg.GCParallelism),
		gcParallelism:    cfg.GCParallelism,
		gcBucketBusy:     make([]atomic.Uint32, cfg.BucketCount),
		garbageCh:        make(chan struct{}, 1),
	}
	c.closer = c.lfDiscardStats.closer

	metrics.DefaultValueLogGCCollector().SetParallelism(cfg.GCParallelism)
	c.SetFileSize(cfg.FileSize)

	if deps.LSM != nil {
		c.ReconcileManifest(deps.LSM.ValueLogStatusSnapshot())
	}

	heads, err := c.openReplay(replayHeads, replayFn)
	if err != nil {
		_ = c.Close()
		return nil, nil, err
	}
	return c, heads, nil
}

// openReplay runs the per-bucket replay loop: walks every existing FID,
// invokes the host replayFn, calls SegmentInit on sealed FIDs, and
// returns the heads observed at the end.
func (c *Consumer) openReplay(replayHeads map[uint32]kv.ValuePtr, replayFn kv.LogEntry) (map[uint32]kv.ValuePtr, error) {
	if replayFn == nil {
		replayFn = func(*kv.Entry, *kv.ValuePtr) error { return nil }
	}
	c.lfDiscardStats.closer.Add(1)
	go c.flushDiscardStats()

	if len(c.managers) == 0 {
		return nil, errors.New("valueLog.open: no value log buckets found")
	}
	c.filesToDeleteLock.Lock()
	c.filesToBeDeleted = nil
	c.filesToDeleteLock.Unlock()

	heads := make(map[uint32]kv.ValuePtr, len(c.managers))
	for bucket, mgr := range c.managers {
		if mgr == nil {
			continue
		}
		fids := mgr.ListFIDs()
		if len(fids) == 0 {
			return nil, fmt.Errorf("valueLog.open: no value log files found for bucket %d", bucket)
		}
		head := replayHeads[uint32(bucket)]
		activeFID := mgr.ActiveFID()
		for _, fid := range fids {
			offset := uint32(0)
			if head.Bucket == uint32(bucket) && fid == head.Fid {
				offset = head.Offset + head.Len
			}
			c.Logf("Scanning file id: %d bucket: %d at offset: %d", fid, bucket, offset)
			start := time.Now()
			if err := c.replayLog(uint32(bucket), fid, offset, replayFn, fid == activeFID); err != nil {
				if err == utils.ErrDeleteVlogFile {
					if removeErr := c.removeValueLogFile(uint32(bucket), fid); removeErr != nil {
						return nil, removeErr
					}
					continue
				}
				return nil, err
			}
			c.Logf("Scan took: %s", time.Since(start))

			if fid != activeFID {
				if err := mgr.SegmentInit(fid); err != nil {
					return nil, err
				}
			}
		}
		headPtr := mgr.Head()
		if existing, ok := replayHeads[uint32(bucket)]; ok && !existing.IsZero() {
			heads[uint32(bucket)] = existing
		} else {
			heads[uint32(bucket)] = headPtr
		}
	}
	if err := c.PopulateDiscardStats(); err != nil {
		if err != utils.ErrKeyNotFound {
			slog.Default().Warn("populate discard stats", "error", err)
		}
	}
	return heads, nil
}

// Close stops the background flush goroutine and closes all managers.
func (c *Consumer) Close() error {
	if c == nil {
		return nil
	}
	if c.closer != nil {
		c.closer.Close()
		<-c.closer.Closed()
	}
	var firstErr error
	for _, mgr := range c.managers {
		if mgr == nil {
			continue
		}
		if err := mgr.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// BackgroundCloser exposes the lifecycle Closer so the host can wire
// auxiliary background goroutines (e.g. the periodic GC ticker) onto the
// same shutdown channel as the discard-stats flush worker.
func (c *Consumer) BackgroundCloser() *utils.Closer {
	if c == nil {
		return nil
	}
	return c.closer
}

// SetFileSize hot-resizes the per-bucket Manager max-size cap.
func (c *Consumer) SetFileSize(sz int) {
	if c == nil || sz <= 0 {
		return
	}
	c.cfg.FileSize = sz
	for _, mgr := range c.managers {
		if mgr == nil {
			continue
		}
		mgr.SetMaxSize(int64(sz))
	}
}

// Logf emits a value-log diagnostic line when Verbose was set.
func (c *Consumer) Logf(format string, args ...any) {
	if c == nil || !c.cfg.Verbose {
		return
	}
	slog.Default().Info(fmt.Sprintf(format, args...))
}

// Metrics snapshots backlog counters: live segments, per-bucket heads,
// discard-stats flush queue depth, pending-deletion list size.
func (c *Consumer) Metrics() metrics.ValueLogMetrics {
	if c == nil || len(c.managers) == 0 {
		return metrics.ValueLogMetrics{}
	}
	heads := make(map[uint32]kv.ValuePtr, len(c.managers))
	segments := 0
	for bucket, mgr := range c.managers {
		if mgr == nil {
			continue
		}
		segments += len(mgr.ListFIDs())
		heads[uint32(bucket)] = mgr.Head()
	}
	stats := metrics.ValueLogMetrics{
		Segments: segments,
		Heads:    heads,
	}

	if c.lfDiscardStats != nil {
		stats.DiscardQueue = len(c.lfDiscardStats.flushChan)
	}

	c.filesToDeleteLock.Lock()
	stats.PendingDeletes = len(c.filesToBeDeleted)
	c.filesToDeleteLock.Unlock()

	return stats
}

// ReconcileManifest aligns each bucket's live FID set with the manifest
// snapshot: invalid entries are removed, orphan FIDs above the manifest
// high-water are pruned. Called once at Open.
func (c *Consumer) ReconcileManifest(status map[manifest.ValueLogID]manifest.ValueLogMeta) {
	if c == nil || len(c.managers) == 0 || len(status) == 0 {
		return
	}
	for bucket, mgr := range c.managers {
		if mgr == nil {
			continue
		}
		existing := make(map[uint32]struct{})
		for _, fid := range mgr.ListFIDs() {
			existing[fid] = struct{}{}
		}
		var (
			maxValid uint32
			hasValid bool
		)
		for id, meta := range status {
			if id.Bucket != uint32(bucket) {
				continue
			}
			fid := id.FileID
			if !meta.Valid {
				if _, ok := existing[fid]; ok {
					if err := mgr.Remove(fid); err != nil {
						slog.Default().Warn("value log reconcile remove", "fid", fid, "bucket", bucket, "error", err)
						continue
					}
					delete(existing, fid)
					metrics.DefaultValueLogGCCollector().IncSegmentsRemoved()
				}
				continue
			}
			hasValid = true
			if fid > maxValid {
				maxValid = fid
			}
			if _, ok := existing[fid]; ok {
				delete(existing, fid)
				continue
			}
			slog.Default().Warn("value log reconcile missing file", "fid", fid, "bucket", bucket)
		}
		if !hasValid {
			continue
		}
		threshold := maxValid
		for fid := range existing {
			if fid <= threshold {
				continue
			}
			if err := mgr.Remove(fid); err != nil {
				slog.Default().Warn("value log reconcile remove orphan", "fid", fid, "bucket", bucket, "error", err)
				continue
			}
			metrics.DefaultValueLogGCCollector().IncSegmentsRemoved()
			slog.Default().Warn("value log reconcile removed untracked segment", "fid", fid, "bucket", bucket)
		}
	}
}

// ManagerFor returns the per-bucket Manager. Returns an error if the
// bucket id is out of range or the slot is nil (which would indicate a
// construction-time bug).
func (c *Consumer) ManagerFor(bucket uint32) (*Manager, error) {
	if c == nil || int(bucket) >= len(c.managers) {
		return nil, fmt.Errorf("value log: invalid bucket %d", bucket)
	}
	mgr := c.managers[bucket]
	if mgr == nil {
		return nil, fmt.Errorf("value log: missing manager for bucket %d", bucket)
	}
	return mgr, nil
}

// Managers exposes the per-bucket Manager slice for callers that need to
// iterate every bucket (e.g. metric snapshots, head reconciliation).
// Callers must not mutate the returned slice.
func (c *Consumer) Managers() []*Manager {
	if c == nil {
		return nil
	}
	return c.managers
}

// BucketCount returns the configured number of vlog buckets.
func (c *Consumer) BucketCount() uint32 {
	if c == nil {
		return 0
	}
	return c.bucketCount
}

func (c *Consumer) bucketForEntry(e *kv.Entry) uint32 {
	if c == nil || e == nil {
		return 0
	}
	buckets := c.bucketCount
	if buckets <= 1 {
		return 0
	}
	return kv.ValueLogBucket(e.Key, buckets)
}

// removeValueLogFile journals the deletion through the manifest, then
// removes the segment from the per-bucket Manager. On Manager.Remove
// failure we attempt to re-log the previous metadata so the manifest
// stays in sync with the on-disk state.
func (c *Consumer) removeValueLogFile(bucket uint32, fid uint32) error {
	if c == nil || c.deps.LSM == nil {
		return fmt.Errorf("valueLog.removeValueLogFile: missing LSM dep")
	}
	mgr, err := c.ManagerFor(bucket)
	if err != nil {
		return err
	}
	status := c.deps.LSM.ValueLogStatusSnapshot()
	var (
		meta    manifest.ValueLogMeta
		hasMeta bool
	)
	if status != nil {
		meta, hasMeta = status[manifest.ValueLogID{Bucket: bucket, FileID: fid}]
	}
	if err := c.deps.LSM.LogValueLogDelete(bucket, fid); err != nil {
		return errors.Wrapf(err, "log value log delete fid %d (bucket %d)", fid, bucket)
	}
	if err := mgr.Remove(fid); err != nil {
		if hasMeta {
			if errRestore := c.deps.LSM.LogValueLogUpdate(&meta); errRestore != nil {
				slog.Default().Error("value log delete rollback", "fid", fid, "bucket", bucket, "error", errRestore)
			}
		}
		return errors.Wrapf(err, "remove value log fid %d (bucket %d)", fid, bucket)
	}
	metrics.DefaultValueLogGCCollector().IncSegmentsRemoved()
	return nil
}

// NewValuePtr writes a single entry through the value log and returns
// its allocated pointer. Used by callers that need to materialize a
// pointer outside the normal write batch path.
func (c *Consumer) NewValuePtr(e *kv.Entry) (*kv.ValuePtr, error) {
	req := dbruntime.RequestPool.Get().(*dbruntime.Request)
	req.Reset()
	req.Entries = []*kv.Entry{e}
	req.IncrRef()
	defer func() {
		req.Entries = nil
		req.DecrRef()
	}()

	if err := c.Write([]*dbruntime.Request{req}); err != nil {
		return nil, err
	}
	if len(req.Ptrs) == 0 {
		return nil, errors.New("valueLog.newValuePtr: missing value pointer")
	}
	vp := req.Ptrs[0]
	return &vp, nil
}

// Read reads a value at vp; returns either an inline-copied small slice
// (cb==nil) or a borrowed mmap view + release callback. Threshold for
// the inline path is SmallCopyThreshold.
func (c *Consumer) Read(vp *kv.ValuePtr) ([]byte, func(), error) {
	if vp == nil {
		return nil, nil, errors.New("valueLog.read: nil value pointer")
	}
	mgr, err := c.ManagerFor(vp.Bucket)
	if err != nil {
		return nil, nil, err
	}
	return mgr.ReadValue(vp, ReadOptions{
		Mode:                ReadModeAuto,
		SmallValueThreshold: SmallCopyThreshold,
	})
}

// Write fans the entries in reqs out across per-bucket Managers, populates
// req.Ptrs/PtrIdxs/PtrBuckets, and rewinds every touched bucket on the
// first failure so the manifest never sees half-written segments.
func (c *Consumer) Write(reqs []*dbruntime.Request) error {
	heads := make(map[uint32]kv.ValuePtr)
	touched := make(map[uint32]struct{})
	fail := func(err error, context string) error {
		for _, req := range reqs {
			req.Ptrs = req.Ptrs[:0]
			req.PtrIdxs = req.PtrIdxs[:0]
			req.PtrBuckets = req.PtrBuckets[:0]
		}
		for bucket := range touched {
			mgr, mgrErr := c.ManagerFor(bucket)
			if mgrErr != nil {
				continue
			}
			if head, ok := heads[bucket]; ok {
				if rewindErr := mgr.Rewind(head); rewindErr != nil {
					slog.Default().Error("value log rewind after failure", "context", context, "bucket", bucket, "error", rewindErr)
				}
			}
		}
		return err
	}
	wrote := false
	for _, req := range reqs {
		if req == nil {
			continue
		}
		req.Ptrs = req.Ptrs[:0]
		req.PtrIdxs = req.PtrIdxs[:0]
		req.PtrBuckets = req.PtrBuckets[:0]
		bucketEntries := make(map[uint32][]int)
		for i, e := range req.Entries {
			if !c.deps.PeekShouldWriteValueToLSM(e) {
				wrote = true
				bucket := c.bucketForEntry(e)
				bucketEntries[bucket] = append(bucketEntries[bucket], i)
			}
		}
		if len(bucketEntries) == 0 {
			continue
		}
		if cap(req.Ptrs) < len(req.Entries) {
			req.Ptrs = make([]kv.ValuePtr, len(req.Entries))
		} else {
			req.Ptrs = req.Ptrs[:len(req.Entries)]
			for i := range req.Ptrs {
				req.Ptrs[i] = kv.ValuePtr{}
			}
		}
		for bucket, idxs := range bucketEntries {
			mgr, err := c.ManagerFor(bucket)
			if err != nil {
				return fail(err, "value log bucket manager")
			}
			if _, ok := heads[bucket]; !ok {
				heads[bucket] = mgr.Head()
			}
			entries := make([]*kv.Entry, len(idxs))
			for i, idx := range idxs {
				entries[i] = req.Entries[idx]
			}
			ptrs, err := mgr.AppendEntries(entries, nil)
			if err != nil {
				return fail(err, "rewind value log after append failure")
			}
			for i, idx := range idxs {
				req.Ptrs[idx] = ptrs[i]
			}
			req.PtrIdxs = append(req.PtrIdxs, idxs...)
			req.PtrBuckets = append(req.PtrBuckets, bucket)
			touched[bucket] = struct{}{}
		}
	}
	if wrote && c.cfg.SyncWrites {
		byBucket := make(map[uint32]map[uint32]struct{})
		for _, req := range reqs {
			if len(req.PtrBuckets) == 0 {
				continue
			}
			for _, ptr := range req.Ptrs {
				if ptr.IsZero() {
					continue
				}
				if _, ok := byBucket[ptr.Bucket]; !ok {
					byBucket[ptr.Bucket] = make(map[uint32]struct{})
				}
				byBucket[ptr.Bucket][ptr.Fid] = struct{}{}
			}
		}
		for bucket, fids := range byBucket {
			mgr, err := c.ManagerFor(bucket)
			if err != nil {
				return fail(err, "sync value log after append")
			}
			list := make([]uint32, 0, len(fids))
			for fid := range fids {
				list = append(list, fid)
			}
			if err := mgr.SyncFIDs(list); err != nil {
				return fail(err, "sync value log after append")
			}
		}
	}
	return nil
}

// replayLog scans the segment payload from offset, invokes replayFn for
// each entry, and decides truncation/bootstrap based on where scanning
// stopped relative to the on-disk size and the active-vs-sealed status.
func (c *Consumer) replayLog(bucket uint32, fid uint32, offset uint32, replayFn kv.LogEntry, isActive bool) error {
	mgr, err := c.ManagerFor(bucket)
	if err != nil {
		return err
	}
	endOffset, err := mgr.Iterate(fid, offset, replayFn)
	if err != nil {
		return errors.Wrapf(err, "Unable to replay logfile: fid=%d bucket=%d", fid, bucket)
	}
	size, err := mgr.SegmentSize(fid)
	if err != nil {
		return err
	}
	if int64(endOffset) == size {
		return nil
	}

	if endOffset <= uint32(kv.ValueLogHeaderSize) {
		if !isActive {
			return utils.ErrDeleteVlogFile
		}
		return mgr.SegmentBootstrap(fid)
	}

	c.Logf("Truncating vlog file %05d (bucket %d) to offset: %d", fid, bucket, endOffset)
	if err := mgr.SegmentTruncate(fid, endOffset); err != nil {
		return fmt.Errorf("truncation needed at offset %d. Can be done manually as well: %w", endOffset, err)
	}
	return nil
}
