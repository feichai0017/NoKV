package NoKV

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/manifest"
	vlogpkg "github.com/feichai0017/NoKV/engine/vlog"
	dbruntime "github.com/feichai0017/NoKV/internal/runtime"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

const (
	valueLogHeadLogInterval           = uint32(1 << 20) // 1 MiB persistence interval for value-log head.
	valueLogSmallCopyThreshold        = 4 << 10         // copy small values to reduce read lock hold.
	defaultDiscardStatsFlushThreshold = 100
	valueLogDiscardStatsKeyString     = "!NoKV!discard"
)

var valueLogDiscardStatsKey = []byte(valueLogDiscardStatsKeyString)

type valueLogDiscardStats struct {
	sync.RWMutex
	m                 map[manifest.ValueLogID]int64
	flushChan         chan map[manifest.ValueLogID]int64
	closer            *utils.Closer
	updatesSinceFlush int
	flushThreshold    int
}

func newValueLogDiscardStats(flushChan chan map[manifest.ValueLogID]int64, flushThreshold int) *valueLogDiscardStats {
	if flushThreshold <= 0 {
		flushThreshold = defaultDiscardStatsFlushThreshold
	}
	return &valueLogDiscardStats{
		m:              make(map[manifest.ValueLogID]int64),
		flushChan:      flushChan,
		closer:         utils.NewCloser(),
		flushThreshold: flushThreshold,
	}
}

func encodeDiscardStats(stats map[manifest.ValueLogID]int64) ([]byte, error) {
	wire := make(map[string]int64, len(stats))
	for id, count := range stats {
		wire[fmt.Sprintf("%d:%d", id.Bucket, id.FileID)] = count
	}
	return json.Marshal(wire)
}

func decodeDiscardStats(data []byte) (map[manifest.ValueLogID]int64, error) {
	if len(data) == 0 {
		return nil, nil
	}
	wire := make(map[string]int64)
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, err
	}
	out := make(map[manifest.ValueLogID]int64, len(wire))
	for key, count := range wire {
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid discard stat key: %s", key)
		}
		bucket, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid discard stat bucket: %w", err)
		}
		fid, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid discard stat fid: %w", err)
		}
		out[manifest.ValueLogID{Bucket: uint32(bucket), FileID: uint32(fid)}] = count
	}
	return out, nil
}

func shouldSkipValueLogRewrite(e *kv.Entry) bool {
	if e == nil {
		return true
	}
	if e.IsDeletedOrExpired() {
		return true
	}
	return !kv.IsValuePtr(e)
}

type valueLog struct {
	dirPath            string
	bucketCount        uint32
	managers           []*vlogpkg.Manager
	filesToDeleteLock  sync.Mutex
	filesToBeDeleted   []manifest.ValueLogID
	numActiveIterators atomic.Int32
	db                 *DB
	opt                Options
	gcTokens           chan struct{}
	gcParallelism      int
	gcBucketBusy       []atomic.Uint32
	gcPickSeed         atomic.Uint64
	garbageCh          chan struct{}
	lfDiscardStats     *valueLogDiscardStats
}

func (vlog *valueLog) setValueLogFileSize(sz int) {
	if vlog == nil || sz <= 0 {
		return
	}
	vlog.opt.ValueLogFileSize = sz
	if vlog.db != nil && vlog.db.opt != nil {
		vlog.db.opt.ValueLogFileSize = sz
	}
	for _, mgr := range vlog.managers {
		if mgr == nil {
			continue
		}
		mgr.SetMaxSize(int64(sz))
	}
}

func (vlog *valueLog) logf(format string, args ...any) {
	if vlog == nil || !vlog.opt.ValueLogVerbose {
		return
	}
	slog.Default().Info(fmt.Sprintf(format, args...))
}

// metrics captures backlog counters for the value log.
func (vlog *valueLog) metrics() metrics.ValueLogMetrics {
	if vlog == nil || len(vlog.managers) == 0 {
		return metrics.ValueLogMetrics{}
	}
	heads := make(map[uint32]kv.ValuePtr, len(vlog.managers))
	segments := 0
	for bucket, mgr := range vlog.managers {
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

	if vlog.lfDiscardStats != nil {
		stats.DiscardQueue = len(vlog.lfDiscardStats.flushChan)
	}

	vlog.filesToDeleteLock.Lock()
	stats.PendingDeletes = len(vlog.filesToBeDeleted)
	vlog.filesToDeleteLock.Unlock()

	return stats
}

func (vlog *valueLog) reconcileManifest(status map[manifest.ValueLogID]manifest.ValueLogMeta) {
	if vlog == nil || len(vlog.managers) == 0 || len(status) == 0 {
		return
	}
	for bucket, mgr := range vlog.managers {
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

func (vlog *valueLog) managerFor(bucket uint32) (*vlogpkg.Manager, error) {
	if vlog == nil || int(bucket) >= len(vlog.managers) {
		return nil, fmt.Errorf("value log: invalid bucket %d", bucket)
	}
	mgr := vlog.managers[bucket]
	if mgr == nil {
		return nil, fmt.Errorf("value log: missing manager for bucket %d", bucket)
	}
	return mgr, nil
}

func (vlog *valueLog) bucketForEntry(e *kv.Entry) uint32 {
	if vlog == nil || e == nil {
		return 0
	}
	buckets := vlog.bucketCount
	if buckets <= 1 {
		return 0
	}
	return kv.ValueLogBucket(e.Key, buckets)
}

func (vlog *valueLog) removeValueLogFile(bucket uint32, fid uint32) error {
	if vlog == nil || vlog.db == nil || vlog.db.lsm == nil {
		return fmt.Errorf("valueLog.removeValueLogFile: missing dependencies")
	}
	mgr, err := vlog.managerFor(bucket)
	if err != nil {
		return err
	}
	status := vlog.db.lsm.ValueLogStatusSnapshot()
	var (
		meta    manifest.ValueLogMeta
		hasMeta bool
	)
	if status != nil {
		meta, hasMeta = status[manifest.ValueLogID{Bucket: bucket, FileID: fid}]
	}
	if err := vlog.db.lsm.LogValueLogDelete(bucket, fid); err != nil {
		return errors.Wrapf(err, "log value log delete fid %d (bucket %d)", fid, bucket)
	}
	if err := mgr.Remove(fid); err != nil {
		if hasMeta {
			if errRestore := vlog.db.lsm.LogValueLogUpdate(&meta); errRestore != nil {
				slog.Default().Error("value log delete rollback", "fid", fid, "bucket", bucket, "error", errRestore)
			}
		}
		return errors.Wrapf(err, "remove value log fid %d (bucket %d)", fid, bucket)
	}
	metrics.DefaultValueLogGCCollector().IncSegmentsRemoved()
	return nil
}

func (vlog *valueLog) newValuePtr(e *kv.Entry) (*kv.ValuePtr, error) {
	req := dbruntime.RequestPool.Get().(*dbruntime.Request)
	req.Reset()
	req.Entries = []*kv.Entry{e}
	req.IncrRef()
	defer func() {
		req.Entries = nil
		req.DecrRef()
	}()

	if err := vlog.write([]*dbruntime.Request{req}); err != nil {
		return nil, err
	}
	if len(req.Ptrs) == 0 {
		return nil, errors.New("valueLog.newValuePtr: missing value pointer")
	}
	vp := req.Ptrs[0]
	return &vp, nil
}

func (vlog *valueLog) open(heads map[uint32]kv.ValuePtr, replayFn kv.LogEntry) error {
	if replayFn == nil {
		replayFn = func(*kv.Entry, *kv.ValuePtr) error { return nil }
	}
	vlog.lfDiscardStats.closer.Add(1)
	go vlog.flushDiscardStats()

	if len(vlog.managers) == 0 {
		return errors.New("valueLog.open: no value log buckets found")
	}
	vlog.filesToDeleteLock.Lock()
	vlog.filesToBeDeleted = nil
	vlog.filesToDeleteLock.Unlock()

	for bucket, mgr := range vlog.managers {
		if mgr == nil {
			continue
		}
		fids := mgr.ListFIDs()
		if len(fids) == 0 {
			return fmt.Errorf("valueLog.open: no value log files found for bucket %d", bucket)
		}
		head := heads[uint32(bucket)]
		activeFID := mgr.ActiveFID()
		for _, fid := range fids {
			offset := uint32(0)
			if head.Bucket == uint32(bucket) && fid == head.Fid {
				offset = head.Offset + head.Len
			}
			vlog.logf("Scanning file id: %d bucket: %d at offset: %d", fid, bucket, offset)
			start := time.Now()
			if err := vlog.replayLog(uint32(bucket), fid, offset, replayFn, fid == activeFID); err != nil {
				if err == utils.ErrDeleteVlogFile {
					if removeErr := vlog.removeValueLogFile(uint32(bucket), fid); removeErr != nil {
						return removeErr
					}
					continue
				}
				return err
			}
			vlog.logf("Scan took: %s", time.Since(start))

			if fid != activeFID {
				if err := mgr.SegmentInit(fid); err != nil {
					return err
				}
			}
		}
		if vlog.db.vheads == nil {
			vlog.db.vheads = make(map[uint32]kv.ValuePtr)
		}
		headPtr := mgr.Head()
		if _, ok := vlog.db.vheads[uint32(bucket)]; !ok || vlog.db.vheads[uint32(bucket)].IsZero() {
			vlog.db.vheads[uint32(bucket)] = headPtr
		}
	}
	if err := vlog.populateDiscardStats(); err != nil {
		if err != utils.ErrKeyNotFound {
			slog.Default().Warn("populate discard stats", "error", err)
		}
	}
	return nil
}

func (vlog *valueLog) read(vp *kv.ValuePtr) ([]byte, func(), error) {
	if vp == nil {
		return nil, nil, errors.New("valueLog.read: nil value pointer")
	}
	mgr, err := vlog.managerFor(vp.Bucket)
	if err != nil {
		return nil, nil, err
	}
	return mgr.ReadValue(vp, vlogpkg.ReadOptions{
		Mode:                vlogpkg.ReadModeAuto,
		SmallValueThreshold: valueLogSmallCopyThreshold,
	})
}

func (vlog *valueLog) write(reqs []*dbruntime.Request) error {
	heads := make(map[uint32]kv.ValuePtr)
	touched := make(map[uint32]struct{})
	fail := func(err error, context string) error {
		for _, req := range reqs {
			req.Ptrs = req.Ptrs[:0]
			req.PtrIdxs = req.PtrIdxs[:0]
			req.PtrBuckets = req.PtrBuckets[:0]
		}
		for bucket := range touched {
			mgr, mgrErr := vlog.managerFor(bucket)
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
			if !vlog.db.peekShouldWriteValueToLSM(e) {
				wrote = true
				bucket := vlog.bucketForEntry(e)
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
			mgr, err := vlog.managerFor(bucket)
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
	if wrote && vlog.db != nil && vlog.db.opt.SyncWrites {
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
			mgr, err := vlog.managerFor(bucket)
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

func (vlog *valueLog) close() error {
	if vlog == nil || vlog.db == nil {
		return nil
	}
	<-vlog.lfDiscardStats.closer.Closed()
	var firstErr error
	for _, mgr := range vlog.managers {
		if mgr == nil {
			continue
		}
		if err := mgr.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (db *DB) initVLog() {
	heads := db.getHeads()
	vlogDir := filepath.Join(db.opt.WorkDir, "vlog")

	bucketCount := max(db.opt.ValueLogBucketCount, 1)
	gcParallelism := db.opt.ValueLogGCParallelism
	if gcParallelism <= 0 {
		gcParallelism = max(db.opt.NumCompactors/2, 1)
	}
	if gcParallelism < 1 {
		gcParallelism = 1
	}
	if gcParallelism > bucketCount {
		gcParallelism = bucketCount
	}

	managers := make([]*vlogpkg.Manager, bucketCount)
	for bucket := range bucketCount {
		bucketDir := filepath.Join(vlogDir, fmt.Sprintf("bucket-%03d", bucket))
		manager, err := vlogpkg.Open(vlogpkg.Config{
			Dir:      bucketDir,
			FileMode: utils.DefaultFileMode,
			MaxSize:  int64(db.opt.ValueLogFileSize),
			Bucket:   uint32(bucket),
			FS:       db.fs,
		})
		utils.Panic(err)
		managers[bucket] = manager
	}

	status := db.lsm.ValueLogStatusSnapshot()

	threshold := db.opt.DiscardStatsFlushThreshold

	vlog := &valueLog{
		dirPath:          vlogDir,
		bucketCount:      uint32(bucketCount),
		managers:         managers,
		filesToBeDeleted: make([]manifest.ValueLogID, 0),
		lfDiscardStats:   newValueLogDiscardStats(db.discardStatsCh, threshold),
		db:               db,
		opt:              *db.opt,
		gcTokens:         make(chan struct{}, gcParallelism),
		gcParallelism:    gcParallelism,
		gcBucketBusy:     make([]atomic.Uint32, bucketCount),
		garbageCh:        make(chan struct{}, 1),
	}
	metrics.DefaultValueLogGCCollector().SetParallelism(gcParallelism)
	vlog.setValueLogFileSize(db.opt.ValueLogFileSize)
	vlog.reconcileManifest(status)
	db.vheads = heads
	if db.vheads == nil {
		db.vheads = make(map[uint32]kv.ValuePtr)
	}
	db.lastLoggedHeads = make(map[uint32]kv.ValuePtr, len(db.vheads))
	maps.Copy(db.lastLoggedHeads, db.vheads)
	if err := vlog.open(heads, nil); err != nil {
		utils.Panic(err)
	}
	db.vlog = vlog
}

func (db *DB) getHeads() map[uint32]kv.ValuePtr {
	heads := db.lsm.ValueLogHeadSnapshot()
	if len(heads) == 0 {
		return make(map[uint32]kv.ValuePtr)
	}
	return heads
}

func (db *DB) valueLogStatusSnapshot() map[manifest.ValueLogID]manifest.ValueLogMeta {
	if db == nil || db.lsm == nil {
		return nil
	}
	return db.lsm.ValueLogStatusSnapshot()
}

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
		mgr, err := db.vlog.managerFor(bucket)
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

func (vlog *valueLog) replayLog(bucket uint32, fid uint32, offset uint32, replayFn kv.LogEntry, isActive bool) error {
	mgr, err := vlog.managerFor(bucket)
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

	vlog.logf("Truncating vlog file %05d (bucket %d) to offset: %d", fid, bucket, endOffset)
	if err := mgr.SegmentTruncate(fid, endOffset); err != nil {
		return fmt.Errorf("truncation needed at offset %d. Can be done manually as well: %w", endOffset, err)
	}
	return nil
}
