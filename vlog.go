package NoKV

import (
	"fmt"
	"maps"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/feichai0017/hotring"
	"github.com/pkg/errors"
)

const (
	defaultDiscardStatsFlushThreshold = 100
	valueLogHeadLogInterval           = uint32(1 << 20) // 1 MiB persistence interval for value-log head.
	valueLogSmallCopyThreshold        = 4 << 10         // copy small values to reduce read lock hold.
)

var lfDiscardStatsKey = []byte("!NoKV!discard") // For storing lfDiscardStats

type valueLog struct {
	dirPath            string
	bucketCount        uint32
	managers           []*vlogpkg.Manager
	filesToDeleteLock  sync.Mutex
	filesToBeDeleted   []manifest.ValueLogID
	numActiveIterators int32
	db                 *DB
	opt                Options
	hot                *hotring.HotRing
	gcTokens           chan struct{}
	gcParallelism      int
	gcBucketBusy       []atomic.Uint32
	gcPickSeed         uint64
	garbageCh          chan struct{}
	lfDiscardStats     *lfDiscardStats
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
	fmt.Printf(format+"\n", args...)
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
						_ = utils.Err(fmt.Errorf("value log reconcile remove fid %d (bucket %d): %v", fid, bucket, err))
						continue
					}
					delete(existing, fid)
					metrics.IncValueLogSegmentsRemoved()
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
			_ = utils.Err(fmt.Errorf("value log reconcile: manifest references missing file %d (bucket %d)", fid, bucket))
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
				_ = utils.Err(fmt.Errorf("value log reconcile remove orphan fid %d (bucket %d): %v", fid, bucket, err))
				continue
			}
			metrics.IncValueLogSegmentsRemoved()
			_ = utils.Err(fmt.Errorf("value log reconcile: removed untracked value log segment %d (bucket %d)", fid, bucket))
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
	hotBuckets := vlog.opt.ValueLogHotBucketCount
	threshold := vlog.opt.ValueLogHotKeyThreshold
	if hotBuckets <= 0 || threshold <= 0 || vlog.hot == nil {
		return kv.ValueLogBucket(e.Key, buckets)
	}
	if uint32(hotBuckets) >= buckets {
		hotBuckets = int(buckets) - 1
		if hotBuckets <= 0 {
			return kv.ValueLogBucket(e.Key, buckets)
		}
	}

	cf := e.CF
	userKey := e.Key
	if len(e.Key) > 0 {
		parsedCF, parsedKey, _ := kv.SplitInternalKey(e.Key)
		if len(parsedKey) > 0 {
			userKey = parsedKey
			if parsedCF.Valid() {
				cf = parsedCF
			}
		}
	}
	skey := cfHotKey(cf, userKey)
	if skey == "" {
		return kv.ValueLogBucket(e.Key, buckets)
	}

	count := vlog.hot.Touch(skey)
	hash := kv.ValueLogHash(e.Key)
	if count >= threshold {
		return hash % uint32(hotBuckets)
	}
	coldBuckets := buckets - uint32(hotBuckets)
	if coldBuckets == 0 {
		return kv.ValueLogBucketFromHash(hash, buckets)
	}
	return uint32(hotBuckets) + (hash % coldBuckets)
}

func (vlog *valueLog) removeValueLogFile(bucket uint32, fid uint32) error {
	if vlog == nil || vlog.db == nil || vlog.db.lsm == nil {
		return fmt.Errorf("valueLog.removeValueLogFile: missing dependencies")
	}
	mgr, err := vlog.managerFor(bucket)
	if err != nil {
		return err
	}
	status := vlog.db.lsm.ValueLogStatus()
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
				_ = utils.Err(fmt.Errorf("value log delete rollback fid %d (bucket %d): %v", fid, bucket, errRestore))
			}
		}
		return errors.Wrapf(err, "remove value log fid %d (bucket %d)", fid, bucket)
	}
	metrics.IncValueLogSegmentsRemoved()
	return nil
}

func (vlog *valueLog) newValuePtr(e *kv.Entry) (*kv.ValuePtr, error) {
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = []*kv.Entry{e}
	req.IncrRef()
	defer func() {
		req.Entries = nil
		req.DecrRef()
	}()

	if err := vlog.write([]*request{req}); err != nil {
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
			_ = utils.Err(fmt.Errorf("failed to populate discard stats: %w", err))
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

func (vlog *valueLog) write(reqs []*request) error {
	heads := make(map[uint32]kv.ValuePtr)
	touched := make(map[uint32]struct{})
	fail := func(err error, context string) error {
		for _, req := range reqs {
			req.Ptrs = req.Ptrs[:0]
		}
		for bucket := range touched {
			mgr, mgrErr := vlog.managerFor(bucket)
			if mgrErr != nil {
				continue
			}
			if head, ok := heads[bucket]; ok {
				if rewindErr := mgr.Rewind(head); rewindErr != nil {
					_ = utils.Err(fmt.Errorf("%s: %v", context, rewindErr))
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
		if cap(req.Ptrs) < len(req.Entries) {
			req.Ptrs = make([]kv.ValuePtr, len(req.Entries))
		} else {
			req.Ptrs = req.Ptrs[:len(req.Entries)]
			for i := range req.Ptrs {
				req.Ptrs[i] = kv.ValuePtr{}
			}
		}
		bucketEntries := make(map[uint32][]int)
		for i, e := range req.Entries {
			if !vlog.db.shouldWriteValueToLSM(e) {
				wrote = true
				bucket := vlog.bucketForEntry(e)
				bucketEntries[bucket] = append(bucketEntries[bucket], i)
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
			touched[bucket] = struct{}{}
		}
	}
	if wrote && vlog.db != nil && vlog.db.opt.SyncWrites {
		byBucket := make(map[uint32]map[uint32]struct{})
		for _, req := range reqs {
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
	<-vlog.lfDiscardStats.closer.CloseSignal
	if vlog.hot != nil {
		vlog.hot.Close()
	}
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

	var hot *hotring.HotRing
	if db.opt.HotRingEnabled &&
		db.opt.ValueLogHotBucketCount > 0 &&
		db.opt.ValueLogHotKeyThreshold > 0 {
		hot = hotring.NewHotRing(db.opt.HotRingBits, nil)
		if db.opt.HotRingWindowSlots > 0 && db.opt.HotRingWindowSlotDuration > 0 {
			hot.EnableSlidingWindow(db.opt.HotRingWindowSlots, db.opt.HotRingWindowSlotDuration)
		}
		if db.opt.HotRingDecayInterval > 0 && db.opt.HotRingDecayShift > 0 {
			hot.EnableDecay(db.opt.HotRingDecayInterval, db.opt.HotRingDecayShift)
		}
	}

	managers := make([]*vlogpkg.Manager, bucketCount)
	for bucket := range bucketCount {
		bucketDir := filepath.Join(vlogDir, fmt.Sprintf("bucket-%03d", bucket))
		manager, err := vlogpkg.Open(vlogpkg.Config{
			Dir:      bucketDir,
			FileMode: utils.DefaultFileMode,
			MaxSize:  int64(db.opt.ValueLogFileSize),
			Bucket:   uint32(bucket),
		})
		utils.Panic(err)
		managers[bucket] = manager
	}

	status := db.lsm.ValueLogStatus()

	threshold := db.opt.DiscardStatsFlushThreshold
	if threshold <= 0 {
		threshold = defaultDiscardStatsFlushThreshold
	}

	vlog := &valueLog{
		dirPath:          vlogDir,
		bucketCount:      uint32(bucketCount),
		managers:         managers,
		filesToBeDeleted: make([]manifest.ValueLogID, 0),
		lfDiscardStats: &lfDiscardStats{
			m:              make(map[manifest.ValueLogID]int64),
			closer:         utils.NewCloser(),
			flushChan:      make(chan map[manifest.ValueLogID]int64, 16),
			flushThreshold: threshold,
		},
		db:            db,
		opt:           *db.opt,
		hot:           hot,
		gcTokens:      make(chan struct{}, gcParallelism),
		gcParallelism: gcParallelism,
		gcBucketBusy:  make([]atomic.Uint32, bucketCount),
		garbageCh:     make(chan struct{}, 1),
	}
	metrics.SetValueLogGCParallelism(gcParallelism)
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
	heads := db.lsm.ValueLogHead()
	if len(heads) == 0 {
		return make(map[uint32]kv.ValuePtr)
	}
	return heads
}

func (db *DB) updateHead(ptrs []kv.ValuePtr) {
	touched := make(map[uint32]struct{})
	for _, p := range ptrs {
		if p.IsZero() {
			continue
		}
		touched[p.Bucket] = struct{}{}
	}
	if len(touched) == 0 {
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
	for bucket := range touched {
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
			utils.CondPanic(true, fmt.Errorf("value log head regression: bucket=%d prev=%+v next=%+v", bucket, prev, next))
		}
		db.vheads[bucket] = *next
		if !db.shouldPersistHead(next, bucket) {
			continue
		}
		if err := db.lsm.LogValueLogHead(next); err != nil {
			_ = utils.Err(fmt.Errorf("log value log head: %w", err))
			continue
		}
		metrics.IncValueLogHeadUpdates()
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
		return utils.WarpErr(fmt.Sprintf("Truncation needed at offset %d. Can be done manually as well.", endOffset), err)
	}
	return nil
}
