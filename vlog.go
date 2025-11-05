package NoKV

import (
	"encoding/json"
	"expvar"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/pkg/errors"
)

const (
	defaultDiscardStatsFlushThreshold = 100
	valueLogHeadLogInterval           = uint32(1 << 20) // 1 MiB persistence interval for value-log head.
)

var lfDiscardStatsKey = []byte("!NoKV!discard") // For storing lfDiscardStats

var (
	valueLogGCRuns          = expvar.NewInt("NoKV.ValueLog.GcRuns")
	valueLogSegmentsRemoved = expvar.NewInt("NoKV.ValueLog.SegmentsRemoved")
	valueLogHeadUpdates     = expvar.NewInt("NoKV.ValueLog.HeadUpdates")
)

type valueLog struct {
	dirPath            string
	manager            *vlogpkg.Manager
	filesToDeleteLock  sync.Mutex
	filesToBeDeleted   []uint32
	numActiveIterators int32
	db                 *DB
	opt                Options
	garbageCh          chan struct{}
	lfDiscardStats     *lfDiscardStats
}

func (vlog *valueLog) logf(format string, args ...interface{}) {
	if vlog == nil || !vlog.opt.ValueLogVerbose {
		return
	}
	fmt.Printf(format+"\n", args...)
}

type valueLogMetrics struct {
	Segments       int
	PendingDeletes int
	DiscardQueue   int
	Head           kv.ValuePtr
}

func (vlog *valueLog) metrics() valueLogMetrics {
	if vlog == nil || vlog.manager == nil {
		return valueLogMetrics{}
	}
	stats := valueLogMetrics{
		Segments: len(vlog.manager.ListFIDs()),
		Head:     vlog.manager.Head(),
	}

	if vlog.lfDiscardStats != nil {
		stats.DiscardQueue = len(vlog.lfDiscardStats.flushChan)
	}

	vlog.filesToDeleteLock.Lock()
	stats.PendingDeletes = len(vlog.filesToBeDeleted)
	vlog.filesToDeleteLock.Unlock()

	return stats
}

func (vlog *valueLog) reconcileManifest(status map[uint32]manifest.ValueLogMeta) {
	if vlog == nil || vlog.manager == nil || len(status) == 0 {
		return
	}
	existing := make(map[uint32]struct{})
	for _, fid := range vlog.manager.ListFIDs() {
		existing[fid] = struct{}{}
	}
	var (
		maxTracked uint32
		maxValid   uint32
		hasValid   bool
	)
	for fid, meta := range status {
		if fid > maxTracked {
			maxTracked = fid
		}
		if !meta.Valid {
			if _, ok := existing[fid]; ok {
				if err := vlog.manager.Remove(fid); err != nil {
					utils.Err(fmt.Errorf("value log reconcile remove fid %d: %v", fid, err))
					continue
				}
				delete(existing, fid)
				valueLogSegmentsRemoved.Add(1)
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
		utils.Err(fmt.Errorf("value log reconcile: manifest references missing file %d", fid))
	}
	if !hasValid {
		return
	}
	threshold := maxValid
	for fid := range existing {
		if fid <= threshold {
			continue
		}
		if err := vlog.manager.Remove(fid); err != nil {
			utils.Err(fmt.Errorf("value log reconcile remove orphan fid %d: %v", fid, err))
			continue
		}
		valueLogSegmentsRemoved.Add(1)
		utils.Err(fmt.Errorf("value log reconcile: removed untracked value log segment %d", fid))
	}
}

func (vlog *valueLog) removeValueLogFile(fid uint32) error {
	if vlog == nil || vlog.db == nil || vlog.db.lsm == nil {
		return fmt.Errorf("valueLog.removeValueLogFile: missing dependencies")
	}
	status := vlog.db.lsm.ValueLogStatus()
	var (
		meta    manifest.ValueLogMeta
		hasMeta bool
	)
	if status != nil {
		meta, hasMeta = status[fid]
	}
	if err := vlog.db.lsm.LogValueLogDelete(fid); err != nil {
		return errors.Wrapf(err, "log value log delete fid %d", fid)
	}
	if err := vlog.manager.Remove(fid); err != nil {
		if hasMeta {
			if errRestore := vlog.db.lsm.LogValueLogUpdate(&meta); errRestore != nil {
				utils.Err(fmt.Errorf("value log delete rollback fid %d: %v", fid, errRestore))
			}
		}
		return errors.Wrapf(err, "remove value log fid %d", fid)
	}
	valueLogSegmentsRemoved.Add(1)
	return nil
}

func (vlog *valueLog) newValuePtr(e *kv.Entry) (*kv.ValuePtr, error) {
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = []*kv.Entry{e}
	req.IncrRef()
	defer func() {
		req.Entries = nil // Break the link to avoid resetting the entry.
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

func (vlog *valueLog) open(ptr *kv.ValuePtr, replayFn kv.LogEntry) error {
	vlog.lfDiscardStats.closer.Add(1)
	go vlog.flushDiscardStats()

	fids := vlog.manager.ListFIDs()
	if len(fids) == 0 {
		return errors.New("valueLog.open: no value log files found")
	}
	vlog.filesToDeleteLock.Lock()
	vlog.filesToBeDeleted = nil
	vlog.filesToDeleteLock.Unlock()

	activeFID := vlog.manager.ActiveFID()
	for _, fid := range fids {
		offset := uint32(0)
		if fid == ptr.Fid {
			offset = ptr.Offset + ptr.Len
		}
		vlog.logf("Replaying file id: %d at offset: %d", fid, offset)
		start := time.Now()
		if err := vlog.replayLog(fid, offset, replayFn, fid == activeFID); err != nil {
			if err == utils.ErrDeleteVlogFile {
				if removeErr := vlog.removeValueLogFile(fid); removeErr != nil {
					return removeErr
				}
				continue
			}
			return err
		}
		vlog.logf("Replay took: %s", time.Since(start))

		if fid != activeFID {
			if err := vlog.manager.SegmentInit(fid); err != nil {
				return err
			}
		}
	}

	head := vlog.manager.Head()
	if vlog.db.vhead == nil || vlog.db.vhead.IsZero() {
		vlog.db.vhead = &head
	}
	if err := vlog.populateDiscardStats(); err != nil {
		if err != utils.ErrKeyNotFound {
			utils.Err(fmt.Errorf("failed to populate discard stats: %w", err))
		}
	}
	return nil
}

func (vlog *valueLog) read(vp *kv.ValuePtr) ([]byte, func(), error) {
	data, unlock, err := vlog.manager.Read(vp)
	if err != nil {
		return nil, unlock, err
	}
	val, _, err := kv.DecodeValueSlice(data)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	return val, unlock, nil
}

func (vlog *valueLog) write(reqs []*request) error {
	head := vlog.manager.Head()
	fail := func(err error, context string) error {
		for _, req := range reqs {
			req.Ptrs = req.Ptrs[:0]
		}
		if rewindErr := vlog.manager.Rewind(head); rewindErr != nil {
			utils.Err(fmt.Errorf("%s: %v", context, rewindErr))
		}
		return err
	}

	for _, req := range reqs {
		req.Ptrs = req.Ptrs[:0]
		for _, e := range req.Entries {
			if vlog.db.shouldWriteValueToLSM(e) {
				req.Ptrs = append(req.Ptrs, kv.ValuePtr{})
				continue
			}
			ptr, err := vlog.manager.AppendEntry(e)
			if err != nil {
				return fail(err, "rewind value log after append failure")
			}
			req.Ptrs = append(req.Ptrs, *ptr)

			if int(ptr.Offset)+int(ptr.Len) > vlog.opt.ValueLogFileSize {
				if err := vlog.manager.Rotate(); err != nil {
					return fail(err, "rewind value log after rotate failure")
				}
				atomic.AddInt32(&vlog.db.logRotates, 1)
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
	return vlog.manager.Close()
}

func (vlog *valueLog) runGC(discardRatio float64, head *kv.ValuePtr) error {
	select {
	case vlog.garbageCh <- struct{}{}:
		defer func() {
			<-vlog.garbageCh
		}()

		files := vlog.pickLog(head)
		if len(files) == 0 {
			return utils.ErrNoRewrite
		}
		tried := make(map[uint32]bool)
		for _, fid := range files {
			if _, done := tried[fid]; done {
				continue
			}
			tried[fid] = true
			if err := vlog.doRunGC(fid, discardRatio); err == nil {
				return nil
			} else if err != utils.ErrNoRewrite {
				return err
			}
		}
		return utils.ErrNoRewrite
	default:
		return utils.ErrRejected
	}
}

func (vlog *valueLog) doRunGC(fid uint32, discardRatio float64) (err error) {
	defer func() {
		if err == nil {
			vlog.lfDiscardStats.Lock()
			delete(vlog.lfDiscardStats.m, fid)
			vlog.lfDiscardStats.Unlock()
		}
	}()

	opts := vlogpkg.SampleOptions{
		SizeRatio:     vlog.gcSampleSizeRatio(),
		CountRatio:    vlog.gcSampleCountRatio(),
		FromBeginning: vlog.opt.ValueLogGCSampleFromHead,
		MaxEntries:    vlog.opt.ValueLogMaxEntries,
	}
	start := time.Now()
	stats, err := vlog.manager.Sample(fid, opts, func(e *kv.Entry, vp *kv.ValuePtr) (bool, error) {
		if time.Since(start) > 10*time.Second {
			return false, utils.ErrStop
		}
		cf, userKey, _ := kv.SplitInternalKey(e.Key)
		entry, err := vlog.db.GetCF(cf, userKey)
		if err != nil {
			return false, err
		}
		if kv.DiscardEntry(e, entry) {
			return true, nil
		}

		if len(entry.Value) == 0 {
			return false, nil
		}
		var newVP kv.ValuePtr
		newVP.Decode(entry.Value)

		if newVP.Fid > fid || (newVP.Fid == fid && newVP.Offset > e.Offset) {
			return true, nil
		}
		return false, nil
	})
	if err != nil && err != utils.ErrStop {
		return err
	}
	if stats == nil {
		return utils.ErrNoRewrite
	}

	vlog.logf("Fid: %d. Skipped: %5.2fMB Data status={total:%5.2f discard:%5.2f count:%d}", fid, stats.SkippedMiB, stats.TotalMiB, stats.DiscardMiB, stats.Count)

	sizeWindow := stats.SizeWindow
	if sizeWindow == 0 {
		sizeWindow = float64(vlog.opt.ValueLogFileSize) / float64(utils.Mi)
	}
	if (stats.Count < stats.CountWindow && stats.TotalMiB < sizeWindow*0.75) || stats.DiscardMiB < discardRatio*stats.TotalMiB {
		return utils.ErrNoRewrite
	}

	if err = vlog.rewrite(fid); err != nil {
		return err
	}
	valueLogGCRuns.Add(1)
	return nil
}

func (vlog *valueLog) rewrite(fid uint32) error {
	activeFID := vlog.manager.ActiveFID()
	utils.CondPanic(fid >= activeFID, fmt.Errorf("fid to move: %d. Current active fid: %d", fid, activeFID))

	wb := make([]*kv.Entry, 0, 1000)
	var size int64

	process := func(e *kv.Entry, ptr *kv.ValuePtr) error {
		entry, err := vlog.db.lsm.Get(e.Key)
		if err != nil {
			return err
		}
		if kv.DiscardEntry(e, entry) {
			return nil
		}

		if len(entry.Value) == 0 {
			return errors.Errorf("empty value: %+v", entry)
		}

		var diskVP kv.ValuePtr
		diskVP.Decode(entry.Value)

		if diskVP.Fid > fid || (diskVP.Fid == fid && diskVP.Offset > ptr.Offset) {
			return nil
		}

		ne := kv.EntryPool.Get().(*kv.Entry)
		ne.IncrRef()
		ne.Meta = 0
		ne.ExpiresAt = e.ExpiresAt
		ne.Key = append(ne.Key[:0], e.Key...)
		ne.Value = append(ne.Value[:0], e.Value...)

		es := int64(ne.EstimateSize(vlog.db.opt.ValueLogFileSize))
		es += int64(len(e.Value))
		if int64(len(wb)+1) >= vlog.opt.MaxBatchCount || size+es >= vlog.opt.MaxBatchSize {
			if err := vlog.db.batchSet(wb); err != nil {
				return err
			}
			size = 0
			wb = wb[:0]
		}
		wb = append(wb, ne)
		size += es
		return nil
	}

	if _, err := vlog.manager.Iterate(fid, 0, func(e *kv.Entry, vp *kv.ValuePtr) error {
		return process(e, vp)
	}); err != nil && err != utils.ErrStop {
		return err
	}

	batchSize := 1024
	for i := 0; i < len(wb); {
		end := min(i+batchSize, len(wb))
		if err := vlog.db.batchSet(wb[i:end]); err != nil {
			if err == utils.ErrTxnTooBig {
				if batchSize <= 1 {
					return utils.ErrNoRewrite
				}
				batchSize = batchSize / 2
				continue
			}
			return err
		}
		i += batchSize
	}
	if len(wb) > 0 {
		testKey := wb[len(wb)-1].Key
		if vs, err := vlog.db.lsm.Get(testKey); err == nil {
			var vp kv.ValuePtr
			vp.Decode(vs.Value)
		} else {
			return err
		}
	}

	deleteNow := false
	vlog.filesToDeleteLock.Lock()
	if vlog.iteratorCount() == 0 {
		deleteNow = true
	} else {
		vlog.filesToBeDeleted = append(vlog.filesToBeDeleted, fid)
	}
	vlog.filesToDeleteLock.Unlock()

	if deleteNow {
		if err := vlog.removeValueLogFile(fid); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) iteratorCount() int {
	return int(atomic.LoadInt32(&vlog.numActiveIterators))
}

func (vlog *valueLog) decrIteratorCount() error {
	if atomic.AddInt32(&vlog.numActiveIterators, -1) != 0 {
		return nil
	}

	vlog.filesToDeleteLock.Lock()
	fids := append([]uint32(nil), vlog.filesToBeDeleted...)
	vlog.filesToBeDeleted = nil
	vlog.filesToDeleteLock.Unlock()

	for _, fid := range fids {
		if err := vlog.removeValueLogFile(fid); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) filterPendingDeletes(fids []uint32) []uint32 {
	vlog.filesToDeleteLock.Lock()
	defer vlog.filesToDeleteLock.Unlock()

	if len(vlog.filesToBeDeleted) == 0 {
		out := make([]uint32, len(fids))
		copy(out, fids)
		return out
	}

	toDelete := make(map[uint32]struct{}, len(vlog.filesToBeDeleted))
	for _, fid := range vlog.filesToBeDeleted {
		toDelete[fid] = struct{}{}
	}

	out := make([]uint32, 0, len(fids))
	for _, fid := range fids {
		if _, ok := toDelete[fid]; ok {
			continue
		}
		out = append(out, fid)
	}
	return out
}

func (vlog *valueLog) pickLog(head *kv.ValuePtr) (files []uint32) {
	fids := vlog.manager.ListFIDs()
	if len(fids) <= 1 {
		return nil
	}
	fids = vlog.filterPendingDeletes(fids)
	if len(fids) <= 1 {
		return nil
	}

	activeFID := vlog.manager.ActiveFID()
	candidate := struct {
		fid     uint32
		discard int64
	}{math.MaxUint32, 0}

	vlog.lfDiscardStats.RLock()
	for _, fid := range fids {
		if fid >= head.Fid || fid >= activeFID {
			break
		}
		if vlog.lfDiscardStats.m[fid] > candidate.discard {
			candidate.fid = fid
			candidate.discard = vlog.lfDiscardStats.m[fid]
		}
	}
	vlog.lfDiscardStats.RUnlock()

	if candidate.fid != math.MaxUint32 {
		files = append(files, candidate.fid)
	}

	idxHead := 0
	for i, fid := range fids {
		if fid == head.Fid {
			idxHead = i
			break
		}
		if fid > head.Fid {
			idxHead = i
			break
		}
	}
	if idxHead == 0 {
		idxHead = 1
	}
	idx := rand.Intn(idxHead)
	if idx > 0 {
		idx = rand.Intn(idx + 1)
	}
	files = append(files, fids[idx])
	return files
}

func (vlog *valueLog) replayLog(fid uint32, offset uint32, replayFn kv.LogEntry, isActive bool) error {
	endOffset, err := vlog.manager.Iterate(fid, offset, replayFn)
	if err != nil {
		return errors.Wrapf(err, "Unable to replay logfile: fid=%d", fid)
	}
	size, err := vlog.manager.SegmentSize(fid)
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
		return vlog.manager.SegmentBootstrap(fid)
	}

	vlog.logf("Truncating vlog file %05d to offset: %d", fid, endOffset)
	if err := vlog.manager.SegmentTruncate(fid, endOffset); err != nil {
		return utils.WarpErr(
			fmt.Sprintf("Truncation needed at offset %d. Can be done manually as well.", endOffset), err)
	}
	return nil
}

func (vlog *valueLog) populateDiscardStats() error {
	var statsMap map[uint32]int64
	vs, err := vlog.db.GetCF(kv.CFDefault, lfDiscardStatsKey)
	if err != nil {
		return err
	}
	if vs.Meta == 0 && len(vs.Value) == 0 {
		return nil
	}
	val := vs.Value
	if kv.IsValuePtr(vs) {
		var vp kv.ValuePtr
		vp.Decode(val)
		result, cb, err := vlog.read(&vp)
		val = kv.SafeCopy(nil, result)
		kv.RunCallback(cb)
		if err != nil {
			return err
		}
	}
	if len(val) == 0 {
		return nil
	}
	if err := json.Unmarshal(val, &statsMap); err != nil {
		return errors.Wrapf(err, "failed to unmarshal discard stats")
	}
	vlog.logf("Value Log Discard stats: %v", statsMap)
	vlog.lfDiscardStats.flushChan <- statsMap
	return nil
}

func (vlog *valueLog) gcSampleSizeRatio() float64 {
	r := vlog.opt.ValueLogGCSampleSizeRatio
	if r <= 0 {
		return 0.10
	}
	return r
}

func (vlog *valueLog) gcSampleCountRatio() float64 {
	r := vlog.opt.ValueLogGCSampleCountRatio
	if r <= 0 {
		return 0.01
	}
	return r
}

func (db *DB) initVLog() {
	vp, _ := db.getHead()
	vlogDir := filepath.Join(db.opt.WorkDir, "vlog")
	manager, err := vlogpkg.Open(vlogpkg.Config{
		Dir:      vlogDir,
		FileMode: utils.DefaultFileMode,
		MaxSize:  int64(db.opt.ValueLogFileSize),
	})
	utils.Panic(err)

	status := db.lsm.ValueLogStatus()

	threshold := db.opt.DiscardStatsFlushThreshold
	if threshold <= 0 {
		threshold = defaultDiscardStatsFlushThreshold
	}

	vlog := &valueLog{
		dirPath:          vlogDir,
		manager:          manager,
		filesToBeDeleted: make([]uint32, 0),
		lfDiscardStats: &lfDiscardStats{
			m:              make(map[uint32]int64),
			closer:         utils.NewCloser(),
			flushChan:      make(chan map[uint32]int64, 16),
			flushThreshold: threshold,
		},
		db:        db,
		opt:       *db.opt,
		garbageCh: make(chan struct{}, 1),
	}
	vlog.reconcileManifest(status)
	db.vhead = vp
	if vp != nil {
		db.lastLoggedHead = *vp
	} else {
		db.lastLoggedHead = kv.ValuePtr{}
	}
	if err := vlog.open(vp, db.replayFunction()); err != nil {
		utils.Panic(err)
	}
	db.vlog = vlog
}

func (db *DB) getHead() (*kv.ValuePtr, uint64) {
	vp, ok := db.lsm.ValueLogHead()
	if !ok {
		var zero kv.ValuePtr
		return &zero, 0
	}
	ptr := vp
	return &ptr, uint64(ptr.Offset)
}

func (db *DB) replayFunction() func(*kv.Entry, *kv.ValuePtr) error {
	toLSM := func(k []byte, vs kv.ValueStruct) {
		e := kv.NewEntry(k, vs.Value)
		e.ExpiresAt = vs.ExpiresAt
		e.Meta = vs.Meta
		db.lsm.Set(e)
		e.DecrRef()
	}

	return func(e *kv.Entry, vp *kv.ValuePtr) error {
		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		var nv []byte
		meta := e.Meta
		if db.shouldWriteValueToLSM(e) {
			nv = make([]byte, len(e.Value))
			copy(nv, e.Value)
		} else {
			nv = vp.Encode()
			meta = meta | kv.BitValuePointer
		}
		db.updateHead([]kv.ValuePtr{*vp})

		v := kv.ValueStruct{
			Value:     nv,
			Meta:      meta,
			ExpiresAt: e.ExpiresAt,
		}
		toLSM(nk, v)
		return nil
	}
}

func (db *DB) updateHead(ptrs []kv.ValuePtr) {
	var (
		ptr   kv.ValuePtr
		found bool
	)
	for i := len(ptrs) - 1; i >= 0; i-- {
		p := ptrs[i]
		if !p.IsZero() {
			ptr = p
			found = true
			break
		}
	}
	if !found || ptr.IsZero() {
		return
	}

	if db.vlog == nil || db.vlog.manager == nil {
		return
	}
	head := db.vlog.manager.Head()
	if head.IsZero() {
		return
	}

	next := &kv.ValuePtr{Fid: head.Fid, Offset: head.Offset}
	if db.vhead != nil && next.Less(db.vhead) {
		utils.CondPanic(true, fmt.Errorf("value log head regression: prev=%+v next=%+v", db.vhead, next))
	}
	db.vhead = next
	if !db.shouldPersistHead(next) {
		return
	}
	if err := db.lsm.LogValueLogHead(next); err != nil {
		utils.Err(fmt.Errorf("log value log head: %w", err))
		return
	}
	valueLogHeadUpdates.Add(1)
	db.lastLoggedHead = *next
}

func (db *DB) shouldPersistHead(next *kv.ValuePtr) bool {
	if db == nil || next == nil || next.IsZero() {
		return false
	}
	if db.headLogDelta == 0 {
		return true
	}
	last := db.lastLoggedHead
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

type lfDiscardStats struct {
	sync.RWMutex
	m                 map[uint32]int64
	flushChan         chan map[uint32]int64
	closer            *utils.Closer
	updatesSinceFlush int
	flushThreshold    int
}

func (vlog *valueLog) flushDiscardStats() {
	defer vlog.lfDiscardStats.closer.Done()

	mergeStats := func(stats map[uint32]int64, force bool) ([]byte, error) {
		vlog.lfDiscardStats.Lock()
		defer vlog.lfDiscardStats.Unlock()
		if stats != nil {
			for fid, count := range stats {
				vlog.lfDiscardStats.m[fid] += count
				vlog.lfDiscardStats.updatesSinceFlush++
			}
		}

		threshold := vlog.lfDiscardStats.flushThreshold
		if threshold <= 0 {
			threshold = defaultDiscardStatsFlushThreshold
		}

		if !force && vlog.lfDiscardStats.updatesSinceFlush < threshold {
			return nil, nil
		}
		if vlog.lfDiscardStats.updatesSinceFlush == 0 {
			return nil, nil
		}

		encodedDS, err := json.Marshal(vlog.lfDiscardStats.m)
		if err != nil {
			return nil, err
		}
		vlog.lfDiscardStats.updatesSinceFlush = 0
		return encodedDS, nil
	}

	process := func(stats map[uint32]int64, force bool) error {
		encodedDS, err := mergeStats(stats, force)
		if err != nil || encodedDS == nil {
			return err
		}

		entries := []*kv.Entry{{
			Key:   kv.InternalKey(kv.CFDefault, lfDiscardStatsKey, 1),
			Value: encodedDS,
		}}
		req, err := vlog.db.sendToWriteCh(entries)
		if err != nil {
			return errors.Wrapf(err, "failed to push discard stats to write channel")
		}
		return req.Wait()
	}

	closer := vlog.lfDiscardStats.closer
	for {
		select {
		case <-closer.CloseSignal:
			for {
				select {
				case stats := <-vlog.lfDiscardStats.flushChan:
					if err := process(stats, false); err != nil {
						utils.Err(fmt.Errorf("unable to process discardstats with error: %s", err))
					}
				default:
					goto drainComplete
				}
			}
		drainComplete:
			if err := process(nil, true); err != nil {
				utils.Err(fmt.Errorf("unable to process discardstats with error: %s", err))
			}
			return
		case stats := <-vlog.lfDiscardStats.flushChan:
			if err := process(stats, false); err != nil {
				utils.Err(fmt.Errorf("unable to process discardstats with error: %s", err))
			}
		}
	}
}

var requestPool = sync.Pool{
	New: func() any {
		return new(request)
	},
}

type request struct {
	Entries   []*kv.Entry
	Ptrs      []kv.ValuePtr
	Err       error
	ref       int32
	enqueueAt time.Time
	doneCh    chan error
}

func (req *request) reset() {
	req.Entries = req.Entries[:0]
	req.Ptrs = req.Ptrs[:0]
	req.Err = nil
	req.ref = 0
	req.enqueueAt = time.Time{}
	req.doneCh = nil
}

func (req *request) loadEntries(entries []*kv.Entry) {
	if cap(req.Entries) < len(entries) {
		req.Entries = make([]*kv.Entry, len(entries))
	} else {
		req.Entries = req.Entries[:len(entries)]
	}
	copy(req.Entries, entries)
}

func (vlog *valueLog) waitOnGC(lc *utils.Closer) {
	defer lc.Done()
	<-lc.CloseSignal
	vlog.garbageCh <- struct{}{}
}

func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) DecrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	// Call DecrRef on all entries to release them back to the pool.
	for _, e := range req.Entries {
		e.DecrRef()
	}
	req.Entries = nil
	req.Ptrs = nil
	requestPool.Put(req)
}

func (req *request) Wait() error {
	if req.doneCh != nil {
		err, ok := <-req.doneCh
		if ok {
			req.Err = err
		} else if req.Err != nil {
			err = req.Err
		}
		req.Err = err
	}
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
}
