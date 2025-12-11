package NoKV

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/pkg/errors"
)

const (
	defaultDiscardStatsFlushThreshold = 100
	valueLogHeadLogInterval           = uint32(1 << 20) // 1 MiB persistence interval for value-log head.
)

var lfDiscardStatsKey = []byte("!NoKV!discard") // For storing lfDiscardStats

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

type valueLogWriter interface {
	WriteRequests(reqs []*request) error
}

func (vlog *valueLog) logf(format string, args ...any) {
	if vlog == nil || !vlog.opt.ValueLogVerbose {
		return
	}
	fmt.Printf(format+"\n", args...)
}

// metrics captures backlog counters for the value log.
func (vlog *valueLog) metrics() metrics.ValueLogMetrics {
	if vlog == nil || vlog.manager == nil {
		return metrics.ValueLogMetrics{}
	}
	stats := metrics.ValueLogMetrics{
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
		metrics.IncValueLogSegmentsRemoved()
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
	metrics.IncValueLogHeadUpdates()
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
		return utils.WarpErr(fmt.Sprintf("Truncation needed at offset %d. Can be done manually as well.", endOffset), err)
	}
	return nil
}
