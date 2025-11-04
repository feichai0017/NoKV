package NoKV

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"expvar"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/feichai0017/NoKV/wal"
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
		lf, ok := vlog.manager.LogFile(fid)
		if !ok {
			return errors.Errorf("valueLog.open: missing log file %d", fid)
		}
		offset := uint32(0)
		if fid == ptr.Fid {
			offset = ptr.Offset + ptr.Len
		}
		fmt.Printf("Replaying file id: %d at offset: %d\n", fid, offset)
		start := time.Now()
		if err := vlog.replayLog(lf, offset, replayFn, fid == activeFID); err != nil {
			if err == utils.ErrDeleteVlogFile {
				if removeErr := vlog.removeValueLogFile(fid); removeErr != nil {
					return removeErr
				}
				continue
			}
			return err
		}
		fmt.Printf("Replay took: %s\n", time.Since(start))

		if fid != activeFID {
			if err := lf.Init(); err != nil {
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
			fmt.Printf("Failed to populate discard stats: %s\n", err)
		}
	}
	return nil
}

func (vlog *valueLog) read(vp *kv.ValuePtr) ([]byte, func(), error) {
	data, unlock, err := vlog.manager.Read(vp)
	if err != nil {
		return nil, unlock, err
	}
	val, _, err := wal.DecodeValueSlice(data)
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
		for _, lf := range files {
			if _, done := tried[lf.FID]; done {
				continue
			}
			tried[lf.FID] = true
			if err := vlog.doRunGC(lf, discardRatio); err == nil {
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

func (vlog *valueLog) doRunGC(lf *file.LogFile, discardRatio float64) (err error) {
	defer func() {
		if err == nil {
			vlog.lfDiscardStats.Lock()
			delete(vlog.lfDiscardStats.m, lf.FID)
			vlog.lfDiscardStats.Unlock()
		}
	}()

	s := &sampler{
		lf:            lf,
		countRatio:    0.01,
		sizeRatio:     0.1,
		fromBeginning: false,
	}

	if _, err = vlog.sample(s, discardRatio); err != nil {
		return err
	}

	if err = vlog.rewrite(lf); err != nil {
		return err
	}
	valueLogGCRuns.Add(1)
	return nil
}

func decodeWalEntry(data []byte) (*kv.Entry, int, int, error) {
	if len(data) == 0 {
		return nil, 0, 0, io.EOF
	}

	readVarint := func(b []byte) (uint64, int, error) {
		val, n := binary.Uvarint(b)
		if n <= 0 {
			if n == 0 {
				return 0, 0, io.ErrUnexpectedEOF
			}
			return 0, 0, io.ErrUnexpectedEOF
		}
		return val, n, nil
	}

	// Binary Format (same as WAL entry format):
	// +----------------+----------------+------+-------+----------+
	// | Key Length (v) | Val Length (v) | Meta | ExpAt | Key      |
	// +----------------+----------------+------+-------+----------+
	// | Value          | Checksum (4B)  |
	// +----------------+----------------+
	// (v) denotes Uvarint encoding.

	idx := 0
	keyLenU, n, err := readVarint(data[idx:])
	if err != nil {
		return nil, 0, 0, err
	}
	idx += n

	valLenU, n, err := readVarint(data[idx:])
	if err != nil {
		return nil, 0, 0, err
	}
	idx += n

	metaU, n, err := readVarint(data[idx:])
	if err != nil {
		return nil, 0, 0, err
	}
	idx += n

	expiresAt, n, err := readVarint(data[idx:])
	if err != nil {
		return nil, 0, 0, err
	}
	idx += n

	headerLen := idx
	keyLen := int(keyLenU)
	valLen := int(valLenU)
	total := headerLen + keyLen + valLen + crc32.Size
	if len(data) < total {
		return nil, 0, 0, io.ErrUnexpectedEOF
	}

	keyStart := idx
	idx += keyLen

	valStart := idx
	idx += valLen

	hash := crc32.New(kv.CastagnoliCrcTable)
	if _, err := hash.Write(data[:idx]); err != nil {
		return nil, 0, 0, err
	}
	checksum := binary.BigEndian.Uint32(data[idx : idx+crc32.Size])
	if checksum != hash.Sum32() {
		return nil, 0, 0, utils.ErrTruncate
	}

	entry := kv.EntryPool.Get().(*kv.Entry)
	entry.IncrRef()
	entry.Key = append(entry.Key[:0], data[keyStart:keyStart+keyLen]...)
	entry.Value = append(entry.Value[:0], data[valStart:valStart+valLen]...)
	entry.Meta = byte(metaU)
	entry.ExpiresAt = expiresAt
	entry.Version = 0
	entry.Offset = 0
	entry.Hlen = 0
	entry.ValThreshold = 0
	return entry, headerLen, total, nil
}

func (vlog *valueLog) rewrite(f *file.LogFile) error {
	activeFID := vlog.manager.ActiveFID()
	utils.CondPanic(f.FID >= activeFID, fmt.Errorf("fid to move: %d. Current active fid: %d", f.FID, activeFID))

	wb := make([]*kv.Entry, 0, 1000)
	var size int64

	fe := func(e *kv.Entry) error {
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

		var vp kv.ValuePtr
		vp.Decode(entry.Value)

		if vp.Fid > f.FID || (vp.Fid == f.FID && vp.Offset > e.Offset) {
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

	sizeBytes := int(f.Size())
	if sizeBytes == 0 {
		return nil
	}
	buf := make([]byte, sizeBytes)
	if _, err := f.FD().ReadAt(buf, 0); err != nil && err != io.EOF {
		return err
	}

	for offset := uint32(0); offset < uint32(len(buf)); {
		entry, headerLen, recordLen, err := decodeWalEntry(buf[offset:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		entry.Offset = offset
		entry.Hlen = headerLen
		feErr := fe(entry)
		entry.DecrRef()
		if feErr != nil {
			return feErr
		}
		offset += uint32(recordLen)
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
		vlog.filesToBeDeleted = append(vlog.filesToBeDeleted, f.FID)
	}
	vlog.filesToDeleteLock.Unlock()

	if deleteNow {
		if err := vlog.removeValueLogFile(f.FID); err != nil {
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

func (vlog *valueLog) pickLog(head *kv.ValuePtr) (files []*file.LogFile) {
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
		if lf, ok := vlog.manager.LogFile(candidate.fid); ok {
			files = append(files, lf)
		}
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
	if lf, ok := vlog.manager.LogFile(fids[idx]); ok {
		files = append(files, lf)
	}
	return files
}

type sampler struct {
	lf            *file.LogFile
	sizeRatio     float64
	countRatio    float64
	fromBeginning bool
}

func (vlog *valueLog) sample(samp *sampler, discardRatio float64) (*reason, error) {
	sizePercent := samp.sizeRatio
	countPercent := samp.countRatio
	fileSize := samp.lf.Size()
	sizeWindow := float64(fileSize) * sizePercent
	sizeWindowM := sizeWindow / (1 << 20)
	countWindow := int(float64(vlog.opt.ValueLogMaxEntries) * countPercent)

	var skipFirstM float64
	if !samp.fromBeginning {
		skipFirstM = float64(rand.Int63n(fileSize))
		skipFirstM -= sizeWindow
		skipFirstM /= float64(utils.Mi)
	}
	var skipped float64

	var r reason
	start := time.Now()
	_, err := vlog.iterate(samp.lf, 0, func(e *kv.Entry, vp *kv.ValuePtr) error {
		esz := float64(vp.Len) / (1 << 20)
		if skipped < skipFirstM {
			skipped += esz
			return nil
		}
		if r.count > countWindow || r.total > sizeWindowM || time.Since(start) > 10*time.Second {
			return utils.ErrStop
		}
		r.total += esz
		r.count++

		cf, userKey, _ := kv.SplitInternalKey(e.Key)
		entry, err := vlog.db.GetCF(cf, userKey)
		if err != nil {
			return err
		}
		if kv.DiscardEntry(e, entry) {
			r.discard += esz
			return nil
		}

		if len(entry.Value) == 0 {
			return nil
		}
		var newVP kv.ValuePtr
		newVP.Decode(entry.Value)

		if newVP.Fid > samp.lf.FID || (newVP.Fid == samp.lf.FID && newVP.Offset > e.Offset) {
			r.discard += esz
			return nil
		}
		return nil
	})
	if err != nil && err != utils.ErrStop {
		return nil, err
	}

	fmt.Printf("Fid: %d. Skipped: %5.2fMB Data status=%+v\n", samp.lf.FID, skipped, r)
	if (r.count < countWindow && r.total < sizeWindowM*0.75) || r.discard < discardRatio*r.total {
		return nil, utils.ErrNoRewrite
	}
	return &r, nil
}

func (vlog *valueLog) replayLog(lf *file.LogFile, offset uint32, replayFn kv.LogEntry, isActive bool) error {
	endOffset, err := vlog.iterate(lf, offset, replayFn)
	if err != nil {
		return errors.Wrapf(err, "Unable to replay logfile:[%s]", lf.FileName())
	}
	if int64(endOffset) == lf.Size() {
		return nil
	}

	if endOffset <= utils.VlogHeaderSize {
		if !isActive {
			return utils.ErrDeleteVlogFile
		}
		return lf.Bootstrap()
	}

	fmt.Printf("Truncating vlog file %s to offset: %d\n", lf.FileName(), endOffset)
	if err := lf.Truncate(int64(endOffset)); err != nil {
		return utils.WarpErr(
			fmt.Sprintf("Truncation needed at offset %d. Can be done manually as well.", endOffset), err)
	}
	return nil
}

func (vlog *valueLog) iterate(lf *file.LogFile, offset uint32, fn kv.LogEntry) (uint32, error) {
	if offset == 0 {
		offset = utils.VlogHeaderSize
	}
	if int64(offset) == lf.Size() {
		return offset, nil
	}

	if _, err := lf.Seek(int64(offset), io.SeekStart); err != nil {
		return 0, errors.Wrapf(err, "Unable to seek, name:%s", lf.FileName())
	}

	reader := bufio.NewReader(lf.FD())
	read := &safeRead{
		k:            make([]byte, 10),
		v:            make([]byte, 10),
		recordOffset: offset,
		lf:           lf,
	}

	validEndOffset := offset

	for {
		e, err := read.Entry(reader)
		switch {
		case err == io.EOF:
			return validEndOffset, nil
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			return validEndOffset, nil
		case err != nil:
			return 0, err
		case e == nil:
			continue
		}

		var vp kv.ValuePtr
		vp.Len = uint32(int(e.Hlen) + len(e.Key) + len(e.Value) + crc32.Size)
		vp.Offset = e.Offset
		vp.Fid = lf.FID
		validEndOffset = read.recordOffset
		callErr := fn(e, &vp)
		e.DecrRef()
		if callErr != nil {
			if callErr == utils.ErrStop {
				return validEndOffset, nil
			}
			return 0, utils.WarpErr(fmt.Sprintf("Iteration function %s", lf.FileName()), callErr)
		}
	}
}

type safeRead struct {
	k            []byte
	v            []byte
	recordOffset uint32
	lf           *file.LogFile
}

func (r *safeRead) Entry(reader io.Reader) (*kv.Entry, error) {
	tee := kv.NewHashReader(reader)
	var headerBytes int
	readVarint := func() (uint64, error) {
		val, err := binary.ReadUvarint(tee)
		headerBytes = tee.BytesRead
		return val, err
	}

	klen, err := readVarint()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}
	vlen, err := readVarint()
	if err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	meta, err := readVarint()
	if err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	expiresAt, err := readVarint()
	if err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}

	if klen > uint64(1<<32) || vlen > uint64(1<<32) {
		return nil, utils.ErrTruncate
	}

	keyLen := int(klen)
	entry := kv.EntryPool.Get().(*kv.Entry)
	entry.IncrRef()
	entry.Version = 0
	entry.Hlen = headerBytes
	entry.Offset = r.recordOffset
	entry.ValThreshold = 0
	entry.Meta = byte(meta)
	entry.ExpiresAt = expiresAt

	if cap(entry.Key) < keyLen {
		entry.Key = make([]byte, keyLen)
	} else {
		entry.Key = entry.Key[:keyLen]
	}
	if _, err := io.ReadFull(tee, entry.Key); err != nil {
		entry.DecrRef()
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	valLen := int(vlen)
	if cap(entry.Value) < valLen {
		entry.Value = make([]byte, valLen)
	} else {
		entry.Value = entry.Value[:valLen]
	}
	if _, err := io.ReadFull(tee, entry.Value); err != nil {
		entry.DecrRef()
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}

	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := kv.BytesToU32(crcBuf[:])
	if crc != tee.Sum32() {
		entry.DecrRef()
		return nil, utils.ErrTruncate
	}

	recordLen := uint32(headerBytes) + uint32(len(entry.Key)) + uint32(len(entry.Value)) + crc32.Size
	r.recordOffset += recordLen
	return entry, nil
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
	fmt.Printf("Value Log Discard stats: %v\n", statsMap)
	vlog.lfDiscardStats.flushChan <- statsMap
	return nil
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

	mergeStats := func(stats map[uint32]int64) ([]byte, error) {
		vlog.lfDiscardStats.Lock()
		defer vlog.lfDiscardStats.Unlock()
		for fid, count := range stats {
			vlog.lfDiscardStats.m[fid] += count
			vlog.lfDiscardStats.updatesSinceFlush++
		}

		threshold := vlog.lfDiscardStats.flushThreshold
		if threshold <= 0 {
			threshold = defaultDiscardStatsFlushThreshold
		}
		if vlog.lfDiscardStats.updatesSinceFlush >= threshold {
			encodedDS, err := json.Marshal(vlog.lfDiscardStats.m)
			if err != nil {
				return nil, err
			}
			vlog.lfDiscardStats.updatesSinceFlush = 0
			return encodedDS, nil
		}
		return nil, nil
	}

	process := func(stats map[uint32]int64) error {
		encodedDS, err := mergeStats(stats)
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
			return
		case stats := <-vlog.lfDiscardStats.flushChan:
			if err := process(stats); err != nil {
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

type reason struct {
	total   float64
	discard float64
	count   int
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
