package NoKV

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/pkg/errors"
)

type lfDiscardStats struct {
	sync.RWMutex
	m                 map[manifest.ValueLogID]int64
	flushChan         chan map[manifest.ValueLogID]int64
	closer            *utils.Closer
	updatesSinceFlush int
	flushThreshold    int
}

func (vlog *valueLog) flushDiscardStats() {
	defer vlog.lfDiscardStats.closer.Done()

	mergeStats := func(stats map[manifest.ValueLogID]int64, force bool) ([]byte, error) {
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

		if !force && vlog.lfDiscardStats.updatesSinceFlush < threshold {
			return nil, nil
		}
		if vlog.lfDiscardStats.updatesSinceFlush == 0 {
			return nil, nil
		}

		encodedDS, err := encodeDiscardStats(vlog.lfDiscardStats.m)
		if err != nil {
			return nil, err
		}
		vlog.lfDiscardStats.updatesSinceFlush = 0
		return encodedDS, nil
	}

	process := func(stats map[manifest.ValueLogID]int64, force bool) error {
		encodedDS, err := mergeStats(stats, force)
		if err != nil || encodedDS == nil {
			return err
		}

		entry := kv.NewEntryWithCF(kv.CFDefault, kv.InternalKey(kv.CFDefault, lfDiscardStatsKey, 1), encodedDS)
		entries := []*kv.Entry{entry}
		req, err := vlog.db.sendToWriteCh(entries, false)
		if err != nil {
			entry.DecrRef()
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
						_ = utils.Err(fmt.Errorf("unable to process discardstats with error: %s", err))
					}
				default:
					goto drainComplete
				}
			}
		drainComplete:
			if err := process(nil, true); err != nil {
				_ = utils.Err(fmt.Errorf("unable to process discardstats with error: %s", err))
			}
			return
		case stats := <-vlog.lfDiscardStats.flushChan:
			if err := process(stats, false); err != nil {
				_ = utils.Err(fmt.Errorf("unable to process discardstats with error: %s", err))
			}
		}
	}
}

func (vlog *valueLog) runGC(discardRatio float64, heads map[uint32]kv.ValuePtr) error {
	select {
	case vlog.garbageCh <- struct{}{}:
		defer func() {
			<-vlog.garbageCh
		}()

		files := vlog.pickLog(heads)
		if len(files) == 0 {
			return utils.ErrNoRewrite
		}
		tried := make(map[manifest.ValueLogID]bool)
		for _, id := range files {
			if _, done := tried[id]; done {
				continue
			}
			tried[id] = true
			if err := vlog.doRunGC(id.Bucket, id.FileID, discardRatio); err == nil {
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

func (vlog *valueLog) doRunGC(bucket uint32, fid uint32, discardRatio float64) (err error) {
	defer func() {
		if err == nil {
			vlog.lfDiscardStats.Lock()
			delete(vlog.lfDiscardStats.m, manifest.ValueLogID{Bucket: bucket, FileID: fid})
			vlog.lfDiscardStats.Unlock()
		}
	}()

	mgr, err := vlog.managerFor(bucket)
	if err != nil {
		return err
	}
	opts := vlogpkg.SampleOptions{
		SizeRatio:     vlog.gcSampleSizeRatio(),
		CountRatio:    vlog.gcSampleCountRatio(),
		FromBeginning: vlog.opt.ValueLogGCSampleFromHead,
		MaxEntries:    vlog.opt.ValueLogMaxEntries,
	}
	start := time.Now()
	stats, err := mgr.Sample(fid, opts, func(e *kv.Entry, vp *kv.ValuePtr) (bool, error) {
		if time.Since(start) > 10*time.Second {
			return false, utils.ErrStop
		}
		if e == nil || len(e.Key) == 0 {
			return false, nil
		}
		cf, userKey, _ := kv.SplitInternalKey(e.Key)
		if len(userKey) == 0 {
			return false, nil
		}
		entry, err := vlog.db.GetCF(cf, userKey)
		if err != nil {
			if errors.Is(err, utils.ErrEmptyKey) {
				return false, nil
			}
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

		if newVP.Bucket != bucket {
			return true, nil
		}
		if newVP.Fid > fid || (newVP.Fid == fid && newVP.Offset > e.Offset) {
			return true, nil
		}
		return false, nil
	})
	if err != nil && err != utils.ErrStop {
		// Skip this round if writes are blocked/DB is closing; GC can retry later.
		if errors.Is(err, utils.ErrBlockedWrites) || errors.Is(err, utils.ErrDBClosed) {
			return utils.ErrNoRewrite
		}
		return err
	}
	if stats == nil {
		return utils.ErrNoRewrite
	}

	vlog.logf("Fid: %d bucket: %d. Skipped: %5.2fMB Data status={total:%5.2f discard:%5.2f count:%d}", fid, bucket, stats.SkippedMiB, stats.TotalMiB, stats.DiscardMiB, stats.Count)

	sizeWindow := stats.SizeWindow
	if sizeWindow == 0 {
		sizeWindow = float64(vlog.opt.ValueLogFileSize) / float64(utils.Mi)
	}
	if (stats.Count < stats.CountWindow && stats.TotalMiB < sizeWindow*0.75) || stats.DiscardMiB < discardRatio*stats.TotalMiB {
		return utils.ErrNoRewrite
	}

	if err = vlog.rewrite(bucket, fid); err != nil {
		return err
	}
	metrics.IncValueLogGCRuns()
	return nil
}

func (vlog *valueLog) rewrite(bucket uint32, fid uint32) error {
	mgr, err := vlog.managerFor(bucket)
	if err != nil {
		return err
	}
	activeFID := mgr.ActiveFID()
	utils.CondPanic(fid >= activeFID, fmt.Errorf("fid to move: %d. Current active fid: %d (bucket %d)", fid, activeFID, bucket))

	wb := make([]*kv.Entry, 0, 1000)
	var size int64

	process := func(e *kv.Entry, ptr *kv.ValuePtr) error {
		if e == nil || len(e.Key) == 0 {
			return nil
		}
		entry, err := vlog.db.lsm.Get(e.Key)
		if err != nil {
			// If LSM can't find it (e.g., concurrent compaction/move), fall back to the
			// value log copy so we don't drop a live key.
			if errors.Is(err, utils.ErrKeyNotFound) {
				entry = e
			} else if errors.Is(err, utils.ErrEmptyKey) {
				return nil
			} else {
				return err
			}
		}
		if kv.DiscardEntry(e, entry) {
			return nil
		}

		if len(entry.Value) == 0 {
			return errors.Errorf("empty value: %+v", entry)
		}

		var diskVP kv.ValuePtr
		diskVP.Decode(entry.Value)

		if diskVP.Bucket != bucket {
			return nil
		}
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

	if _, err := mgr.Iterate(fid, 0, func(e *kv.Entry, vp *kv.ValuePtr) error {
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
		vlog.filesToBeDeleted = append(vlog.filesToBeDeleted, manifest.ValueLogID{Bucket: bucket, FileID: fid})
	}
	vlog.filesToDeleteLock.Unlock()

	if deleteNow {
		if err := vlog.removeValueLogFile(bucket, fid); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) iteratorCount() int {
	return int(atomic.LoadInt32(&vlog.numActiveIterators))
}

func (vlog *valueLog) filterPendingDeletes(fids []manifest.ValueLogID) []manifest.ValueLogID {
	vlog.filesToDeleteLock.Lock()
	defer vlog.filesToDeleteLock.Unlock()

	if len(vlog.filesToBeDeleted) == 0 {
		out := make([]manifest.ValueLogID, len(fids))
		copy(out, fids)
		return out
	}

	toDelete := make(map[manifest.ValueLogID]struct{}, len(vlog.filesToBeDeleted))
	for _, id := range vlog.filesToBeDeleted {
		toDelete[id] = struct{}{}
	}

	out := make([]manifest.ValueLogID, 0, len(fids))
	for _, id := range fids {
		if _, ok := toDelete[id]; ok {
			continue
		}
		out = append(out, id)
	}
	return out
}

func (vlog *valueLog) pickLog(heads map[uint32]kv.ValuePtr) (files []manifest.ValueLogID) {
	if len(vlog.managers) == 0 {
		return nil
	}

	var (
		bestID      manifest.ValueLogID
		bestDiscard int64
	)

	vlog.lfDiscardStats.RLock()
	for id, discard := range vlog.lfDiscardStats.m {
		if int(id.Bucket) >= len(vlog.managers) {
			continue
		}
		mgr := vlog.managers[id.Bucket]
		if mgr == nil {
			continue
		}
		activeFID := mgr.ActiveFID()
		head := heads[id.Bucket]
		if id.FileID >= activeFID {
			continue
		}
		if head.Fid != 0 && id.FileID >= head.Fid {
			continue
		}
		if discard > bestDiscard {
			bestDiscard = discard
			bestID = id
		}
	}
	vlog.lfDiscardStats.RUnlock()

	if bestDiscard > 0 {
		files = append(files, bestID)
	}

	candidates := make([]manifest.ValueLogID, 0)
	for bucket, mgr := range vlog.managers {
		if mgr == nil {
			continue
		}
		head := heads[uint32(bucket)]
		activeFID := mgr.ActiveFID()
		for _, fid := range mgr.ListFIDs() {
			if fid >= activeFID {
				continue
			}
			if head.Fid != 0 && fid >= head.Fid {
				continue
			}
			candidates = append(candidates, manifest.ValueLogID{Bucket: uint32(bucket), FileID: fid})
		}
	}
	if len(candidates) == 0 {
		return files
	}
	candidates = vlog.filterPendingDeletes(candidates)
	if len(candidates) == 0 {
		return files
	}
	files = append(files, candidates[rand.Intn(len(candidates))])
	return files
}

func (vlog *valueLog) populateDiscardStats() error {
	var statsMap map[manifest.ValueLogID]int64
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
	statsMap, err = decodeDiscardStats(val)
	if err != nil {
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

func encodeDiscardStats(stats map[manifest.ValueLogID]int64) ([]byte, error) {
	wire := make(map[string]int64, len(stats))
	for id, count := range stats {
		key := fmt.Sprintf("%d:%d", id.Bucket, id.FileID)
		wire[key] = count
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
