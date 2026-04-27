package lsm

import (
	"errors"
	"fmt"
	storagepb "github.com/feichai0017/NoKV/pb/storage"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/tombstone"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/utils"
)

// doCompact selects tables from a level and merges them into the target level.
func (lm *levelManager) doCompact(id int, p Priority) (retErr error) {
	l := p.Level
	utils.CondPanicFunc(l >= lm.opt.MaxLevelNum, func() error { return errors.New("[doCompact] Sanity check. l >= lm.opt.MaxLevelNum") }) // Sanity check.
	t := p.Target
	if t.BaseLevel == 0 {
		t = lm.levelTargets()
	}
	// Build the concrete compaction plan.
	cd := compactDef{
		compactorId: id,
		plan: Plan{
			ThisLevel:    l,
			ThisFileSize: lm.targetFileSizeForLevel(t, l),
			StagingMode:  p.StagingMode,
			DropPrefixes: p.DropPrefixes,
			StatsTag:     p.StatsTag,
		},
		thisLevel: lm.levels[l],
		adjusted:  p.Adjusted,
	}

	var cleanup bool
	defer func() {
		if cleanup {
			if err := lm.compactState.Delete(cd.stateEntry()); err != nil {
				lm.getLogger().Warn("failed to cleanup compaction state", "worker", id, "err", err)
				retErr = errors.Join(retErr, err)
			}
		}
	}()

	if p.StagingMode.UsesStaging() && l > 0 {
		cd.setNextLevel(lm, t, cd.thisLevel)
		order := cd.thisLevel.staging.shardOrderBySize()
		if len(order) == 0 {
			return ErrFillTables
		}
		baseLimit := lm.opt.StagingShardParallelism
		if baseLimit <= 0 {
			baseLimit = max(lm.opt.NumCompactors/2, 1)
		}
		if baseLimit > len(order) {
			baseLimit = len(order)
		}
		// Adaptive bump: more backlog => allow more shards, capped by shard count.
		shardLimit := baseLimit
		if p.Score > 1.0 {
			shardLimit += int(math.Ceil(p.Score / 2))
			if shardLimit > len(order) {
				shardLimit = len(order)
			}
		}
		var ran bool
		for i := 0; i < shardLimit; i++ {
			sub := cd
			if !lm.fillTablesStagingShard(&sub, order[i]) {
				continue
			}
			sub.plan.StagingMode = p.StagingMode
			sub.plan.StatsTag = p.StatsTag
			if err := lm.runCompactDef(id, l, sub); err != nil {
				lm.getLogger().Error("staging compaction failed", "worker", id, "err", err, "def", sub)
				if stateDelErr := lm.compactState.Delete(sub.stateEntry()); stateDelErr != nil {
					return errors.Join(err, stateDelErr)
				}
				return err
			}
			if err := lm.compactState.Delete(sub.stateEntry()); err != nil {
				return err
			}
			ran = true
			lm.getLogger().Info("staging compaction complete", "worker", id, "level", sub.thisLevel.levelNum, "shard", order[i])
		}
		if !ran {
			return ErrFillTables
		}
		return nil
	}

	// L0 uses a dedicated selection path.
	if l == 0 {
		cd.setNextLevel(lm, t, lm.levels[t.BaseLevel])
		if !lm.fillTablesL0(&cd) {
			return ErrFillTables
		}
		cleanup = true
		if cd.nextLevel.levelNum != 0 {
			if err := lm.moveToStaging(&cd); err != nil {
				lm.getLogger().Error("move to staging failed", "worker", id, "err", err, "def", cd)
				return err
			}
			lm.getLogger().Info("moved L0 tables to staging buffer", "worker", id, "tables", len(cd.top), "target_level", cd.nextLevel.levelNum)
			return nil
		}
	} else {
		cd.setNextLevel(lm, t, cd.thisLevel)
		// For non-last levels, compact into the next level.
		if !cd.thisLevel.isLastLevel() {
			cd.setNextLevel(lm, t, lm.levels[l+1])
		}
		if !lm.fillTables(&cd) {
			return ErrFillTables
		}
		cleanup = true
		if lm.canMoveToNextLevel(&cd) {
			if err := lm.moveToNextLevel(&cd); err != nil {
				lm.getLogger().Error("trivial move failed", "worker", id, "err", err, "def", cd)
				return err
			}
			lm.getLogger().Info("trivial move complete", "worker", id, "from_level", cd.thisLevel.levelNum, "to_level", cd.nextLevel.levelNum, "tables", len(cd.top))
			return nil
		}
		// Continue with the normal merge path.
		if err := lm.runCompactDef(id, l, cd); err != nil {
			lm.getLogger().Error("compaction failed", "worker", id, "err", err, "def", cd)
			return err
		}
		lm.getLogger().Info("compaction complete", "worker", id, "level", cd.thisLevel.levelNum)
		return nil
	}

	// Execute the merge plan.
	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		lm.getLogger().Error("compaction failed", "worker", id, "err", err, "def", cd)
		return err
	}
	lm.getLogger().Info("compaction complete", "worker", id, "level", cd.thisLevel.levelNum)
	return nil
}

func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
	if cd.plan.NextFileSize <= 0 {
		return errors.New("Next file size cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	utils.CondPanicFunc(len(cd.splits) != 0, func() error { return errors.New("len(cd.splits) != 0") })
	if thisLevel == nextLevel {
		// No special handling for L0->L0 and Lmax->Lmax.
	} else {
		lm.addSplits(&cd)
	}
	// Append an empty range placeholder when no split is found.
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, KeyRange{})
	}

	newTables, decr, err := lm.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	cleanupNeeded := true
	defer func() {
		if !cleanupNeeded {
			return
		}
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables)

	// Update the manifest.
	var manifestEdits []manifest.Edit
	levelByID := make(map[uint64]int, len(cd.top)+len(cd.bot))
	for _, t := range cd.top {
		levelByID[t.fid] = cd.thisLevel.levelNum
	}
	for _, t := range cd.bot {
		levelByID[t.fid] = cd.nextLevel.levelNum
	}
	for _, ch := range changeSet.Changes {
		switch ch.Op {
		case storagepb.ManifestChange_CREATE:
			tbl := findTableByID(newTables, ch.Id)
			if tbl == nil {
				continue
			}
			add := manifest.Edit{
				Type: manifest.EditAddFile,
				File: &manifest.FileMeta{
					Level:     int(ch.Level),
					FileID:    tbl.fid,
					Size:      uint64(tbl.Size()),
					Smallest:  kv.SafeCopy(nil, tbl.MinKey()),
					Largest:   kv.SafeCopy(nil, tbl.MaxKey()),
					CreatedAt: uint64(time.Now().Unix()),
					ValueSize: tbl.ValueSize(),
					Staging:   cd.plan.StagingMode == StagingKeep,
				},
			}
			manifestEdits = append(manifestEdits, add)
		case storagepb.ManifestChange_DELETE:
			level := levelByID[ch.Id]
			del := manifest.Edit{
				Type: manifest.EditDeleteFile,
				File: &manifest.FileMeta{FileID: ch.Id, Level: level},
			}
			manifestEdits = append(manifestEdits, del)
		}
	}
	if err := lm.manifestMgr.LogEdits(manifestEdits...); err != nil {
		return err
	}
	cleanupNeeded = false

	if cd.plan.StagingMode == StagingKeep {
		if err := thisLevel.replaceStagingTables(cd.top, newTables); err != nil {
			return err
		}
		if thisLevel.levelNum > 0 {
			thisLevel.Sort()
		}
	} else {
		if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
			return err
		}
		switch cd.plan.StagingMode {
		case StagingDrain:
			if err := thisLevel.deleteStagingTables(cd.top); err != nil {
				return err
			}
		default:
			// StagingNone (and unknown modes) own top tables in the main level list.
			if err := thisLevel.deleteTables(cd.top); err != nil {
				return err
			}
		}
	}

	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		lm.getLogger().Info(
			"compaction detail",
			"worker", id,
			"expensive", dur > time.Second,
			"from_level", thisLevel.levelNum,
			"to_level", nextLevel.levelNum,
			"top_tables", len(cd.top),
			"bottom_tables", len(cd.bot),
			"new_tables", len(newTables),
			"splits", len(cd.splits),
			"from", strings.Join(from, " "),
			"to", strings.Join(to, " "),
			"duration", dur.Round(time.Millisecond).String(),
		)
	}
	// Record staging metrics if applicable.
	if cd.plan.StagingMode.UsesStaging() {
		tablesCompacted := len(cd.top) + len(cd.bot)
		cd.thisLevel.recordStagingMetrics(cd.plan.StagingMode == StagingKeep, time.Since(timeStart), tablesCompacted)
	}
	lm.recordCompactionMetrics(time.Since(timeStart))
	// After max-level compaction, range tombstone layout may change.
	// Rebuild the in-memory range tombstone index to keep read visibility correct.
	if cd.nextLevel != nil && cd.nextLevel.levelNum == lm.opt.MaxLevelNum-1 && lm.rtCollector != nil {
		lm.rebuildRangeTombstones()
	}
	return nil
}

func (lm *levelManager) canMoveToNextLevel(cd *compactDef) bool {
	if cd == nil || cd.thisLevel == nil || cd.nextLevel == nil {
		return false
	}
	if cd.plan.StagingMode != StagingNone {
		return false
	}
	if cd.thisLevel == cd.nextLevel {
		return false
	}
	if len(cd.top) == 0 || len(cd.bot) != 0 {
		return false
	}
	if cd.thisLevel.levelNum == 0 {
		// L0 trivial move is only safe when the chosen group has no overlap
		// with any other L0 table. Otherwise promoting it would leave older
		// L0 tables masking newer keys at the destination level.
		if !l0GroupHasNoOtherOverlap(cd.top, cd.thisLevel.tables) {
			return false
		}
	}
	return true
}

func (lm *levelManager) moveToNextLevel(cd *compactDef) error {
	if !lm.canMoveToNextLevel(cd) {
		return errors.New("invalid compaction definition for trivial move")
	}
	var edits []manifest.Edit
	for _, tbl := range cd.top {
		if tbl == nil {
			continue
		}
		edits = append(edits, manifest.Edit{
			Type: manifest.EditDeleteFile,
			File: &manifest.FileMeta{FileID: tbl.fid, Level: cd.thisLevel.levelNum},
		})
		add := manifest.Edit{
			Type: manifest.EditAddFile,
			File: &manifest.FileMeta{
				Level:     cd.nextLevel.levelNum,
				FileID:    tbl.fid,
				Size:      uint64(tbl.Size()),
				Smallest:  kv.SafeCopy(nil, tbl.MinKey()),
				Largest:   kv.SafeCopy(nil, tbl.MaxKey()),
				ValueSize: tbl.ValueSize(),
			},
		}
		if created := tbl.GetCreatedAt(); created != nil {
			add.File.CreatedAt = uint64(created.Unix())
		}
		edits = append(edits, add)
	}
	if len(edits) == 0 {
		return nil
	}
	if err := lm.manifestMgr.LogEdits(edits...); err != nil {
		return err
	}

	toMove := make(map[uint64]*table, len(cd.top))
	for _, tbl := range cd.top {
		if tbl != nil {
			toMove[tbl.fid] = tbl
		}
	}

	first, second := cd.thisLevel, cd.nextLevel
	if first.levelNum > second.levelNum {
		first, second = second, first
	}
	first.Lock()
	second.Lock()

	remaining := cd.thisLevel.tables[:0]
	for _, tbl := range cd.thisLevel.tables {
		if _, found := toMove[tbl.fid]; found {
			cd.thisLevel.subtractSize(tbl)
			continue
		}
		remaining = append(remaining, tbl)
	}
	cd.thisLevel.tables = remaining
	cd.thisLevel.rebuildRangeFilterLocked()

	for _, tbl := range cd.top {
		if tbl == nil {
			continue
		}
		tbl.setLevel(cd.nextLevel.levelNum)
		cd.nextLevel.addSize(tbl)
		cd.nextLevel.tables = append(cd.nextLevel.tables, tbl)
	}
	cd.nextLevel.sortTablesLocked()
	cd.nextLevel.rebuildRangeFilterLocked()

	second.Unlock()
	first.Unlock()
	return nil
}

// tablesToString
func tablesToString(tables []*table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, ".")
	return res
}

// buildChangeSet _
func buildChangeSet(cd *compactDef, newTables []*table) storagepb.ManifestChangeSet {
	changes := []*storagepb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes, newCreateChange(table.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.fid))
	}
	return storagepb.ManifestChangeSet{Changes: changes}
}

func newDeleteChange(id uint64) *storagepb.ManifestChange {
	return &storagepb.ManifestChange{
		Id: id,
		Op: storagepb.ManifestChange_DELETE,
	}
}

// newCreateChange
func newCreateChange(id uint64, level int) *storagepb.ManifestChange {
	return &storagepb.ManifestChange{
		Id:    id,
		Op:    storagepb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

// compactBuildTables merges SSTables from two levels.
func (lm *levelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {

	topTables := append([]*table(nil), cd.top...)
	botTables := append([]*table(nil), cd.bot...)
	// Ensure concat/merge inputs are in ascending key-range order.
	// Some planning paths may preserve selection order but not strict key order.
	if len(topTables) > 1 {
		sort.Slice(topTables, func(i, j int) bool {
			return kv.CompareInternalKeys(topTables[i].MinKey(), topTables[j].MinKey()) < 0
		})
	}
	if len(botTables) > 1 {
		sort.Slice(botTables, func(i, j int) bool {
			return kv.CompareInternalKeys(botTables[i].MinKey(), botTables[j].MinKey()) < 0
		})
	}
	iterOpt := &index.Options{
		IsAsc:          true,
		AccessPattern:  utils.AccessPatternSequential,
		PrefetchBlocks: 1,
	}
	botCanConcat := tablesStrictlyOrdered(botTables)
	//numTables := int64(len(topTables) + len(botTables))
	newIterator := func() []index.Iterator {
		// Create iterators across all the tables involved first.
		var iters []index.Iterator
		switch {
		case lev == 0:
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		case len(topTables) > 0:
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		}
		if len(botTables) == 0 {
			return iters
		}
		if botCanConcat {
			return append(iters, NewConcatIterator(botTables, iterOpt))
		}
		// Fallback for overlapping/out-of-order next-level windows.
		// ConcatIterator assumes strict non-overlap; merge keeps global key ordering.
		return append(iters, iteratorsReversed(botTables, iterOpt)...)
	}

	// Start parallel compaction tasks.
	res := make(chan *table, 3)
	// Throttle inflight builders to bound memory and file handles.
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		if err := inflightBuilders.Go(func() error {
			it := NewMergeIterator(newIterator(), false)
			defer func() { _ = it.Close() }()
			lm.subcompact(it, kr, cd, inflightBuilders, res)
			return nil
		}); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
	}

	// Collect table handles via fan-in.
	var newTables []*table
	var wg sync.WaitGroup
	wg.Go(func() {
		for t := range res {
			newTables = append(newTables, t)
		}
	})

	// Wait for all compaction tasks to finish.
	err := inflightBuilders.Finish()
	// Release channel resources.
	close(res)
	// Wait for all builders to flush to disk.
	wg.Wait()

	if err == nil && lm.opt.ManifestSync {
		// Strict durability mode: persist new SST directory entries before manifest edits.
		err = vfs.SyncDir(lm.opt.FS, lm.opt.WorkDir)
	}

	if err != nil {
		// On error, delete newly created files.
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return kv.CompareInternalKeys(newTables[i].MaxKey(), newTables[j].MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

func iteratorsReversed(th []*table, opt *index.Options) []index.Iterator {
	out := make([]index.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}

// tablesStrictlyOrdered reports whether consecutive tables are in strictly
// increasing, non-overlapping user-key order.
func tablesStrictlyOrdered(tables []*table) bool {
	if len(tables) <= 1 {
		return true
	}
	prev := tables[0]
	if prev == nil {
		return false
	}
	for i := 1; i < len(tables); i++ {
		cur := tables[i]
		if cur == nil {
			return false
		}
		// Non-overlap requires prev.max base key < cur.min base key.
		if kv.CompareBaseKeys(prev.MaxKey(), cur.MinKey()) >= 0 {
			return false
		}
		prev = cur
	}
	return true
}

func (lm *levelManager) updateDiscardStats(discardStats map[manifest.ValueLogID]int64) {
	if lm == nil || lm.lsm == nil {
		return
	}
	ch := lm.lsm.getDiscardStatsCh()
	if ch == nil {
		return
	}
	select {
	case ch <- discardStats:
	default:
	}
}

// subcompact runs a single parallel compaction over a key range.
func (lm *levelManager) subcompact(it index.Iterator, kr KeyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *table) {
	var lastKey []byte
	// Track discardStats for value log GC.
	discardStats := make(map[manifest.ValueLogID]int64)
	valueBias := 1.0
	if cd.thisLevel != nil {
		valueBias = cd.thisLevel.valueBias(lm.opt.CompactionValueWeight)
	}
	defer func() {
		lm.updateDiscardStats(discardStats)
	}()
	updateStats := func(e *kv.Entry) {
		if e.Meta&kv.BitValuePointer > 0 {
			var vp kv.ValuePtr
			vp.Decode(e.Value)
			weighted := float64(vp.Len) * valueBias
			if weighted < 1 {
				weighted = float64(vp.Len)
			}
			discardStats[manifest.ValueLogID{Bucket: vp.Bucket, FileID: vp.Fid}] += int64(math.Round(weighted))
		}
	}

	// Keep tombstone state across builder splits.
	rtTracker := tombstone.NewCompactionTracker()

	addKeys := func(builder *tableBuilder) {
		var tableKr KeyRange

		for ; it.Valid(); it.Next() {
			entry := it.Item().Entry()
			key := entry.Key
			isExpired := entry.IsDeletedOrExpired()

			if entry.IsRangeDelete() {
				// Preserve range tombstones even at max level. Dropping them during
				// partial Lmax->Lmax rewrites can resurrect older covered keys that
				// remain in untouched tables outside this compaction unit.
				// Copy range tombstone data to avoid iterator reuse issues.
				cf, rtStart, rtVersion, ok := kv.SplitInternalKey(entry.Key)
				if !ok {
					continue
				}
				rt := tombstone.Range{
					CF:      cf,
					Start:   kv.SafeCopy(nil, rtStart),
					End:     kv.SafeCopy(nil, entry.RangeEnd()),
					Version: rtVersion,
				}
				rtTracker.Add(rt)
			}
			if !kv.SameBaseKey(key, lastKey) {
				if len(kr.Right) > 0 && kv.CompareInternalKeys(key, kr.Right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					break
				}
				lastKey = kv.SafeCopy(lastKey, key)
				if len(tableKr.Left) == 0 {
					tableKr.Left = kv.SafeCopy(tableKr.Left, key)
				}
				tableKr.Right = lastKey
			}

			if !entry.IsRangeDelete() {
				cf, userKey, version, ok := kv.SplitInternalKey(key)
				if !ok {
					continue
				}
				if rtTracker.Covers(cf, userKey, version) {
					// Covered point versions become stale once a newer range
					// tombstone is active at this key.
					updateStats(entry)
					continue
				}
			}

			valueLen := entryValueLen(entry)
			if isExpired {
				updateStats(entry)
				builder.AddStaleEntryWithLen(entry, valueLen)
			} else {
				builder.AddKeyWithLen(entry, valueLen)
			}
		}
	}

	// If the left bound remains, seek there to resume a partial scan.
	if len(kr.Left) > 0 {
		it.Seek(kr.Left)
	} else {
		//
		it.Rewind()
	}
	for it.Valid() {
		key := it.Item().Entry().Key
		if len(kr.Right) > 0 && kv.CompareInternalKeys(key, kr.Right) >= 0 {
			break
		}
		// Copy Options so background tuning does not affect the active compaction.
		builderOpt := lm.opt.Clone()
		builder := newTableBuilerWithSSTSize(builderOpt, cd.plan.NextFileSize)
		builder.pacer = lm.compactionPacerForBuild()

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.empty() {
			// Cleanup builder resources:
			_, _ = builder.finish()
			builder.Close()
			continue
		}
		// Leverage SSD parallel write throughput.
		b := builder
		if err := inflightBuilders.Go(func() error {
			defer b.Close()
			newFID := lm.maxFID.Add(1) // Compaction does not allocate memtables; advance maxFID.
			sstName := vfs.FileNameSSTable(lm.opt.WorkDir, newFID)
			tbl, err := openTable(lm, sstName, b)
			if err != nil || tbl == nil {
				slog.Default().Error("open compacted table", "path", sstName, "error", err)
				return nil
			}
			res <- tbl
			return nil
		}); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
	}
}
