package lsm

import (
	"errors"
	"fmt"
	"log"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm/compact"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
)

type compactDef struct {
	compactorId int
	plan        compact.Plan
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table
	bot []*table

	splits []compact.KeyRange

	thisSize int64

	adjusted float64
}

// Compaction flow: pick a Plan in compact, resolve table IDs here, then execute the merge.

func (cd *compactDef) targetFileSize() int64 {
	return cd.fileSize(cd.plan.ThisLevel)
}

func (cd *compactDef) fileSize(level int) int64 {
	switch level {
	case cd.plan.ThisLevel:
		return cd.plan.ThisFileSize
	case cd.plan.NextLevel:
		return cd.plan.NextFileSize
	default:
		return 0
	}
}

func (cd *compactDef) stateEntry() compact.StateEntry {
	return cd.plan.StateEntry(cd.thisSize)
}

func (cd *compactDef) setNextLevel(lm *levelManager, t compact.Targets, next *levelHandler) {
	cd.nextLevel = next
	if next == nil {
		return
	}
	cd.plan.NextLevel = next.levelNum
	cd.plan.NextFileSize = lm.targetFileSizeForLevel(t, next.levelNum)
}

func (cd *compactDef) applyPlan(plan compact.Plan) {
	plan.ThisFileSize = cd.plan.ThisFileSize
	plan.NextFileSize = cd.plan.NextFileSize
	plan.IngestMode = cd.plan.IngestMode
	plan.DropPrefixes = cd.plan.DropPrefixes
	plan.StatsTag = cd.plan.StatsTag
	cd.plan = plan
}

// resolvePlanLocked binds plan tables; caller must hold cd level locks.
func (lm *levelManager) resolvePlanLocked(cd *compactDef) bool {
	if cd == nil || cd.thisLevel == nil || cd.nextLevel == nil {
		return false
	}
	topFromIngest := cd.plan.IngestMode.UsesIngest()
	top := resolveTablesLocked(cd.thisLevel, cd.plan.TopIDs, topFromIngest)
	if len(cd.plan.TopIDs) != len(top) {
		return false
	}
	bot := resolveTablesLocked(cd.nextLevel, cd.plan.BotIDs, false)
	if len(cd.plan.BotIDs) != len(bot) {
		return false
	}
	cd.top = top
	cd.bot = bot
	cd.thisSize = 0
	for _, t := range cd.top {
		if t != nil {
			cd.thisSize += t.Size()
		}
	}
	return true
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

// doCompact selects tables from a level and merges them into the target level.
func (lm *levelManager) doCompact(id int, p compact.Priority) error {
	l := p.Level
	utils.CondPanic(l >= lm.opt.MaxLevelNum, errors.New("[doCompact] Sanity check. l >= lm.opt.MaxLevelNum")) // Sanity check.
	t := p.Target
	if t.BaseLevel == 0 {
		t = lm.levelTargets()
	}
	// Build the concrete compaction plan.
	cd := compactDef{
		compactorId: id,
		plan: compact.Plan{
			ThisLevel:    l,
			ThisFileSize: lm.targetFileSizeForLevel(t, l),
			IngestMode:   p.IngestMode,
			DropPrefixes: p.DropPrefixes,
			StatsTag:     p.StatsTag,
		},
		thisLevel: lm.levels[l],
		adjusted:  p.Adjusted,
	}

	var cleanup bool
	defer func() {
		if cleanup {
			lm.compactState.Delete(cd.stateEntry())
		}
	}()

	if p.IngestMode.UsesIngest() && l > 0 {
		cd.setNextLevel(lm, t, cd.thisLevel)
		order := cd.thisLevel.ingest.shardOrderBySize()
		if len(order) == 0 {
			return utils.ErrFillTables
		}
		baseLimit := lm.opt.IngestShardParallelism
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
			if !lm.fillTablesIngestShard(&sub, order[i]) {
				continue
			}
			sub.plan.IngestMode = p.IngestMode
			sub.plan.StatsTag = p.StatsTag
			if err := lm.runCompactDef(id, l, sub); err != nil {
				log.Printf("[Compactor: %d] LOG Ingest Compact FAILED with error: %+v: %+v", id, err, sub)
				lm.compactState.Delete(sub.stateEntry())
				return err
			}
			lm.compactState.Delete(sub.stateEntry())
			ran = true
			log.Printf("[Compactor: %d] Ingest compaction for level: %d shard=%d DONE", id, sub.thisLevel.levelNum, order[i])
		}
		if !ran {
			return utils.ErrFillTables
		}
		return nil
	}

	// L0 uses a dedicated selection path.
	if l == 0 {
		cd.setNextLevel(lm, t, lm.levels[t.BaseLevel])
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
		cleanup = true
		if cd.nextLevel.levelNum != 0 {
			if err := lm.moveToIngest(&cd); err != nil {
				log.Printf("[Compactor: %d] LOG Move to ingest FAILED with error: %+v: %+v", id, err, cd)
				return err
			}
			log.Printf("[Compactor: %d] Moved %d tables from L0 to ingest buffer of L%d", id, len(cd.top), cd.nextLevel.levelNum)
			return nil
		}
	} else {
		cd.setNextLevel(lm, t, cd.thisLevel)
		// For non-last levels, compact into the next level.
		if !cd.thisLevel.isLastLevel() {
			cd.setNextLevel(lm, t, lm.levels[l+1])
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
		cleanup = true
		// Continue with the normal merge path.
		if err := lm.runCompactDef(id, l, cd); err != nil {
			log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
			return err
		}
		log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
		return nil
	}

	// Execute the merge plan.
	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}
	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}

// AdjustThrottle enables or clears write throttling based on L0 table pressure.
func (lm *levelManager) AdjustThrottle() {
	if lm == nil || lm.lsm == nil || len(lm.levels) == 0 {
		return
	}
	limit := lm.opt.NumLevelZeroTables
	if limit <= 0 {
		limit = 4
	}
	l0Tables := lm.levels[0].numTables()
	highWatermark := limit * 2
	switch {
	case l0Tables >= highWatermark:
		lm.lsm.throttleWrites(true)
	case l0Tables <= limit:
		lm.lsm.throttleWrites(false)
	}
}

// NeedsCompaction reports whether any level currently exceeds compaction thresholds.
func (lm *levelManager) NeedsCompaction() bool {
	return len(lm.pickCompactLevels()) > 0
}

// PickCompactLevels returns current compaction candidates ordered by priority.
func (lm *levelManager) PickCompactLevels() []compact.Priority {
	return lm.pickCompactLevels()
}

// DoCompact executes one compaction job selected by the picker.
func (lm *levelManager) DoCompact(id int, p compact.Priority) error {
	return lm.doCompact(id, p)
}

// pickCompactLevels chooses compaction candidates and returns priorities.
func (lm *levelManager) pickCompactLevels() (prios []compact.Priority) {
	input := lm.buildPickerInput()
	if len(input.Levels) == 0 {
		return nil
	}
	return compact.PickPriorities(input)
}

func (lm *levelManager) buildPickerInput() compact.PickerInput {
	if lm == nil || lm.opt == nil {
		return compact.PickerInput{}
	}
	var hotKeys [][]byte
	if v := lm.hotProvider.Load(); v != nil {
		if fn, ok := v.(func() [][]byte); ok && fn != nil {
			hotKeys = fn()
		}
	}
	levels := make([]compact.LevelInput, len(lm.levels))
	for i, lvl := range lm.levels {
		if lvl == nil {
			continue
		}
		li := compact.LevelInput{
			Level:              i,
			NumTables:          lvl.numTables(),
			TotalSize:          lvl.getTotalSize(),
			TotalValueBytes:    lvl.getTotalValueSize(),
			MainValueBytes:     lvl.mainValueBytes(),
			IngestTables:       lvl.numIngestTables(),
			IngestSize:         lvl.ingestDataSize(),
			IngestValueBytes:   lvl.ingestValueBytes(),
			IngestValueDensity: lvl.ingestValueDensity(),
			IngestAgeSeconds:   lvl.maxIngestAgeSeconds(),
		}
		if lm.compactState != nil {
			li.DelSize = lm.compactState.DelSize(i)
		}
		if len(hotKeys) > 0 {
			li.HotOverlap = lvl.hotOverlapScore(hotKeys, false)
			li.HotOverlapIngest = lvl.hotOverlapScore(hotKeys, true)
		}
		levels[i] = li
	}
	return compact.PickerInput{
		Levels:                  levels,
		Targets:                 lm.levelTargets(),
		NumLevelZeroTables:      lm.opt.NumLevelZeroTables,
		BaseTableSize:           lm.opt.BaseTableSize,
		BaseLevelSize:           lm.opt.BaseLevelSize,
		IngestBacklogMergeScore: lm.opt.IngestBacklogMergeScore,
		CompactionValueWeight:   lm.opt.CompactionValueWeight,
	}
}

// levelTargets
func (lm *levelManager) levelTargets() compact.Targets {
	if lm == nil || lm.opt == nil || len(lm.levels) == 0 {
		return compact.Targets{}
	}
	return compact.BuildTargets(lm.levelSizes(), compact.TargetOptions{
		BaseLevelSize:       lm.opt.BaseLevelSize,
		LevelSizeMultiplier: lm.opt.LevelSizeMultiplier,
		BaseTableSize:       lm.opt.BaseTableSize,
		TableSizeMultiplier: lm.opt.TableSizeMultiplier,
		MemTableSize:        lm.opt.MemTableSize,
	})
}

func (lm *levelManager) targetFileSizeForLevel(t compact.Targets, level int) int64 {
	if level < 0 {
		return 0
	}
	if level < len(t.FileSz) && t.FileSz[level] > 0 {
		return t.FileSz[level]
	}
	if level < len(t.TargetSz) && t.TargetSz[level] > 0 {
		return t.TargetSz[level]
	}
	return 0
}

func (lm *levelManager) levelSizes() []int64 {
	if lm == nil || len(lm.levels) == 0 {
		return nil
	}
	sizes := make([]int64, len(lm.levels))
	for i, lvl := range lm.levels {
		if lvl == nil {
			continue
		}
		sizes[i] = lvl.getTotalSize()
	}
	return sizes
}

func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	if cd.thisLevel.numTables() == 0 {
		if cd.thisLevel.isLastLevel() && cd.thisLevel.numIngestTables() > 0 {
			meta := cd.thisLevel.ingest.allMeta()
			if len(meta) == 0 {
				return false
			}
			plan, ok := compact.PlanForIngestFallback(cd.thisLevel.levelNum, meta)
			if !ok {
				return false
			}
			cd.plan.IngestMode = compact.IngestKeep
			cd.applyPlan(plan)
			if !lm.resolvePlanLocked(cd) {
				return false
			}
			return lm.compactState.CompareAndAdd(compact.LevelsLocked{}, cd.stateEntry())
		}
		return false
	}
	tables := make([]*table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}
	plan, ok := compact.PlanForRegular(cd.thisLevel.levelNum, tableMetaSnapshot(tables), cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), lm.compactState)
	if !ok {
		return false
	}
	cd.applyPlan(plan)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(compact.LevelsLocked{}, cd.stateEntry())
}

func (lm *levelManager) fillTablesIngestShard(cd *compactDef, shardIdx int) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	totalIngest := cd.thisLevel.numIngestTables()
	if totalIngest == 0 {
		return false
	}
	batchSize := lm.opt.IngestCompactBatchSize
	if batchSize <= 0 || batchSize > totalIngest {
		batchSize = totalIngest
	}
	if shardIdx < 0 {
		shardIdx = cd.thisLevel.ingestShardByBacklog()
	}
	shMeta := cd.thisLevel.ingest.shardMetaByIndex(shardIdx)
	if len(shMeta) == 0 {
		return false
	}
	plan, ok := compact.PlanForIngestShard(cd.thisLevel.levelNum, shMeta, cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), cd.targetFileSize(), batchSize, lm.compactState)
	if !ok {
		return false
	}
	cd.applyPlan(plan)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(compact.LevelsLocked{}, cd.stateEntry())
}
func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
	if cd.plan.NextFileSize <= 0 {
		return errors.New("Next file size cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	utils.CondPanic(len(cd.splits) != 0, errors.New("len(cd.splits) != 0"))
	if thisLevel == nextLevel {
		// No special handling for L0->L0 and Lmax->Lmax.
	} else {
		lm.addSplits(&cd)
	}
	// Append an empty range placeholder when no split is found.
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, compact.KeyRange{})
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
		case pb.ManifestChange_CREATE:
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
					Ingest:    cd.plan.IngestMode == compact.IngestKeep,
				},
			}
			manifestEdits = append(manifestEdits, add)
		case pb.ManifestChange_DELETE:
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

	defer func() {
		_ = decrRefs(cd.top)
	}()
	if cd.plan.IngestMode == compact.IngestKeep {
		if err := thisLevel.replaceIngestTables(cd.top, newTables); err != nil {
			return err
		}
		if thisLevel.levelNum > 0 {
			thisLevel.Sort()
		}
	} else {
		if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
			return err
		}
		if err := thisLevel.deleteIngestTables(cd.top); err != nil {
			return err
		}
		if err := thisLevel.deleteTables(cd.top); err != nil {
			return err
		}
	}

	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		fmt.Printf("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.levelNum, nextLevel.levelNum, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}
	// Record ingest metrics if applicable.
	if cd.plan.IngestMode.UsesIngest() {
		tablesCompacted := len(cd.top) + len(cd.bot)
		cd.thisLevel.recordIngestMetrics(cd.plan.IngestMode == compact.IngestKeep, time.Since(timeStart), tablesCompacted)
	}
	lm.recordCompactionMetrics(time.Since(timeStart))
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

// resolveTablesLocked maps IDs to tables; caller must hold lh lock.
func resolveTablesLocked(lh *levelHandler, ids []uint64, ingest bool) []*table {
	if lh == nil || len(ids) == 0 {
		return nil
	}
	var tables []*table
	if ingest {
		tables = lh.ingest.allTables()
	} else {
		tables = lh.tables
	}
	if len(tables) == 0 {
		return nil
	}
	byID := make(map[uint64]*table, len(tables))
	for _, t := range tables {
		if t != nil {
			byID[t.fid] = t
		}
	}
	out := make([]*table, 0, len(ids))
	for _, id := range ids {
		t, ok := byID[id]
		if !ok {
			return nil
		}
		out = append(out, t)
	}
	return out
}

func tableMetaSnapshot(tables []*table) []compact.TableMeta {
	if len(tables) == 0 {
		return nil
	}
	out := make([]compact.TableMeta, 0, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		meta := compact.TableMeta{
			ID:         t.fid,
			MinKey:     t.MinKey(),
			MaxKey:     t.MaxKey(),
			Size:       t.Size(),
			StaleSize:  int64(t.StaleDataSize()),
			MaxVersion: t.MaxVersionVal(),
		}
		if created := t.GetCreatedAt(); created != nil {
			meta.CreatedAt = *created
		}
		out = append(out, meta)
	}
	return out
}

func findTableByID(tables []*table, fid uint64) *table {
	for _, t := range tables {
		if t.fid == fid {
			return t
		}
	}
	return nil
}

// buildChangeSet _
func buildChangeSet(cd *compactDef, newTables []*table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes, newCreateChange(table.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.fid))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

func newDeleteChange(id uint64) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id: id,
		Op: pb.ManifestChange_DELETE,
	}
}

// newCreateChange
func newCreateChange(id uint64, level int) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:    id,
		Op:    pb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

// compactBuildTables merges SSTables from two levels.
func (lm *levelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {

	topTables := cd.top
	botTables := cd.bot
	iterOpt := &utils.Options{
		IsAsc:          true,
		AccessPattern:  utils.AccessPatternSequential,
		PrefetchBlocks: 1,
	}
	//numTables := int64(len(topTables) + len(botTables))
	newIterator := func() []utils.Iterator {
		// Create iterators across all the tables involved first.
		var iters []utils.Iterator
		switch {
		case lev == 0:
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		case len(topTables) > 0:
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		}
		return append(iters, NewConcatIterator(botTables, iterOpt))
	}

	// Start parallel compaction tasks.
	res := make(chan *table, 3)
	// Throttle inflight builders to bound memory and file handles.
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		kr := kr
		if err := inflightBuilders.Go(func() error {
			it := NewMergeIterator(newIterator(), false)
			defer it.Close()
			lm.subcompact(it, kr, cd, inflightBuilders, res)
			return nil
		}); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
	}

	// Collect table handles via fan-in.
	var newTables []*table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()

	// Wait for all compaction tasks to finish.
	err := inflightBuilders.Finish()
	// Release channel resources.
	close(res)
	// Wait for all builders to flush to disk.
	wg.Wait()

	if err == nil {
		// Sync the workdir to ensure data is persisted.
		err = utils.SyncDir(lm.opt.WorkDir)
	}

	if err != nil {
		// On error, delete newly created files.
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return utils.CompareKeys(newTables[i].MaxKey(), newTables[j].MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

// addSplits prepares key ranges for parallel sub-compactions.
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	width := max(int(math.Ceil(float64(len(cd.bot))/5.0)), 3)
	skr := cd.plan.ThisRange
	skr.Extend(cd.plan.NextRange)

	addRange := func(right []byte) {
		skr.Right = slices.Clone(right)
		cd.splits = append(cd.splits, skr)
		skr.Left = skr.Right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			// Set the right bound to the max key.
			right := kv.KeyWithTs(kv.ParseKey(t.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}
}

// fillMaxLevelTables handles max-level compaction.
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	plan, ok := compact.PlanForMaxLevel(cd.thisLevel.levelNum, tableMetaSnapshot(tables), cd.plan.ThisFileSize, lm.compactState, time.Now())
	if !ok {
		return false
	}
	cd.applyPlan(plan)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(compact.LevelsLocked{}, cd.stateEntry())
}

// fillTablesL0 tries L0->Lbase first, then falls back to L0->L0.
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return lm.fillTablesL0ToL0(cd)
}

func (lm *levelManager) moveToIngest(cd *compactDef) error {
	if cd == nil || cd.thisLevel == nil || cd.nextLevel == nil {
		return errors.New("invalid compaction definition for ingest move")
	}
	if len(cd.top) == 0 {
		return nil
	}
	var edits []manifest.Edit
	for _, tbl := range cd.top {
		if tbl == nil {
			continue
		}
		del := manifest.Edit{
			Type: manifest.EditDeleteFile,
			File: &manifest.FileMeta{FileID: tbl.fid, Level: cd.thisLevel.levelNum},
		}
		edits = append(edits, del)
		add := manifest.Edit{
			Type: manifest.EditAddFile,
			File: &manifest.FileMeta{
				Level:     cd.nextLevel.levelNum,
				FileID:    tbl.fid,
				Size:      uint64(tbl.Size()),
				Smallest:  kv.SafeCopy(nil, tbl.MinKey()),
				Largest:   kv.SafeCopy(nil, tbl.MaxKey()),
				CreatedAt: uint64(time.Now().Unix()),
				ValueSize: tbl.ValueSize(),
				Ingest:    true,
			},
		}
		edits = append(edits, add)
	}
	if err := lm.manifestMgr.LogEdits(edits...); err != nil {
		return err
	}

	toDel := make(map[uint64]struct{}, len(cd.top))
	for _, tbl := range cd.top {
		if tbl == nil {
			continue
		}
		toDel[tbl.fid] = struct{}{}
	}

	// Update in-memory state atomically across the source and target levels to avoid
	// a visibility gap for readers walking L0 -> Ln.
	first, second := cd.thisLevel, cd.nextLevel
	if first.levelNum > second.levelNum {
		first, second = second, first
	}
	first.Lock()
	second.Lock()
	var remaining []*table
	for _, tbl := range cd.thisLevel.tables {
		if _, found := toDel[tbl.fid]; found {
			cd.thisLevel.subtractSize(tbl)
			continue
		}
		remaining = append(remaining, tbl)
	}
	cd.thisLevel.tables = remaining

	cd.nextLevel.ingest.ensureInit()
	for _, t := range cd.top {
		if t == nil {
			continue
		}
		t.setLevel(cd.nextLevel.levelNum)
	}
	cd.nextLevel.ingest.addBatch(cd.top)
	cd.nextLevel.ingest.sortShards()
	second.Unlock()
	first.Unlock()

	if lm.compaction != nil {
		lm.compaction.Trigger("ingest-buffer")
	}
	return nil
}

func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("base level can be zero"))
	}
	// Skip if priority is below 1.
	if cd.adjusted > 0.0 && cd.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}
	plan, ok := compact.PlanForL0ToLbase(tableMetaSnapshot(top), cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), lm.compactState)
	if !ok {
		return false
	}
	cd.applyPlan(plan)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(compact.LevelsLocked{}, cd.stateEntry())
}

// fillTablesL0ToL0 performs L0->L0 compaction.
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		// Only allow compactor 0 to avoid L0->L0 contention.
		return false
	}

	cd.nextLevel = lm.levels[0]
	cd.plan.NextLevel = cd.plan.ThisLevel
	cd.plan.NextFileSize = cd.plan.ThisFileSize
	cd.plan.NextRange = compact.KeyRange{}
	cd.bot = nil

	// We intentionally avoid calling compactDef.lockLevels here. Both thisLevel and nextLevel
	// point at L0, so grabbing the RLock twice would violate RWMutex semantics and can deadlock
	// once another goroutine attempts a write lock. Taking the shared lock exactly once matches
	// Badger's approach and keeps lock acquisition order (level -> compactState) consistent.
	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	top := cd.thisLevel.tables
	now := time.Now()
	plan, ok := compact.PlanForL0ToL0(cd.thisLevel.levelNum, tableMetaSnapshot(top), cd.plan.ThisFileSize, lm.compactState, now)
	if !ok {
		// Skip when fewer than four tables qualify.
		return false
	}
	cd.applyPlan(plan)
	if !lm.resolvePlanLocked(cd) {
		return false
	}

	// Avoid L0->other-level compactions during this phase.
	lm.compactState.AddRangeWithTables(cd.thisLevel.levelNum, compact.InfRange, cd.plan.TopIDs)

	// L0->L0 compaction collapses into a single file, reducing L0 count and read amplification.
	cd.plan.ThisFileSize = math.MaxUint32
	cd.plan.NextFileSize = cd.plan.ThisFileSize
	return true
}

// getKeyRange returns the merged min/max key range for a set of tables.
func getKeyRange(tables ...*table) compact.KeyRange {
	if len(tables) == 0 {
		return compact.KeyRange{}
	}
	minKey := tables[0].MinKey()
	maxKey := tables[0].MaxKey()
	for i := 1; i < len(tables); i++ {
		if utils.CompareKeys(tables[i].MinKey(), minKey) < 0 {
			minKey = tables[i].MinKey()
		}
		if utils.CompareKeys(tables[i].MaxKey(), maxKey) > 0 {
			maxKey = tables[i].MaxKey()
		}
	}

	// We pick all the versions of the smallest and the biggest key. Note that version zero would
	// be the rightmost key, considering versions are default sorted in descending order.
	return compact.KeyRange{
		Left:  kv.KeyWithTs(kv.ParseKey(minKey), math.MaxUint64),
		Right: kv.KeyWithTs(kv.ParseKey(maxKey), 0),
	}
}

func iteratorsReversed(th []*table, opt *utils.Options) []utils.Iterator {
	out := make([]utils.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}
func (lm *levelManager) updateDiscardStats(discardStats map[manifest.ValueLogID]int64) {
	select {
	case *lm.lsm.option.DiscardStatsCh <- discardStats:
	default:
	}
}

// subcompact runs a single parallel compaction over a key range.
func (lm *levelManager) subcompact(it utils.Iterator, kr compact.KeyRange, cd compactDef,
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
	addKeys := func(builder *tableBuilder) {
		var tableKr compact.KeyRange
		for ; it.Valid(); it.Next() {
			key := it.Item().Entry().Key
			//version := kv.ParseTs(key)
			isExpired := IsDeletedOrExpired(it.Item().Entry())
			if !kv.SameKey(key, lastKey) {
				// Stop when the iterator key is beyond the range.
				if len(kr.Right) > 0 && utils.CompareKeys(key, kr.Right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// Stop when the SST size estimate is exceeded.
					break
				}
				// Update lastKey with the current key.
				lastKey = kv.SafeCopy(lastKey, key)
				//umVersions = 0
				// If the left bound is empty, set it to the current key.
				if len(tableKr.Left) == 0 {
					tableKr.Left = kv.SafeCopy(tableKr.Left, key)
				}
				// Update the right bound.
				tableKr.Right = lastKey
			}
			// Drop expired entries.
			valueLen := entryValueLen(it.Item().Entry())
			switch {
			case isExpired:
				updateStats(it.Item().Entry())
				builder.AddStaleEntryWithLen(it.Item().Entry(), valueLen)
			default:
				builder.AddKeyWithLen(it.Item().Entry(), valueLen)
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
		if len(kr.Right) > 0 && utils.CompareKeys(key, kr.Right) >= 0 {
			break
		}
		// Copy Options so background tuning does not affect the active compaction.
		builderOpt := lm.opt.Clone()
		builder := newTableBuilerWithSSTSize(builderOpt, cd.plan.NextFileSize)

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.empty() {
			// Cleanup builder resources:
			builder.finish()
			builder.Close()
			continue
		}
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
		// Leverage SSD parallel write throughput.
		go func(builder *tableBuilder) {
			defer inflightBuilders.Done(nil)
			defer builder.Close()
			var tbl *table
			newFID := atomic.AddUint64(&lm.maxFID, 1) // Compaction does not allocate memtables; advance maxFID.
			sstName := utils.FileNameSSTable(lm.opt.WorkDir, newFID)
			tbl = openTable(lm, sstName, builder)
			if tbl == nil {
				return
			}
			res <- tbl
		}(builder)
	}
}

// IsDeletedOrExpired reports whether an entry should be dropped.
func IsDeletedOrExpired(e *kv.Entry) bool {
	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

func (lsm *LSM) newCompactStatus() *compact.State {
	return compact.NewState(lsm.option.MaxLevelNum)
}
