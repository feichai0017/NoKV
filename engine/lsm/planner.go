package lsm

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/utils"
)

type compactDef struct {
	compactorId int
	plan        Plan
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table
	bot []*table

	splits []KeyRange

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

func (cd *compactDef) stateEntry() StateEntry {
	return cd.plan.StateEntry(cd.thisSize)
}

func (cd *compactDef) setNextLevel(lm *levelManager, t Targets, next *levelHandler) {
	cd.nextLevel = next
	if next == nil {
		return
	}
	cd.plan.NextLevel = next.levelNum
	cd.plan.NextFileSize = lm.targetFileSizeForLevel(t, next.levelNum)
}

func (cd *compactDef) applyPlan(plan Plan) {
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
	if cd.nextLevel != cd.thisLevel {
		cd.nextLevel.RLock()
	}
}

func (cd *compactDef) unlockLevels() {
	if cd.nextLevel != cd.thisLevel {
		cd.nextLevel.RUnlock()
	}
	cd.thisLevel.RUnlock()
}

func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	if cd.thisLevel.numTablesLocked() == 0 {
		if cd.thisLevel.isLastLevel() && cd.thisLevel.numIngestTablesLocked() > 0 {
			meta := cd.thisLevel.ingest.allMeta()
			if len(meta) == 0 {
				return false
			}
			plan, ok := PlanForIngestFallback(cd.thisLevel.levelNum, meta)
			if !ok {
				return false
			}
			cd.plan.IngestMode = IngestKeep
			cd.applyPlan(plan)
			if !lm.resolvePlanLocked(cd) {
				return false
			}
			return lm.compactState.CompareAndAdd(LevelsLocked{}, cd.stateEntry())
		}
		return false
	}
	tables := make([]*table, cd.thisLevel.numTablesLocked())
	copy(tables, cd.thisLevel.tables)
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}
	plan, ok := PlanForRegular(cd.thisLevel.levelNum, tableMetaSnapshot(tables), cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), lm.compactState)
	if !ok {
		return false
	}
	cd.applyPlan(plan)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(LevelsLocked{}, cd.stateEntry())
}

func (lm *levelManager) fillTablesIngestShard(cd *compactDef, shardIdx int) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	totalIngest := cd.thisLevel.numIngestTablesLocked()
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
	plan, ok := PlanForIngestShard(cd.thisLevel.levelNum, shMeta, cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), cd.targetFileSize(), batchSize, lm.compactState)
	if !ok {
		return false
	}
	cd.applyPlan(plan)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(LevelsLocked{}, cd.stateEntry())
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

func tableMetaSnapshot(tables []*table) []TableMeta {
	if len(tables) == 0 {
		return nil
	}
	out := make([]TableMeta, 0, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		meta := TableMeta{
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
			cf, userKey, _, ok := kv.SplitInternalKey(t.MaxKey())
			utils.CondPanicFunc(!ok, func() error {
				return fmt.Errorf("addSplits expects internal max key: %x", t.MaxKey())
			})
			right := kv.InternalKey(cf, userKey, math.MaxUint64)
			addRange(right)
		}
	}
}

// fillMaxLevelTables handles max-level compaction.
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	plan, ok := PlanForMaxLevel(cd.thisLevel.levelNum, tableMetaSnapshot(tables), cd.plan.ThisFileSize, lm.compactState, time.Now())
	if !ok {
		return false
	}
	cd.applyPlan(plan)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(LevelsLocked{}, cd.stateEntry())
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
		lm.compaction.Trigger()
	}
	return nil
}

func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		return false
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
	plan, ok := PlanForL0ToLbase(tableMetaSnapshot(top), cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), lm.compactState)
	if !ok {
		return false
	}
	cd.applyPlan(plan)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(LevelsLocked{}, cd.stateEntry())
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
	cd.plan.NextRange = KeyRange{}
	cd.bot = nil

	// We intentionally avoid calling compactDef.lockLevels here. Both thisLevel and nextLevel
	// point at L0, so grabbing the RLock twice would violate RWMutex semantics and can deadlock
	// once another goroutine attempts a write lock. Taking the shared lock exactly once matches
	// Badger's approach and keeps lock acquisition order (level -> compactState) consistent.
	utils.CondPanicFunc(cd.thisLevel.levelNum != 0, func() error { return errors.New("cd.thisLevel.levelNum != 0") })
	utils.CondPanicFunc(cd.nextLevel.levelNum != 0, func() error { return errors.New("cd.nextLevel.levelNum != 0") })
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	top := cd.thisLevel.tables
	now := time.Now()
	plan, ok := PlanForL0ToL0(cd.thisLevel.levelNum, tableMetaSnapshot(top), cd.plan.ThisFileSize, lm.compactState, now)
	if !ok {
		// Skip when fewer than four tables qualify.
		return false
	}
	cd.applyPlan(plan)
	if !lm.resolvePlanLocked(cd) {
		return false
	}

	// Avoid L0->other-level compactions during this phase.
	lm.compactState.AddRangeWithTables(cd.thisLevel.levelNum, InfRange, cd.plan.TopIDs)

	// L0->L0 compaction collapses into a single file, reducing L0 count and read amplification.
	cd.plan.ThisFileSize = math.MaxUint32
	cd.plan.NextFileSize = cd.plan.ThisFileSize
	return true
}

// getKeyRange returns the merged min/max key range for a set of tables.
func getKeyRange(tables ...*table) KeyRange {
	if len(tables) == 0 {
		return KeyRange{}
	}
	minKey := tables[0].MinKey()
	maxKey := tables[0].MaxKey()
	for i := 1; i < len(tables); i++ {
		if kv.CompareInternalKeys(tables[i].MinKey(), minKey) < 0 {
			minKey = tables[i].MinKey()
		}
		if kv.CompareInternalKeys(tables[i].MaxKey(), maxKey) > 0 {
			maxKey = tables[i].MaxKey()
		}
	}

	// We pick all the versions of the smallest and the biggest key. Note that version zero would
	// be the rightmost key, considering versions are default sorted in descending order.
	leftCF, leftUserKey, _, leftOK := kv.SplitInternalKey(minKey)
	utils.CondPanicFunc(!leftOK, func() error {
		return fmt.Errorf("getKeyRange expects internal min key: %x", minKey)
	})
	rightCF, rightUserKey, _, rightOK := kv.SplitInternalKey(maxKey)
	utils.CondPanicFunc(!rightOK, func() error {
		return fmt.Errorf("getKeyRange expects internal max key: %x", maxKey)
	})
	return KeyRange{
		Left:  kv.InternalKey(leftCF, leftUserKey, math.MaxUint64),
		Right: kv.InternalKey(rightCF, rightUserKey, 0),
	}
}

// Plan captures a compaction plan without tying it to in-memory tables.
type Plan struct {
	ThisLevel    int
	NextLevel    int
	TopIDs       []uint64
	BotIDs       []uint64
	ThisRange    KeyRange
	NextRange    KeyRange
	ThisFileSize int64
	NextFileSize int64
	IngestMode   IngestMode
	DropPrefixes [][]byte
	StatsTag     string
}

// StateEntry creates a compaction state entry for this plan.
func (p Plan) StateEntry(thisSize int64) StateEntry {
	entry := StateEntry{
		ThisLevel: p.ThisLevel,
		NextLevel: p.NextLevel,
		ThisRange: p.ThisRange,
		NextRange: p.NextRange,
		ThisSize:  thisSize,
	}
	if len(p.TopIDs) == 0 && len(p.BotIDs) == 0 {
		return entry
	}
	entry.TableIDs = make([]uint64, 0, len(p.TopIDs)+len(p.BotIDs))
	entry.TableIDs = append(entry.TableIDs, p.TopIDs...)
	entry.TableIDs = append(entry.TableIDs, p.BotIDs...)
	return entry
}

// TableMeta captures the metadata needed to plan a compaction (no table refs).
type TableMeta struct {
	ID         uint64
	MinKey     []byte
	MaxKey     []byte
	Size       int64
	StaleSize  int64
	CreatedAt  time.Time
	MaxVersion uint64
}

// RangeForTables returns the combined key span for a set of tables.
func RangeForTables(tables []TableMeta) KeyRange {
	if len(tables) == 0 {
		return KeyRange{}
	}
	minKey := tables[0].MinKey
	maxKey := tables[0].MaxKey
	for i := 1; i < len(tables); i++ {
		if kv.CompareInternalKeys(tables[i].MinKey, minKey) < 0 {
			minKey = tables[i].MinKey
		}
		if kv.CompareInternalKeys(tables[i].MaxKey, maxKey) > 0 {
			maxKey = tables[i].MaxKey
		}
	}
	leftCF, leftUserKey, _, leftOK := kv.SplitInternalKey(minKey)
	utils.CondPanicFunc(!leftOK, func() error {
		return fmt.Errorf("RangeForTables expects internal min key: %x", minKey)
	})
	rightCF, rightUserKey, _, rightOK := kv.SplitInternalKey(maxKey)
	utils.CondPanicFunc(!rightOK, func() error {
		return fmt.Errorf("RangeForTables expects internal max key: %x", maxKey)
	})
	return KeyRange{
		Left:  kv.InternalKey(leftCF, leftUserKey, math.MaxUint64),
		Right: kv.InternalKey(rightCF, rightUserKey, 0),
	}
}

// OverlappingTables returns the half-interval of tables overlapping with kr.
func OverlappingTables(tables []TableMeta, kr KeyRange) (int, int) {
	if len(kr.Left) == 0 || len(kr.Right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(tables), func(i int) bool {
		return kv.CompareInternalKeys(kr.Left, tables[i].MaxKey) <= 0
	})
	right := sort.Search(len(tables), func(i int) bool {
		return kv.CompareInternalKeys(kr.Right, tables[i].MaxKey) < 0
	})
	return left, right
}

// PlanForIngestFallback builds a plan when only ingest tables are available.
func PlanForIngestFallback(level int, tables []TableMeta) (Plan, bool) {
	if len(tables) == 0 {
		return Plan{}, false
	}
	kr := RangeForTables(tables)
	return Plan{
		ThisLevel: level,
		NextLevel: level,
		TopIDs:    tableIDsFromMeta(tables),
		ThisRange: kr,
		NextRange: kr,
	}, true
}

// PlanForRegular selects tables for a standard compaction.
func PlanForRegular(level int, tables []TableMeta, nextLevel int, next []TableMeta, state *State) (Plan, bool) {
	if len(tables) == 0 {
		return Plan{}, false
	}
	sorted := append([]TableMeta(nil), tables...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].MaxVersion < sorted[j].MaxVersion
	})
	for _, t := range sorted {
		kr := RangeForTables([]TableMeta{t})
		if state != nil && state.Overlaps(level, kr) {
			continue
		}
		left, right := OverlappingTables(next, kr)
		bot := next[left:right]
		nextRange := kr
		if len(bot) > 0 {
			nextRange = RangeForTables(bot)
			if state != nil && state.Overlaps(nextLevel, nextRange) {
				continue
			}
		}
		return Plan{
			ThisLevel: level,
			NextLevel: nextLevel,
			TopIDs:    []uint64{t.ID},
			BotIDs:    tableIDsFromMeta(bot),
			ThisRange: kr,
			NextRange: nextRange,
		}, true
	}
	return Plan{}, false
}

// PlanForMaxLevel selects tables to rewrite stale data in the max level.
func PlanForMaxLevel(level int, tables []TableMeta, targetFileSize int64, state *State, now time.Time) (Plan, bool) {
	if len(tables) == 0 {
		return Plan{}, false
	}
	sorted := append([]TableMeta(nil), tables...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].StaleSize > sorted[j].StaleSize
	})
	if sorted[0].StaleSize == 0 {
		return Plan{}, false
	}
	for _, t := range sorted {
		if !t.CreatedAt.IsZero() && now.Sub(t.CreatedAt) < time.Hour {
			continue
		}
		if t.StaleSize < 10<<20 {
			continue
		}
		kr := RangeForTables([]TableMeta{t})
		if state != nil && state.Overlaps(level, kr) {
			continue
		}
		top := []TableMeta{t}
		bot := collectBotTables(t, tables, targetFileSize)
		nextRange := kr
		if len(bot) > 0 {
			nextRange.Extend(RangeForTables(bot))
		}
		return Plan{
			ThisLevel: level,
			NextLevel: level,
			TopIDs:    tableIDsFromMeta(top),
			BotIDs:    tableIDsFromMeta(bot),
			ThisRange: kr,
			NextRange: nextRange,
		}, true
	}
	return Plan{}, false
}

// PlanForIngestShard builds a plan for a single ingest shard.
func PlanForIngestShard(level int, shardTables []TableMeta, nextLevel int, next []TableMeta, targetFileSize int64, batchSize int, state *State) (Plan, bool) {
	if len(shardTables) == 0 {
		return Plan{}, false
	}
	if batchSize <= 0 {
		batchSize = len(shardTables)
	}
	shardSize := int64(0)
	for _, t := range shardTables {
		shardSize += t.Size
	}
	if targetFileSize > 0 {
		score := float64(shardSize) / float64(targetFileSize)
		if score > 1.0 {
			boost := int(math.Ceil(score))
			if boost > 1 {
				batchSize *= boost
			}
		}
	}
	if batchSize > len(shardTables) {
		batchSize = len(shardTables)
	}
	top := shardTables[:batchSize]
	kr := RangeForTables(top)
	if state != nil && state.Overlaps(level, kr) {
		return Plan{}, false
	}
	left, right := OverlappingTables(next, kr)
	bot := next[left:right]
	nextRange := kr
	if len(bot) > 0 {
		nextRange = RangeForTables(bot)
		if state != nil && state.Overlaps(nextLevel, nextRange) {
			return Plan{}, false
		}
	}
	return Plan{
		ThisLevel: level,
		NextLevel: nextLevel,
		TopIDs:    tableIDsFromMeta(top),
		BotIDs:    tableIDsFromMeta(bot),
		ThisRange: kr,
		NextRange: nextRange,
	}, true
}

// PlanForL0ToLbase builds a plan for L0 -> base level compaction.
func PlanForL0ToLbase(l0 []TableMeta, nextLevel int, next []TableMeta, state *State) (Plan, bool) {
	if len(l0) == 0 {
		return Plan{}, false
	}
	var out []TableMeta
	var kr KeyRange
	for _, t := range l0 {
		dkr := RangeForTables([]TableMeta{t})
		if kr.OverlapsWith(dkr) {
			out = append(out, t)
			kr.Extend(dkr)
		} else {
			break
		}
	}
	if len(out) == 0 {
		return Plan{}, false
	}
	thisRange := RangeForTables(out)
	if state != nil && state.Overlaps(0, thisRange) {
		return Plan{}, false
	}
	left, right := OverlappingTables(next, thisRange)
	bot := next[left:right]
	nextRange := thisRange
	if len(bot) > 0 {
		nextRange = RangeForTables(bot)
		if state != nil && state.Overlaps(nextLevel, nextRange) {
			return Plan{}, false
		}
	}
	return Plan{
		ThisLevel: 0,
		NextLevel: nextLevel,
		TopIDs:    tableIDsFromMeta(out),
		BotIDs:    tableIDsFromMeta(bot),
		ThisRange: thisRange,
		NextRange: nextRange,
	}, true
}

// PlanForL0ToL0 builds a plan for L0 -> L0 compaction.
func PlanForL0ToL0(level int, tables []TableMeta, fileSize int64, state *State, now time.Time) (Plan, bool) {
	var out []TableMeta
	for _, t := range tables {
		if fileSize > 0 && t.Size >= 2*fileSize {
			continue
		}
		if !t.CreatedAt.IsZero() && now.Sub(t.CreatedAt) < 10*time.Second {
			continue
		}
		if state != nil && state.HasTable(t.ID) {
			continue
		}
		out = append(out, t)
	}
	if len(out) < 4 {
		return Plan{}, false
	}
	return Plan{
		ThisLevel: level,
		NextLevel: level,
		TopIDs:    tableIDsFromMeta(out),
		ThisRange: InfRange,
		NextRange: InfRange,
	}, true
}

func tableIDsFromMeta(tables []TableMeta) []uint64 {
	if len(tables) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(tables))
	for _, t := range tables {
		ids = append(ids, t.ID)
	}
	return ids
}

func collectBotTables(seed TableMeta, tables []TableMeta, needSz int64) []TableMeta {
	j := sort.Search(len(tables), func(i int) bool {
		return kv.CompareInternalKeys(tables[i].MinKey, seed.MinKey) >= 0
	})
	if j >= len(tables) || tables[j].ID != seed.ID {
		return nil
	}
	j++
	totalSize := seed.Size
	var bot []TableMeta
	for j < len(tables) {
		newT := tables[j]
		totalSize += newT.Size
		if totalSize >= needSz {
			break
		}
		bot = append(bot, newT)
		j++
	}
	return bot
}
