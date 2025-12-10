package lsm

import (
	"bytes"
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
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
)

// 归并优先级
type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            targets
	ingestOnly   bool
	ingestMerge  bool
	statsTag     string
}

func (cp *compactionPriority) applyValueWeight(weight, valueScore float64) {
	if weight <= 0 || valueScore <= 0 {
		return
	}
	capped := math.Min(valueScore, 16)
	cp.score += weight * capped
	cp.adjusted = cp.score
}

// 归并目标
type targets struct {
	baseLevel int
	targetSz  []int64
	fileSz    []int64
}
type compactDef struct {
	compactorId int
	t           targets
	p           compactionPriority
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table
	bot []*table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
	ingestOnly   bool
	ingestMerge  bool
	statsTag     string
}

func (cd *compactDef) targetFileSize() int64 {
	level := cd.thisLevel.levelNum
	if level >= 0 && level < len(cd.t.fileSz) {
		if cd.t.fileSz[level] > 0 {
			return cd.t.fileSz[level]
		}
	}
	if level >= 0 && level < len(cd.t.targetSz) && cd.t.targetSz[level] > 0 {
		return cd.t.targetSz[level]
	}
	return 0
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

func moveL0toFront(prios []compactionPriority) []compactionPriority {
	idx := -1
	for i, p := range prios {
		if p.level == 0 {
			idx = i
			break
		}
	}
	// If idx == -1, we didn't find L0.
	// If idx == 0, then we don't need to do anything. L0 is already at the front.
	if idx > 0 {
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

// doCompact 选择level的某些表合并到目标level
func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	l := p.level
	utils.CondPanic(l >= lm.opt.MaxLevelNum, errors.New("[doCompact] Sanity check. l >= lm.opt.MaxLevelNum")) // Sanity check.
	if p.t.baseLevel == 0 {
		p.t = lm.levelTargets()
	}
	// 创建真正的压缩计划
	cd := compactDef{
		compactorId:  id,
		p:            p,
		t:            p.t,
		thisLevel:    lm.levels[l],
		dropPrefixes: p.dropPrefixes,
		ingestOnly:   p.ingestOnly,
		ingestMerge:  p.ingestMerge,
		statsTag:     p.statsTag,
	}

	var cleanup bool
	defer func() {
		if cleanup {
			lm.compactState.delete(cd)
		}
	}()

	if p.ingestOnly && l > 0 {
		cd.nextLevel = cd.thisLevel
		order := cd.thisLevel.ingest.shardOrderBySize()
		if len(order) == 0 {
			return utils.ErrFillTables
		}
		baseLimit := lm.opt.IngestShardParallelism
		if baseLimit <= 0 {
			baseLimit = lm.opt.NumCompactors / 2
			if baseLimit < 1 {
				baseLimit = 1
			}
		}
		if baseLimit > len(order) {
			baseLimit = len(order)
		}
		// Adaptive bump: more backlog ⇒ allow more shards, capped by shard count.
		shardLimit := baseLimit
		if p.score > 1.0 {
			shardLimit += int(math.Ceil(p.score / 2))
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
			sub.ingestMerge = p.ingestMerge
			sub.statsTag = p.statsTag
			if err := lm.runCompactDef(id, l, sub); err != nil {
				log.Printf("[Compactor: %d] LOG Ingest Compact FAILED with error: %+v: %+v", id, err, sub)
				lm.compactState.delete(sub)
				return err
			}
			lm.compactState.delete(sub)
			ran = true
			log.Printf("[Compactor: %d] Ingest compaction for level: %d shard=%d DONE", id, sub.thisLevel.levelNum, order[i])
		}
		if !ran {
			return utils.ErrFillTables
		}
		return nil
	}

	// 如果是第0层 对齐单独填充处理
	if l == 0 {
		cd.nextLevel = lm.levels[p.t.baseLevel]
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
		cd.nextLevel = cd.thisLevel
		// 如果不是最后一层，则压缩到下一层即可
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = lm.levels[l+1]
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
		cleanup = true
		// 继续执行常规归并
		if err := lm.runCompactDef(id, l, cd); err != nil {
			log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
			return err
		}
		log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
		return nil
	}

	// 执行合并计划
	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}
	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}

// pickCompactLevel 选择合适的level执行合并，返回判断的优先级
func (lm *levelManager) pickCompactLevels() (prios []compactionPriority) {
	t := lm.levelTargets()
	valueWeight := lm.opt.CompactionValueWeight
	prios = make([]compactionPriority, len(lm.levels))
	var extras []compactionPriority
	addPriority := func(level int, score float64, ingest bool, merge bool) {
		pri := compactionPriority{
			level:       level,
			score:       score,
			adjusted:    score,
			t:           t,
			ingestOnly:  ingest,
			ingestMerge: merge,
			statsTag:    "regular",
		}
		if valueWeight > 0 && level < len(lm.levels) {
			lvl := lm.levels[level]
			if lvl != nil {
				var valueBytes int64
				var target float64
				if level == 0 {
					valueBytes = lvl.getTotalValueSize()
					target = float64(lm.opt.BaseLevelSize)
					if target <= 0 {
						target = float64(lm.opt.BaseTableSize)
					}
				} else if ingest {
					valueBytes = lvl.ingestValueBytes()
					target = float64(t.fileSz[level])
					if target <= 0 {
						target = float64(lm.opt.BaseTableSize)
					}
					if target <= 0 {
						target = 1
					}
				} else {
					valueBytes = lvl.mainValueBytes()
					target = float64(t.targetSz[level])
				}
				if target <= 0 {
					target = float64(lm.opt.BaseTableSize)
					if target <= 0 {
						target = 1
					}
				}
				valueScore := float64(valueBytes) / target
				if ingest && valueScore == 0 {
					valueScore = lvl.ingestValueDensity()
				}
				pri.applyValueWeight(valueWeight, valueScore)
			}
		}
		if merge {
			extras = append(extras, pri)
			return
		}
		prios[level] = pri
	}

	// 根据l0表的table数量来对压缩提权
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLevelZeroTables), false, false)

	// 非l0 层都根据大小计算优先级
	for i := 1; i < len(lm.levels); i++ {
		lvl := lm.levels[i]
		if lvl.numIngestTables() > 0 {
			denom := t.fileSz[i]
			if denom <= 0 {
				denom = lm.opt.BaseTableSize
				if denom <= 0 {
					denom = 1
				}
			}
			ingestScore := float64(lvl.ingestDataSize()) / float64(denom)
			if ingestScore < 1.0 {
				ingestScore = 1.0
			}
			// Age bias: older ingest tables get compacted sooner.
			ageSec := lvl.maxIngestAgeSeconds()
			if ageSec > 0 {
				ageFactor := math.Min(ageSec/60.0, 4.0) // cap bias
				ingestScore += ageFactor
			}
			addPriority(i, ingestScore+1.0, true, false)
			trigger := lm.opt.IngestBacklogMergeScore
			if trigger <= 0 {
				trigger = 2.0
			}
			// Dynamic merge trigger: lower threshold when backlog or age is very high.
			dynTrigger := trigger
			if ingestScore >= trigger*2 {
				dynTrigger = trigger * 0.8
			} else if ageSec > 120 {
				dynTrigger = trigger * 0.9
			}
			if ingestScore >= dynTrigger {
				pri := compactionPriority{
					level:       i,
					score:       ingestScore * 0.8,
					adjusted:    ingestScore * 0.8,
					t:           t,
					ingestOnly:  true,
					ingestMerge: true,
					statsTag:    "ingest-merge",
				}
				prios = append(prios, pri)
			}
			continue
		}
		// 处于压缩状态的sst 不能计算在内
		delSize := lm.compactState.delSize(i)
		sz := lvl.getTotalSize() - delSize
		// score的计算是 扣除正在合并的表后的尺寸与目标sz的比值
		addPriority(i, float64(sz)/float64(t.targetSz[i]), false, false)
	}
	// 调整得分
	var prevLevel int
	for level := t.baseLevel; level < len(lm.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			// 避免过大的得分
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// 仅选择得分大于1的压缩内容，并且允许l0到l0的特殊压缩，为了提升查询性能允许l0层独自压缩
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	for _, p := range extras {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// 按优先级排序
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}
func (lm *levelManager) lastLevel() *levelHandler {
	return lm.levels[len(lm.levels)-1]
}

// levelTargets
func (lm *levelManager) levelTargets() targets {
	adjust := func(sz int64) int64 {
		if sz < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return sz
	}

	// 初始化默认都是最大层级
	t := targets{
		targetSz: make([]int64, len(lm.levels)),
		fileSz:   make([]int64, len(lm.levels)),
	}
	// 从最后一个level开始计算
	dbSize := lm.lastLevel().getTotalSize()
	for i := len(lm.levels) - 1; i > 0; i-- {
		leveTargetSize := adjust(dbSize)
		t.targetSz[i] = leveTargetSize
		// 如果当前的level没有达到合并的要求
		if t.baseLevel == 0 && leveTargetSize <= lm.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(lm.opt.LevelSizeMultiplier)
	}

	tsz := lm.opt.BaseTableSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			// l0选择memtable的size作为文件的尺寸
			t.fileSz[i] = lm.opt.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	// 找到最后一个空level作为目标level实现跨level归并，减少写放大
	for i := t.baseLevel + 1; i < len(lm.levels)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	// 如果存在断层，则目标level++
	b := t.baseLevel
	lvl := lm.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

type thisAndNextLevelRLocked struct{}

func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	useIngestFallback := false
	if len(tables) == 0 {
		if cd.thisLevel.isLastLevel() && cd.thisLevel.numIngestTables() > 0 {
			tables = append(tables, cd.thisLevel.ingest.allTables()...)
			useIngestFallback = true
		} else {
			return false
		}
	}
	if useIngestFallback {
		cd.top = tables
		cd.bot = nil
		cd.ingestMerge = true
		cd.thisRange = getKeyRange(cd.top...)
		cd.nextRange = cd.thisRange
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			return false
		}
		return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
	}
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}
	// We pick tables, so we compact older tables first. This is similar to
	// kOldestLargestSeqFirst in RocksDB.
	lm.sortByHeuristic(tables, cd)

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// 如果被压缩过了，则什么都不需要做
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}
		cd.top = []*table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)

		cd.bot = make([]*table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		if len(cd.bot) == 0 {
			cd.bot = []*table{}
			cd.nextRange = cd.thisRange
			if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

		if lm.compactState.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
			continue
		}
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

func (lm *levelManager) fillTablesIngest(cd *compactDef) bool {
	return lm.fillTablesIngestShard(cd, -1)
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
	sh := cd.thisLevel.ingest.shardByIndex(shardIdx)
	if sh == nil || len(sh.tables) == 0 {
		return false
	}
	// Adaptive batch: scale up when shard backlog is large to drain faster.
	if denom := cd.targetFileSize(); denom > 0 {
		score := float64(sh.size) / float64(denom)
		if score > 1.0 {
			boost := int(math.Ceil(score))
			batchSize = min(len(sh.tables), batchSize*boost)
		}
	}
	top := sh.tables
	if batchSize > 0 && batchSize < len(top) {
		top = top[:batchSize]
	}
	cd.top = top
	cd.thisSize = 0
	for _, t := range cd.top {
		if t == nil {
			continue
		}
		cd.thisSize += t.Size()
	}
	cd.thisRange = getKeyRange(cd.top...)

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	if right > left {
		cd.bot = make([]*table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])
	} else {
		cd.bot = []*table{}
	}
	cd.nextRange = cd.thisRange
	if len(cd.bot) > 0 {
		cd.nextRange.extend(getKeyRange(cd.bot...))
	}

	if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
		return false
	}
	if len(cd.bot) > 0 && lm.compactState.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
		return false
	}
	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// compact older tables first.
func (lm *levelManager) sortByHeuristic(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	// Sort tables by max version. This is what RocksDB does.
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].MaxVersionVal() < tables[j].MaxVersionVal()
	})
}
func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	utils.CondPanic(len(cd.splits) != 0, errors.New("len(cd.splits) != 0"))
	if thisLevel == nextLevel {
		// l0 to l0 和 lmax to lmax 不做特殊处理
	} else {
		lm.addSplits(&cd)
	}
	// 追加一个空的
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	newTables, decr, err := lm.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables)

	// 更新 manifest
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
					Ingest:    cd.ingestMerge,
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

	defer decrRefs(cd.top)
	if cd.ingestMerge {
		if err := thisLevel.deleteIngestTables(cd.top); err != nil {
			return err
		}
		thisLevel.ingest.addBatch(newTables)
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
	if cd.ingestOnly {
		tablesCompacted := len(cd.top) + len(cd.bot)
		cd.thisLevel.recordIngestMetrics(cd.ingestMerge, time.Since(timeStart), tablesCompacted)
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

// compactBuildTables 合并两个层的sst文件
func (lm *levelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {

	topTables := cd.top
	botTables := cd.bot
	iterOpt := &utils.Options{
		IsAsc:            true,
		AccessPattern:    utils.AccessPatternSequential,
		ZeroCopy:         true,
		PrefetchBlocks:   1,
		BypassBlockCache: true,
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

	// 开始并行执行压缩过程
	res := make(chan *table, 3)
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

	// mapreduce的方式收集table的句柄
	var newTables []*table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()

	// 在这里等待所有的压缩过程完成
	err := inflightBuilders.Finish()
	// channel 资源回收
	close(res)
	// 等待所有的builder刷到磁盘
	wg.Wait()

	if err == nil {
		// 同步刷盘，保证数据一定落盘
		err = utils.SyncDir(lm.opt.WorkDir)
	}

	if err != nil {
		// 如果出现错误，则删除索引新创建的文件
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return utils.CompareKeys(newTables[i].MaxKey(), newTables[j].MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

// 并行的运行子压缩情况
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte) {
		skr.right = slices.Clone(right)
		cd.splits = append(cd.splits, skr)
		skr.left = skr.right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			// 设置最大值为右区间
			right := kv.KeyWithTs(kv.ParseKey(t.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}
}

// sortByStaleData 对表中陈旧数据的数量对sst文件进行排序
func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	// TODO 统计一个 sst文件中陈旧数据的数量，涉及对存储格式的修改
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// max level 和 max level 的压缩
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		// This is a maxLevel to maxLevel compaction and we don't have any stale data.
		return false
	}
	cd.bot = []*table{}
	collectBotTables := func(t *table, needSz int64) {
		totalSize := t.Size()

		j := sort.Search(len(tables), func(i int) bool {
			return utils.CompareKeys(tables[i].MinKey(), t.MinKey()) >= 0
		})
		utils.CondPanic(tables[j].fid != t.fid, errors.New("tables[j].ID() != t.ID()"))
		j++
		// Collect tables until we reach the the required size.
		for j < len(tables) {
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newT)
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		if now.Sub(*t.GetCreatedAt()) < time.Hour {
			// Just created it an hour ago. Don't pick for compaction.
			continue
		}
		// If the stale data size is less than 10 MB, it might not be worth
		// rewriting the table. Skip it.
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// Set the next range as the same as the current range. If we don't do
		// this, we won't be able to run more than one max level compactions.
		cd.nextRange = cd.thisRange
		// If we're already compacting this range, don't do anything.
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		// Found a valid table!
		cd.top = []*table{t}

		needFileSz := cd.t.fileSz[cd.thisLevel.levelNum]
		// 如果合并的sst size需要的文件尺寸直接终止
		if t.Size() >= needFileSz {
			break
		}
		// TableSize is less than what we want. Collect more tables for compaction.
		// If the level has multiple small tables, we collect all of them
		// together to form a bigger table.
		collectBotTables(t, needFileSz)
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}
	if len(cd.top) == 0 {
		return false
	}

	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0 先尝试从l0 到lbase的压缩，如果失败则对l0自己压缩
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
	cd.thisLevel.Lock()
	var remaining []*table
	for _, tbl := range cd.thisLevel.tables {
		if _, found := toDel[tbl.fid]; found {
			cd.thisLevel.subtractSize(tbl)
			continue
		}
		remaining = append(remaining, tbl)
	}
	cd.thisLevel.tables = remaining
	cd.thisLevel.Unlock()

	cd.nextLevel.addIngestBatch(cd.top)
	if cd.nextLevel.levelNum > 0 {
		cd.nextLevel.Sort()
	}
	if lm.compaction != nil {
		lm.compaction.trigger("ingest-buffer")
	}
	return nil
}

func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("base level can be zero"))
	}
	// 如果优先级低于1 则不执行
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table
	var kr keyRange
	// cd.top[0] 是最老的文件，从最老的文件开始
	for _, t := range top {
		dkr := getKeyRange(t)
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			// 如果有任何一个不重合的区间存在则直接终止
			break
		}
	}
	// 获取目标range list 的全局 range 对象
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0ToL0 l0到l0压缩
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		// 只要0号压缩处理器可以执行，避免l0tol0的资源竞争
		return false
	}

	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	// We intentionally avoid calling compactDef.lockLevels here. Both thisLevel and nextLevel
	// point at L0, so grabbing the RLock twice would violate RWMutex semantics and can deadlock
	// once another goroutine attempts a write lock. Taking the shared lock exactly once matches
	// Badger's approach and keeps lock acquisition order (level -> compactState) consistent.
	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	lm.compactState.Lock()
	defer lm.compactState.Unlock()

	top := cd.thisLevel.tables
	var out []*table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// 在L0 to L0 的压缩过程中，不要对过大的sst文件压缩，这会造成性能抖动
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			// 如果sst的创建时间不足10s 也不要回收
			continue
		}
		// 如果当前的sst 已经在压缩状态 也应该忽略
		if _, beingCompacted := lm.compactState.tables[t.fid]; beingCompacted {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		// 满足条件的sst小于4个那就不压缩了
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// 在这个过程中避免任何l0到其他层的合并
	thisLevel := lm.compactState.levels[cd.thisLevel.levelNum]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		lm.compactState.tables[t.fid] = struct{}{}
	}

	//  l0 to l0的压缩最终都会压缩为一个文件，这大大减少了l0层文件数量，减少了读放大
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

// getKeyRange 返回一组sst的区间合并后的最大与最小值
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
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
	return keyRange{
		left:  kv.KeyWithTs(kv.ParseKey(minKey), math.MaxUint64),
		right: kv.KeyWithTs(kv.ParseKey(maxKey), 0),
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
func (lm *levelManager) updateDiscardStats(discardStats map[uint32]int64) {
	select {
	case *lm.lsm.option.DiscardStatsCh <- discardStats:
	default:
	}
}

// 真正执行并行压缩的子压缩文件
func (lm *levelManager) subcompact(it utils.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *table) {
	var lastKey []byte
	// 更新 discardStats
	discardStats := make(map[uint32]int64)
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
			discardStats[vp.Fid] += int64(math.Round(weighted))
		}
	}
	addKeys := func(builder *tableBuilder) {
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			key := it.Item().Entry().Key
			//version := kv.ParseTs(key)
			isExpired := IsDeletedOrExpired(it.Item().Entry())
			if !kv.SameKey(key, lastKey) {
				// 如果迭代器返回的key大于当前key的范围就不用执行了
				if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// 如果超过预估的sst文件大小，则直接结束
					break
				}
				// 把当前的key变为 lastKey
				lastKey = kv.SafeCopy(lastKey, key)
				//umVersions = 0
				// 如果左边界没有，则当前key给到左边界
				if len(tableKr.left) == 0 {
					tableKr.left = kv.SafeCopy(tableKr.left, key)
				}
				// 更新右边界
				tableKr.right = lastKey
			}
			// 判断是否是过期内容，是的话就删除
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

	//如果 key range left还存在 则seek到这里 说明遍历中途停止了
	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		//
		it.Rewind()
	}
	for it.Valid() {
		key := it.Item().Entry().Key
		if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
			break
		}
		// 为构建任务拷贝一份 Options，避免后台调整与运行中 compaction 互相影响。
		builderOpt := lm.opt.Clone()
		builder := newTableBuilerWithSSTSize(builderOpt, cd.t.fileSz[cd.nextLevel.levelNum])

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
		// 充分发挥 ssd的并行 写入特性
		go func(builder *tableBuilder) {
			defer inflightBuilders.Done(nil)
			defer builder.Close()
			var tbl *table
			newFID := atomic.AddUint64(&lm.maxFID, 1) // compact的时候是没有memtable的，这里自增maxFID即可。
			sstName := utils.FileNameSSTable(lm.opt.WorkDir, newFID)
			tbl = openTable(lm, sstName, builder)
			if tbl == nil {
				return
			}
			res <- tbl
		}(builder)
	}
}

// checkOverlap 检查是否与下一层存在重合
func (lm *levelManager) checkOverlap(tables []*table, lev int) bool {
	kr := getKeyRange(tables...)
	for i, lh := range lm.levels {
		if i < lev { // Skip upper levels.
			continue
		}
		lh.RLock()
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

// 判断是否过期 是可删除
func IsDeletedOrExpired(e *kv.Entry) bool {
	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// compactStatus
type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

func (lsm *LSM) newCompactStatus() *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < lsm.option.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}

func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}

func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum

	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	thisLevel.delSize -= cd.thisSize
	found := thisLevel.remove(cd.thisRange)
	// The following check makes sense only if we're compacting more than one
	// table. In case of the max level, we might rewrite a single table to
	// remove stale data.
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevel.remove(cd.nextRange) && found
	}

	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: %s in this level %d.\n", this, tl)
		fmt.Printf("This Level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", next, cd.nextLevel.levelNum)
		fmt.Printf("Next Level:\n%s\n", nextLevel.debug())
		log.Fatal("keyRange not found")
	}
	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.fid]
		utils.CondPanic(!ok, fmt.Errorf("cs.tables is nil"))
		delete(cs.tables, t.fid)
	}
}

func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum
	utils.CondPanic(tl >= len(cs.levels), fmt.Errorf("Got level %d. Max levels: %d", tl, len(cs.levels)))
	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}
	// Check whether this level really needs compaction or not. Otherwise, we'll end up
	// running parallel compactions for the same level.
	// Update: We should not be checking size here. Compaction priority already did the size checks.
	// Here we should just be executing the wish of others.

	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.fid] = struct{}{}
	}
	return true
}

// levelCompactStatus
type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}
func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

// keyRange
type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64 // size is used for Key splits.
}

func (r keyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.inf
}

var infRange = keyRange{inf: true}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.left, r.right, r.inf)
}

func (r keyRange) equals(dst keyRange) bool {
	return bytes.Equal(r.left, dst.left) &&
		bytes.Equal(r.right, dst.right) &&
		r.inf == dst.inf
}

func (r *keyRange) extend(kr keyRange) {
	if kr.isEmpty() {
		return
	}
	if r.isEmpty() {
		*r = kr
	}
	if len(r.left) == 0 || utils.CompareKeys(kr.left, r.left) < 0 {
		r.left = kr.left
	}
	if len(r.right) == 0 || utils.CompareKeys(kr.right, r.right) > 0 {
		r.right = kr.right
	}
	if kr.inf {
		r.inf = true
	}
}

func (r keyRange) overlapsWith(dst keyRange) bool {
	// Empty keyRange always overlaps.
	if r.isEmpty() {
		return true
	}
	// Empty dst doesn't overlap with anything.
	if dst.isEmpty() {
		return false
	}
	if r.inf || dst.inf {
		return true
	}

	// [dst.left, dst.right] ... [r.left, r.right]
	// If my left is greater than dst right, we have no overlap.
	if utils.CompareKeys(r.left, dst.right) > 0 {
		return false
	}
	// [r.left, r.right] ... [dst.left, dst.right]
	// If my right is less than dst left, we have no overlap.
	if utils.CompareKeys(r.right, dst.left) < 0 {
		return false
	}
	// We have overlap.
	return true
}
