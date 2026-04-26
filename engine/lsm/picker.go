package lsm

import (
	"math"
	"sort"
)

// needsCompaction reports whether any level currently exceeds compaction thresholds.
func (lm *levelManager) needsCompaction() bool {
	return len(lm.pickCompactLevels()) > 0
}

// pickCompactLevels chooses compaction candidates and returns priorities.
func (lm *levelManager) pickCompactLevels() (prios []Priority) {
	input := lm.buildPickerInput()
	if len(input.Levels) == 0 {
		return nil
	}
	return PickPriorities(input)
}

func (lm *levelManager) buildPickerInput() PickerInput {
	if lm == nil || lm.opt == nil {
		return PickerInput{}
	}
	levels := make([]LevelInput, len(lm.levels))
	for i, lvl := range lm.levels {
		if lvl == nil {
			continue
		}
		li := LevelInput{
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
			KeyCount:           lvl.keyCount(),
			RangeTombstones:    lvl.rangeTombstoneCount(),
		}
		if lm.compactState != nil {
			li.DelSize = lm.compactState.DelSize(i)
		}
		levels[i] = li
	}
	return PickerInput{
		Levels:                    levels,
		Targets:                   lm.levelTargets(),
		NumLevelZeroTables:        lm.opt.NumLevelZeroTables,
		BaseTableSize:             lm.opt.BaseTableSize,
		BaseLevelSize:             lm.opt.BaseLevelSize,
		IngestBacklogMergeScore:   lm.opt.IngestBacklogMergeScore,
		CompactionValueWeight:     lm.opt.CompactionValueWeight,
		CompactionTombstoneWeight: lm.opt.CompactionTombstoneWeight,
	}
}

// levelTargets
func (lm *levelManager) levelTargets() Targets {
	if lm == nil || lm.opt == nil || len(lm.levels) == 0 {
		return Targets{}
	}
	return BuildTargets(lm.levelSizes(), TargetOptions{
		BaseLevelSize:       lm.opt.BaseLevelSize,
		LevelSizeMultiplier: lm.opt.LevelSizeMultiplier,
		BaseTableSize:       lm.opt.BaseTableSize,
		TableSizeMultiplier: lm.opt.TableSizeMultiplier,
		MemTableSize:        lm.opt.MemTableSize,
	})
}

func (lm *levelManager) targetFileSizeForLevel(t Targets, level int) int64 {
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

// Targets describes the compaction size targets for each level.
type Targets struct {
	BaseLevel int
	TargetSz  []int64
	FileSz    []int64
}

// Priority describes a single compaction candidate.
type Priority struct {
	Level        int
	Score        float64
	Adjusted     float64
	DropPrefixes [][]byte
	Target       Targets
	IngestMode   IngestMode
	StatsTag     string
}

// ApplyValueWeight boosts the priority based on value log density.
func (cp *Priority) ApplyValueWeight(weight, valueScore float64) {
	if weight <= 0 || valueScore <= 0 {
		return
	}
	capped := math.Min(valueScore, 16)
	cp.Score += weight * capped
	cp.Adjusted = cp.Score
}

// MoveL0ToFront ensures the first priority is for L0 if one exists.
func MoveL0ToFront(prios []Priority) []Priority {
	idx := -1
	for i, p := range prios {
		if p.Level == 0 {
			idx = i
			break
		}
	}
	if idx > 0 {
		out := append([]Priority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

// IngestMode describes how a compaction interacts with ingest tables.
type IngestMode uint8

const (
	// IngestNone indicates a regular compaction using main tables only.
	IngestNone IngestMode = iota
	// IngestDrain compacts ingest tables and writes output into main tables.
	IngestDrain
	// IngestKeep compacts ingest tables and keeps output in ingest buffers.
	IngestKeep
)

func (m IngestMode) UsesIngest() bool {
	return m != IngestNone
}

func (m IngestMode) KeepsIngest() bool {
	return m == IngestKeep
}

// IngestShardView is a lightweight view of an ingest shard for strategy decisions.
type IngestShardView struct {
	Index        int
	TableCount   int
	SizeBytes    int64
	ValueBytes   int64
	MaxAgeSec    float64
	ValueDensity float64
}

// IngestPickInput bundles inputs for ingest shard picking.
type IngestPickInput struct {
	Shards []IngestShardView
}

// PickShardOrder returns shard indices sorted by backlog size (largest first).
func PickShardOrder(in IngestPickInput) []int {
	if len(in.Shards) == 0 {
		return nil
	}
	shards := append([]IngestShardView(nil), in.Shards...)
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].SizeBytes > shards[j].SizeBytes
	})
	out := make([]int, 0, len(shards))
	for _, sh := range shards {
		out = append(out, sh.Index)
	}
	return out
}

// PickShardByBacklog returns the shard index with the highest backlog score.
func PickShardByBacklog(in IngestPickInput) int {
	if len(in.Shards) == 0 {
		return -1
	}
	best := in.Shards[0]
	bestScore := backlogScore(best)
	for i := 1; i < len(in.Shards); i++ {
		score := backlogScore(in.Shards[i])
		if score > bestScore {
			best = in.Shards[i]
			bestScore = score
		}
	}
	return best.Index
}

func backlogScore(sh IngestShardView) float64 {
	score := float64(sh.SizeBytes)
	if sh.MaxAgeSec > 0 {
		score *= 1.0 + math.Min(sh.MaxAgeSec/60.0, 4.0)
	}
	if sh.ValueDensity > 0 {
		score *= 1.0 + math.Min(sh.ValueDensity, 1.0)
	}
	return score
}

// TargetOptions controls how level size targets are computed.
type TargetOptions struct {
	BaseLevelSize       int64
	LevelSizeMultiplier int
	BaseTableSize       int64
	TableSizeMultiplier int
	MemTableSize        int64
}

// BuildTargets computes compaction target sizes for each level.
func BuildTargets(levelSizes []int64, opt TargetOptions) Targets {
	adjust := func(sz int64) int64 {
		if sz < opt.BaseLevelSize {
			return opt.BaseLevelSize
		}
		return sz
	}

	t := Targets{
		TargetSz: make([]int64, len(levelSizes)),
		FileSz:   make([]int64, len(levelSizes)),
	}
	dbSize := int64(0)
	if len(levelSizes) > 0 {
		dbSize = levelSizes[len(levelSizes)-1]
	}
	for i := len(levelSizes) - 1; i > 0; i-- {
		levelTargetSize := adjust(dbSize)
		t.TargetSz[i] = levelTargetSize
		if t.BaseLevel == 0 && levelTargetSize <= opt.BaseLevelSize {
			t.BaseLevel = i
		}
		if opt.LevelSizeMultiplier > 0 {
			dbSize /= int64(opt.LevelSizeMultiplier)
		}
	}

	tsz := opt.BaseTableSize
	if tsz <= 0 {
		tsz = 1
	}
	for i := range levelSizes {
		if i == 0 {
			t.FileSz[i] = opt.MemTableSize
		} else if i <= t.BaseLevel {
			t.FileSz[i] = tsz
		} else {
			tsz *= int64(opt.TableSizeMultiplier)
			if tsz <= 0 {
				tsz = 1
			}
			t.FileSz[i] = tsz
		}
	}

	// Find the last empty level to reduce write amplification.
	for i := t.BaseLevel + 1; i < len(levelSizes)-1; i++ {
		if levelSizes[i] > 0 {
			break
		}
		t.BaseLevel = i
	}

	// If there is a gap, move base level up.
	b := t.BaseLevel
	if b < len(levelSizes)-1 && levelSizes[b] == 0 && levelSizes[b+1] < t.TargetSz[b+1] {
		t.BaseLevel++
	}
	return t
}

// LevelInput captures per-level metrics for compaction picking.
type LevelInput struct {
	Level              int
	NumTables          int
	TotalSize          int64
	TotalValueBytes    int64
	MainValueBytes     int64
	KeyCount           uint64
	RangeTombstones    uint64
	IngestTables       int
	IngestSize         int64
	IngestValueBytes   int64
	IngestValueDensity float64
	IngestAgeSeconds   float64
	DelSize            int64
}

// PickerInput captures the inputs needed for compaction picking.
type PickerInput struct {
	Levels                    []LevelInput
	Targets                   Targets
	NumLevelZeroTables        int
	BaseTableSize             int64
	BaseLevelSize             int64
	IngestBacklogMergeScore   float64
	CompactionValueWeight     float64
	CompactionTombstoneWeight float64
}

// PickPriorities returns compaction candidates ordered by priority.
func PickPriorities(in PickerInput) []Priority {
	if len(in.Levels) == 0 {
		return nil
	}
	prios := make([]Priority, len(in.Levels))
	var extras []Priority
	addPriority := func(level int, score float64, mode IngestMode) {
		pri := Priority{
			Level:      level,
			Score:      score,
			Adjusted:   score,
			Target:     in.Targets,
			IngestMode: mode,
			StatsTag:   "regular",
		}
		ingest := mode.UsesIngest()
		merge := mode.KeepsIngest()
		if in.CompactionValueWeight > 0 && level < len(in.Levels) {
			lvl := in.Levels[level]
			var valueBytes int64
			var target float64
			switch {
			case level == 0:
				valueBytes = lvl.TotalValueBytes
				target = float64(in.BaseLevelSize)
				if target <= 0 {
					target = float64(in.BaseTableSize)
				}
			case ingest:
				valueBytes = lvl.IngestValueBytes
				target = float64(in.Targets.FileSz[level])
				if target <= 0 {
					target = float64(in.BaseTableSize)
				}
				if target <= 0 {
					target = 1
				}
			default:
				valueBytes = lvl.MainValueBytes
				target = float64(in.Targets.TargetSz[level])
			}
			if target <= 0 {
				target = float64(in.BaseTableSize)
				if target <= 0 {
					target = 1
				}
			}
			valueScore := float64(valueBytes) / target
			if ingest && valueScore == 0 {
				valueScore = lvl.IngestValueDensity
			}
			pri.ApplyValueWeight(in.CompactionValueWeight, valueScore)
		}
		if in.CompactionTombstoneWeight > 0 && level < len(in.Levels) {
			lvl := in.Levels[level]
			if lvl.RangeTombstones > 0 && lvl.KeyCount > 0 {
				density := float64(lvl.RangeTombstones) / float64(lvl.KeyCount)
				pri.Score += in.CompactionTombstoneWeight * math.Min(density*4, 4)
				pri.Adjusted = pri.Score
			}
		}
		if merge {
			extras = append(extras, pri)
			return
		}
		prios[level] = pri
	}

	numL0 := in.NumLevelZeroTables
	if numL0 <= 0 {
		numL0 = 1
	}
	addPriority(0, float64(in.Levels[0].NumTables)/float64(numL0), IngestNone)

	for i := 1; i < len(in.Levels); i++ {
		lvl := in.Levels[i]
		if lvl.IngestTables > 0 {
			denom := in.Targets.FileSz[i]
			if denom <= 0 {
				denom = in.BaseTableSize
				if denom <= 0 {
					denom = 1
				}
			}
			ingestScore := float64(lvl.IngestSize) / float64(denom)
			if ingestScore < 1.0 {
				ingestScore = 1.0
			}
			ageSec := lvl.IngestAgeSeconds
			if ageSec > 0 {
				ageFactor := math.Min(ageSec/60.0, 4.0)
				ingestScore += ageFactor
			}
			addPriority(i, ingestScore+1.0, IngestDrain)
			trigger := in.IngestBacklogMergeScore
			if trigger <= 0 {
				trigger = 2.0
			}
			dynTrigger := trigger
			if ingestScore >= trigger*2 {
				dynTrigger = trigger * 0.8
			} else if ageSec > 120 {
				dynTrigger = trigger * 0.9
			}
			if ingestScore >= dynTrigger {
				pri := Priority{
					Level:      i,
					Score:      ingestScore * 0.8,
					Adjusted:   ingestScore * 0.8,
					Target:     in.Targets,
					IngestMode: IngestKeep,
					StatsTag:   "ingest-merge",
				}
				prios = append(prios, pri)
			}
			continue
		}
		sz := lvl.TotalSize - lvl.DelSize
		addPriority(i, float64(sz)/float64(in.Targets.TargetSz[i]), IngestNone)
	}

	var prevLevel int
	for level := in.Targets.BaseLevel; level < len(in.Levels); level++ {
		if prios[prevLevel].Adjusted >= 1 {
			const minScore = 0.01
			if prios[level].Score >= minScore {
				prios[prevLevel].Adjusted /= prios[level].Adjusted
			} else {
				prios[prevLevel].Adjusted /= minScore
			}
		}
		prevLevel = level
	}

	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.Score >= 1.0 {
			out = append(out, p)
		}
	}
	for _, p := range extras {
		if p.Score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	sort.Slice(prios, func(i, j int) bool {
		return prios[i].Adjusted > prios[j].Adjusted
	})
	return prios
}
