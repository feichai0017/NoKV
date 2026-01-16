package compact

import (
	"math"
	"sort"
	"strings"
)

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
	IngestTables       int
	IngestSize         int64
	IngestValueBytes   int64
	IngestValueDensity float64
	IngestAgeSeconds   float64
	HotOverlap         float64
	HotOverlapIngest   float64
	DelSize            int64
}

// PickerInput captures the inputs needed for compaction picking.
type PickerInput struct {
	Levels                  []LevelInput
	Targets                 Targets
	NumLevelZeroTables      int
	BaseTableSize           int64
	BaseLevelSize           int64
	IngestBacklogMergeScore float64
	CompactionValueWeight   float64
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
		hotScore := lvlHotOverlap(in.Levels[level], ingest)
		if hotScore > 0 {
			pri.Score += hotScore
			pri.Adjusted += hotScore * 2
			if !strings.Contains(pri.StatsTag, "hot") {
				pri.StatsTag = "hot-" + pri.StatsTag
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

func lvlHotOverlap(lvl LevelInput, ingest bool) float64 {
	if ingest {
		return lvl.HotOverlapIngest
	}
	return lvl.HotOverlap
}
