package lsm

import "github.com/feichai0017/NoKV/lsm/compact"

// adjustThrottle updates write admission state using a two-stage model:
// slowdown (pace writes) and stop (block writes). Hysteresis is applied to
// avoid oscillation under heavy compaction pressure.
func (lm *levelManager) adjustThrottle() {
	if lm == nil || lm.lsm == nil || len(lm.levels) == 0 {
		return
	}
	l0Tables := lm.levels[0].numTables()
	_, maxScore := lm.compactionStats()

	l0Slow := lm.opt.L0SlowdownWritesTrigger
	l0Stop := lm.opt.L0StopWritesTrigger
	l0Resume := lm.opt.L0ResumeWritesTrigger

	scoreSlow := lm.opt.CompactionSlowdownTrigger
	scoreStop := lm.opt.CompactionStopTrigger
	scoreResume := lm.opt.CompactionResumeTrigger

	stopCond := l0Tables >= l0Stop
	slowCond := l0Tables >= l0Slow || maxScore >= scoreSlow
	resumeCond := l0Tables <= l0Resume && maxScore <= scoreResume

	cur := lm.lsm.ThrottleState()
	target := cur
	switch cur {
	case WriteThrottleStop:
		if stopCond {
			target = WriteThrottleStop
		} else if slowCond {
			target = WriteThrottleSlowdown
		} else if resumeCond {
			target = WriteThrottleNone
		}
	case WriteThrottleSlowdown:
		if stopCond {
			target = WriteThrottleStop
		} else if resumeCond {
			target = WriteThrottleNone
		}
	default:
		if stopCond {
			target = WriteThrottleStop
		} else if slowCond {
			target = WriteThrottleSlowdown
		} else {
			target = WriteThrottleNone
		}
	}
	l0Pressure := normalizedThrottlePressure(float64(l0Tables), float64(l0Slow), float64(l0Stop))
	scorePressure := normalizedThrottlePressure(maxScore, scoreSlow, scoreStop)
	pressure := max(l0Pressure, scorePressure)
	switch target {
	case WriteThrottleNone:
		pressure = 0
	case WriteThrottleStop:
		pressure = 1000
	case WriteThrottleSlowdown:
		if pressure == 0 {
			pressure = 1
		}
	}
	rate := uint64(0)
	if target == WriteThrottleSlowdown {
		rate = throttleRateForPressure(
			uint32(pressure),
			lm.opt.WriteThrottleMinRate,
			lm.opt.WriteThrottleMaxRate,
		)
	}
	lm.lsm.throttleWrites(target, uint32(pressure), rate)
}

func normalizedThrottlePressure(value, slowdown, stop float64) int {
	if stop <= slowdown {
		if value >= stop {
			return 1000
		}
		return 0
	}
	if value <= slowdown {
		return 0
	}
	if value >= stop {
		return 1000
	}
	ratio := (value - slowdown) / (stop - slowdown)
	if ratio <= 0 {
		return 0
	}
	if ratio >= 1 {
		return 1000
	}
	return int(ratio*1000 + 0.5)
}

func throttleRateForPressure(pressure uint32, minRate, maxRate int64) uint64 {
	if pressure == 0 || maxRate <= 0 {
		return 0
	}
	if minRate <= 0 {
		minRate = maxRate
	}
	if maxRate < minRate {
		maxRate = minRate
	}
	ratio := float64(pressure) / 1000
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	curve := ratio * ratio
	rate := float64(maxRate) - (float64(maxRate-minRate) * curve)
	if rate < float64(minRate) {
		rate = float64(minRate)
	}
	return uint64(rate + 0.5)
}

// needsCompaction reports whether any level currently exceeds compaction thresholds.
func (lm *levelManager) needsCompaction() bool {
	return len(lm.pickCompactLevels()) > 0
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
	if lm.hotProvider != nil {
		hotKeys = lm.hotProvider()
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
