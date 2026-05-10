package plan

// Targets describes the compaction size targets for each level.
type Targets struct {
	BaseLevel int
	TargetSz  []int64
	FileSz    []int64
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

// FileSizeForLevel returns the target file size for the given level, falling
// back to the level target size when the per-file target is unset.
func (t Targets) FileSizeForLevel(level int) int64 {
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
