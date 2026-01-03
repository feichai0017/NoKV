package compact

import (
	"math"
	"sort"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

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
		if utils.CompareKeys(tables[i].MinKey, minKey) < 0 {
			minKey = tables[i].MinKey
		}
		if utils.CompareKeys(tables[i].MaxKey, maxKey) > 0 {
			maxKey = tables[i].MaxKey
		}
	}
	return KeyRange{
		Left:  kv.KeyWithTs(kv.ParseKey(minKey), math.MaxUint64),
		Right: kv.KeyWithTs(kv.ParseKey(maxKey), 0),
	}
}

// OverlappingTables returns the half-interval of tables overlapping with kr.
func OverlappingTables(tables []TableMeta, kr KeyRange) (int, int) {
	if len(kr.Left) == 0 || len(kr.Right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(tables), func(i int) bool {
		return utils.CompareKeys(kr.Left, tables[i].MaxKey) <= 0
	})
	right := sort.Search(len(tables), func(i int) bool {
		return utils.CompareKeys(kr.Right, tables[i].MaxKey) < 0
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
		return utils.CompareKeys(tables[i].MinKey, seed.MinKey) >= 0
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
