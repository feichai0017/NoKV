// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package plan

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
)

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
	LandingMode  LandingMode
	DropPrefixes [][]byte
	StatsTag     string
	// IntraLevel marks plans whose input lives entirely on a single level
	// (e.g. L0→L0). Such plans claim their input by table ID only and do
	// not register a key range — see StateEntry.IntraLevel.
	IntraLevel bool
}

// StateEntry creates a compaction state entry for this plan.
func (p Plan) StateEntry(thisSize int64) StateEntry {
	entry := StateEntry{
		ThisLevel:  p.ThisLevel,
		NextLevel:  p.NextLevel,
		ThisRange:  p.ThisRange,
		NextRange:  p.NextRange,
		ThisSize:   thisSize,
		IntraLevel: p.IntraLevel,
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

// ForLandingFallback builds a plan when only landing tables are available.
func ForLandingFallback(level int, tables []TableMeta) (Plan, bool) {
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

// ForRegular selects tables for a standard compaction.
func ForRegular(level int, tables []TableMeta, nextLevel int, next []TableMeta, state *State) (Plan, bool) {
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

// ForMaxLevel selects tables to rewrite stale data in the max level.
func ForMaxLevel(level int, tables []TableMeta, targetFileSize int64, state *State, now time.Time, ttlMinAge time.Duration) (Plan, bool) {
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
		if t.StaleSize == 0 {
			continue
		}
		if shouldTTLCompact(t, now, ttlMinAge) {
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
				StatsTag:  "ttl",
			}, true
		}
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

func shouldTTLCompact(t TableMeta, now time.Time, ttlMinAge time.Duration) bool {
	if ttlMinAge <= 0 || t.CreatedAt.IsZero() || t.StaleSize == 0 {
		return false
	}
	return !now.Before(t.CreatedAt) && now.Sub(t.CreatedAt) >= ttlMinAge
}

// ForLandingShard builds a plan for a single landing shard.
func ForLandingShard(level int, shardTables []TableMeta, nextLevel int, next []TableMeta, targetFileSize int64, batchSize int, state *State) (Plan, bool) {
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

// ForL0ToLbase builds a plan for L0 -> base level compaction.
func ForL0ToLbase(l0 []TableMeta, nextLevel int, next []TableMeta, state *State) (Plan, bool) {
	if len(l0) == 0 {
		return Plan{}, false
	}
	var out []TableMeta
	var kr KeyRange
	for _, t := range l0 {
		// Skip tables already claimed by a peer compactor (in-flight
		// L0→L0 or another L0→Lbase). With IntraLevel L0→L0 claims
		// by table ID only, so we must walk past those claims to find
		// an un-claimed contiguous-overlap group instead of bailing.
		if state != nil && state.HasTable(t.ID) {
			if len(out) > 0 {
				// A gap inside an in-progress accumulation breaks
				// the contiguous-overlap invariant; commit what we
				// have and stop.
				break
			}
			continue
		}
		dkr := RangeForTables([]TableMeta{t})
		if len(out) == 0 || kr.OverlapsWith(dkr) {
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

// L0ToL0MaxTablesPerWorker caps how many L0 tables a single worker grabs in
// one L0→L0 compaction. Lower than this and the merge-collapse benefit is
// too small; higher and one worker eats every available L0 SST and blocks
// peer workers. 8 matches RocksDB's `max_subcompactions` default and is
// enough to drop ~3 levels of write amplification per cycle.
const L0ToL0MaxTablesPerWorker = 8

// ForL0ToL0 builds a plan for L0 -> L0 compaction.
//
// Concurrent workers can each generate a non-conflicting plan: each call
// picks at most L0ToL0MaxTablesPerWorker tables that aren't already
// claimed by state.HasTable. The resulting Plan is marked IntraLevel so
// the state machine claims by table ID only — peer workers see those
// tables filtered out and a concurrent L0→Lbase is not blocked by a
// fictitious "InfRange" claim.
func ForL0ToL0(level int, tables []TableMeta, fileSize int64, state *State, now time.Time) (Plan, bool) {
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
		if len(out) >= L0ToL0MaxTablesPerWorker {
			break
		}
	}
	if len(out) < 4 {
		return Plan{}, false
	}
	return Plan{
		ThisLevel:  level,
		NextLevel:  level,
		TopIDs:     tableIDsFromMeta(out),
		ThisRange:  KeyRange{},
		NextRange:  KeyRange{},
		IntraLevel: true,
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
