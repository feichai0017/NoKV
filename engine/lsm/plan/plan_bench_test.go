// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package plan

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkPlanForL0ToL0Concurrent measures whether N concurrent workers
// can each get a non-conflicting L0→L0 plan. Pre-fix: only worker 0 ever
// got a plan (compactorId != 0 hard-coded reject) AND each L0→L0 wrote
// an InfRange entry that blocked any peer L0→Lbase. Post-fix: workers
// claim disjoint table sets via the IntraLevel state entry and don't
// register a key range.
func BenchmarkPlanForL0ToL0Concurrent(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			tables := makeL0PlannerBenchTables(64)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				state := NewState(8)
				var wg sync.WaitGroup
				wg.Add(workers)
				for range workers {
					go func() {
						defer wg.Done()
						p, ok := ForL0ToL0(0, tables, 0, state, time.Time{})
						if ok {
							_ = state.CompareAndAdd(LevelsLocked{}, p.StateEntry(0))
						}
					}()
				}
				wg.Wait()
			}
		})
	}
}

// BenchmarkPlanForL0ToLbaseUnderL0ToL0Pressure measures the cost of
// ForL0ToLbase scanning past in-flight L0→L0 claims to find a non-conflicting
// group. Pre-fix: an in-flight L0→L0 InfRange entry blocked ForL0ToLbase
// entirely (returned false immediately). Post-fix: ForL0ToLbase walks past
// claimed table IDs and returns a plan for the remaining tables.
func BenchmarkPlanForL0ToLbaseUnderL0ToL0Pressure(b *testing.B) {
	tables := makeL0PlannerBenchTables(64)
	lbase := []TableMeta{
		{ID: 9999, MinKey: ikey("a", 10), MaxKey: ikey("z", 1), Size: 32 << 20},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := NewState(8)
		// Simulate an in-flight L0→L0 holding the first 8 tables.
		holdPlan, _ := ForL0ToL0(0, tables, 0, state, time.Time{})
		_ = state.CompareAndAdd(LevelsLocked{}, holdPlan.StateEntry(0))
		// Measured op: ForL0ToLbase finds a non-conflicting group.
		_, _ = ForL0ToLbase(tables, 1, lbase, state)
	}
}

// BenchmarkPickPriorities measures the cost of the priority calculation under
// a representative multi-level input. The picker is called once per
// compaction tick so its cost shows up in steady-state CPU profiles.
func BenchmarkPickPriorities(b *testing.B) {
	targets := BuildTargets([]int64{0, 0, 100, 1000}, TargetOptions{
		BaseLevelSize:       10,
		LevelSizeMultiplier: 10,
		BaseTableSize:       4,
		TableSizeMultiplier: 2,
		MemTableSize:        8,
	})
	input := PickerInput{
		Levels: []LevelInput{
			{Level: 0, NumTables: 8, TotalValueBytes: 256},
			{Level: 1, LandingTables: 2, LandingSize: 256, LandingValueBytes: 64, LandingValueDensity: 1, LandingAgeSeconds: 300, MainValueBytes: 128, KeyCount: 1024, RangeTombstones: 16},
			{Level: 2, TotalSize: 800, KeyCount: 4096, MainValueBytes: 256},
			{Level: 3, TotalSize: 4096, KeyCount: 16384, MainValueBytes: 1024},
		},
		Targets:                   targets,
		NumLevelZeroTables:        4,
		BaseTableSize:             4,
		BaseLevelSize:             10,
		LandingBacklogMergeScore:  2.0,
		CompactionValueWeight:     1.0,
		CompactionTombstoneWeight: 0.5,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = PickPriorities(input)
	}
}

func makeL0PlannerBenchTables(n int) []TableMeta {
	out := make([]TableMeta, n)
	for i := range out {
		out[i] = TableMeta{
			ID:        uint64(i + 1),
			MinKey:    ikey(formatL0Key(i*2), 10),
			MaxKey:    ikey(formatL0Key(i*2+10), 1),
			Size:      8 << 20,
			CreatedAt: time.Time{},
		}
	}
	return out
}
