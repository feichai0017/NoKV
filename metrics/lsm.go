// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics

// LevelMetrics captures aggregated statistics for a single LSM level.
type LevelMetrics struct {
	Level                  int
	TableCount             int
	SizeBytes              int64
	ValueBytes             int64
	StaleBytes             int64
	LandingTableCount      int
	LandingSizeBytes       int64
	LandingValueBytes      int64
	ValueDensity           float64
	LandingValueDensity    float64
	LandingRuns            int64
	LandingMs              float64
	LandingTablesCompacted int64
	LandingMergeRuns       int64
	LandingMergeMs         float64
	LandingMergeTables     int64
}
