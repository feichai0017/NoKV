// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package metricsplacement_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/feichai0017/NoKV/tools/lint/analyzers/metricsplacement"
)

func TestRecordXOutsideMetricsIsReported(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), metricsplacement.Analyzer, "scattered")
}

func TestRecordXInsideMetricsIsClean(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), metricsplacement.Analyzer, "canonical")
}
