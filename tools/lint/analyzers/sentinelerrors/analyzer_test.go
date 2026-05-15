// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package sentinelerrors_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/feichai0017/NoKV/tools/lint/analyzers/sentinelerrors"
)

func TestScatteredSentinelIsReported(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), sentinelerrors.Analyzer, "scattered")
}

func TestCanonicalErrorsFileIsClean(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), sentinelerrors.Analyzer, "canonical")
}
