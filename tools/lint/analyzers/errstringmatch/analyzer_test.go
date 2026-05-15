// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package errstringmatch_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/feichai0017/NoKV/tools/lint/analyzers/errstringmatch"
)

func TestErrErrorComparisonIsReported(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), errstringmatch.Analyzer, "bad")
}

func TestStructuredErrorBranchesAreClean(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), errstringmatch.Analyzer, "ok")
}
