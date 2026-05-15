// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package generatedheader_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/feichai0017/NoKV/tools/lint/analyzers/generatedheader"
)

func TestGeneratedFileMissingHeaderIsReported(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), generatedheader.Analyzer, "missing")
}

func TestGeneratedFileWithHeaderIsClean(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), generatedheader.Analyzer, "clean")
}
