// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package catchallfile_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/feichai0017/NoKV/tools/lint/analyzers/catchallfile"
)

func TestCatchAllFileNamesAreReported(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), catchallfile.Analyzer, "banned")
}

func TestResponsibilityNamedFilesAreClean(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), catchallfile.Analyzer, "clean")
}
