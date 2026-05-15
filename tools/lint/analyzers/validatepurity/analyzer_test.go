// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package validatepurity_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/feichai0017/NoKV/tools/lint/analyzers/validatepurity"
)

func TestMutatingValidateIsReported(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), validatepurity.Analyzer, "mutating")
}

func TestPureValidateIsClean(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), validatepurity.Analyzer, "pure")
}
