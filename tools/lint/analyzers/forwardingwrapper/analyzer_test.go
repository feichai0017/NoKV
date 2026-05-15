// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package forwardingwrapper_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/feichai0017/NoKV/tools/lint/analyzers/forwardingwrapper"
)

func TestForwardingWrapperIsReported(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), forwardingwrapper.Analyzer, "wrap")
}

func TestForwardingOKMarkerSuppresses(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), forwardingwrapper.Analyzer, "markedok")
}

func TestNonForwardingMethodsAreClean(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), forwardingwrapper.Analyzer, "distinct")
}
