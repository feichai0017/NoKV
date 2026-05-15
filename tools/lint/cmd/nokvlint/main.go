// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Command nokvlint runs NoKV's repository-specific code-contract analyzers as
// a standalone multichecker. golangci-lint loads the same analyzers through
// the module plugin at the repository root; this binary exists for local
// invocation, IDE integration, and CI fallback.
package main

import (
	"golang.org/x/tools/go/analysis/multichecker"

	"github.com/feichai0017/NoKV/tools/lint/analyzers/catchallfile"
	"github.com/feichai0017/NoKV/tools/lint/analyzers/errstringmatch"
	"github.com/feichai0017/NoKV/tools/lint/analyzers/forwardingwrapper"
	"github.com/feichai0017/NoKV/tools/lint/analyzers/generatedheader"
	"github.com/feichai0017/NoKV/tools/lint/analyzers/importboundary"
	"github.com/feichai0017/NoKV/tools/lint/analyzers/mapanyboundary"
	"github.com/feichai0017/NoKV/tools/lint/analyzers/metricsplacement"
	"github.com/feichai0017/NoKV/tools/lint/analyzers/pkgdoc"
	"github.com/feichai0017/NoKV/tools/lint/analyzers/sentinelerrors"
	"github.com/feichai0017/NoKV/tools/lint/analyzers/validatepurity"
)

func main() {
	multichecker.Main(
		catchallfile.Analyzer,
		sentinelerrors.Analyzer,
		importboundary.Analyzer,
		generatedheader.Analyzer,
		pkgdoc.Analyzer,
		metricsplacement.Analyzer,
		mapanyboundary.Analyzer,
		errstringmatch.Analyzer,
		validatepurity.Analyzer,
		forwardingwrapper.Analyzer,
	)
}
