// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package lint

import (
	"github.com/golangci/plugin-module-register/register"
	"golang.org/x/tools/go/analysis"

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

func init() {
	register.Plugin("nokvcontract", newPlugin)
}

func newPlugin(any) (register.LinterPlugin, error) {
	return contractPlugin{}, nil
}

type contractPlugin struct{}

func (contractPlugin) BuildAnalyzers() ([]*analysis.Analyzer, error) {
	return []*analysis.Analyzer{
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
	}, nil
}

func (contractPlugin) GetLoadMode() string {
	return register.LoadModeTypesInfo
}
