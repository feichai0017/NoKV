// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package catchallfile flags catch-all file names that NoKV's code contract
// forbids. The rule is sourced from docs/guide/development/code_contract.md §5:
// files must be responsibility-named, not catch-all dump buckets.
package catchallfile

import (
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports any non-test source file whose base name is one of the
// catch-all names banned by code_contract.md §5.
var Analyzer = &analysis.Analyzer{
	Name: "catchallfile",
	Doc:  "flag catch-all file names (utils.go, helpers.go, common.go, misc.go) banned by code_contract.md §5",
	Run:  run,
}

var banned = map[string]struct{}{
	"utils.go":   {},
	"helpers.go": {},
	"common.go":  {},
	"misc.go":    {},
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		base := filepath.Base(pass.Fset.Position(file.Pos()).Filename)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}
		if _, bad := banned[base]; !bad {
			continue
		}
		pass.Report(analysis.Diagnostic{
			Pos:     file.Pos(),
			End:     file.End(),
			Message: "code_contract §5: file " + base + " is a catch-all bucket; rename it to describe its responsibility (e.g. encode.go, validation.go, lifecycle.go)",
		})
	}
	return nil, nil
}
