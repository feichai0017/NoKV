// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package importboundary

import (
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports every import that violates one of the Rules above. The
// diagnostic position is the offending import declaration so editors jump
// straight to the line that needs to change.
var Analyzer = &analysis.Analyzer{
	Name: "importboundary",
	Doc:  "package-import direction rules from code_contract.md §2 (ownership of truth)",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	if pass.Pkg == nil {
		return nil, nil
	}
	pkgPath := pass.Pkg.Path()
	matched := matchedRules(pkgPath)
	if len(matched) == 0 {
		return nil, nil
	}
	for _, file := range pass.Files {
		// Test files are intentionally out of scope. The contract talks about
		// the production import graph; tests routinely reach across boundaries
		// to construct cross-module fixtures and that pattern is fine. Match
		// the legacy go-list-based check that read Imports without TestImports.
		if strings.HasSuffix(pass.Fset.Position(file.Pos()).Filename, "_test.go") {
			continue
		}
		for _, imp := range file.Imports {
			if imp == nil || imp.Path == nil {
				continue
			}
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}
			for _, rule := range matched {
				for _, forbidden := range rule.Forbidden {
					if !PathMatches(path, forbidden) {
						continue
					}
					pass.Report(analysis.Diagnostic{
						Pos:     imp.Pos(),
						End:     imp.End(),
						Message: "code_contract §2: " + rule.Name + " forbids importing " + path,
					})
				}
			}
		}
	}
	return nil, nil
}

func matchedRules(pkgPath string) []Rule {
	out := make([]Rule, 0, len(Rules))
	for _, rule := range Rules {
		if !rule.MatchesPackage(pkgPath) || rule.IsExempt(pkgPath) {
			continue
		}
		out = append(out, rule)
	}
	return out
}
