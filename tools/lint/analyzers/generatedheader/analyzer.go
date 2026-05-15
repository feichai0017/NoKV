// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package generatedheader enforces code_contract.md §10: generated source
// files must carry the canonical "Code generated ... DO NOT EDIT." header so
// reviewers and tooling can recognise them at a glance.
package generatedheader

import (
	"go/ast"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports generated files whose canonical header is missing.
var Analyzer = &analysis.Analyzer{
	Name: "generatedheader",
	Doc:  "generated files (*.pb.go, *.peras.go, *.gen.go) must carry the 'Code generated ... DO NOT EDIT.' header (code_contract §10)",
	Run:  run,
}

var generatedSuffixes = []string{
	".pb.go",
	".peras.go",
	".gen.go",
	"_generated.go",
}

var generatedHeaderPattern = regexp.MustCompile(`Code generated .* DO NOT EDIT\.`)

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		base := filepath.Base(pass.Fset.Position(file.Pos()).Filename)
		if !looksGenerated(base) {
			continue
		}
		if hasGeneratedHeader(file) {
			continue
		}
		pass.Report(analysis.Diagnostic{
			Pos:     file.Pos(),
			End:     file.End(),
			Message: "code_contract §10: generated file " + base + " is missing the 'Code generated ... DO NOT EDIT.' header",
		})
	}
	return nil, nil
}

func looksGenerated(name string) bool {
	for _, suffix := range generatedSuffixes {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

func hasGeneratedHeader(file *ast.File) bool {
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if generatedHeaderPattern.MatchString(comment.Text) {
				return true
			}
		}
	}
	return false
}
