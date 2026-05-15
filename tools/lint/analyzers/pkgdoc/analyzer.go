// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package pkgdoc enforces code_contract.md §2: every package must own a
// package-level documentation comment that names its responsibility. The
// comment may live in doc.go or above any `package` clause in the package —
// the analyzer is permissive about location.
package pkgdoc

import (
	"go/ast"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports packages whose every file has an empty or missing
// package-level doc comment.
var Analyzer = &analysis.Analyzer{
	Name: "pkgdoc",
	Doc:  "every package must carry a package-level doc comment (code_contract §2)",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	if pass.Pkg == nil {
		return nil, nil
	}
	// Tests are not packages with their own contract.
	if strings.HasSuffix(pass.Pkg.Path(), ".test") {
		return nil, nil
	}
	// `package main` is a binary entry point; the contract section is the CLI
	// usage, not a Go-level package summary. Exempt to avoid documentation
	// noise on every cmd/ subtree.
	if pass.Pkg.Name() == "main" {
		return nil, nil
	}
	var firstFile *ast.File
	for _, file := range pass.Files {
		base := filepath.Base(pass.Fset.Position(file.Pos()).Filename)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}
		if firstFile == nil {
			firstFile = file
		}
		if file.Doc != nil && strings.TrimSpace(file.Doc.Text()) != "" {
			return nil, nil
		}
	}
	if firstFile == nil {
		return nil, nil
	}
	pass.Report(analysis.Diagnostic{
		Pos:     firstFile.Pos(),
		End:     firstFile.End(),
		Message: "code_contract §2: package " + pass.Pkg.Name() + " has no package-level doc comment (add one in doc.go or above any package clause)",
	})
	return nil, nil
}
