// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package sentinelerrors enforces code_contract.md §8: package-level Err*/err*
// sentinels must live in a file named errors.go.
//
// Only declarations whose declared type satisfies the error interface trip the
// check, so unrelated identifiers that happen to use the Err prefix (for
// example error-class enums) do not produce false positives.
package sentinelerrors

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports any package-level var declaration named Err<Upper> or
// err<Upper> that declares an error value outside a file named errors.go.
var Analyzer = &analysis.Analyzer{
	Name: "sentinelerrors",
	Doc:  "package-level Err*/err* sentinels must live in errors.go (code_contract.md §8)",
	Run:  run,
}

var errorInterface = types.Universe.Lookup("error").Type().Underlying().(*types.Interface)

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		base := filepath.Base(pass.Fset.Position(file.Pos()).Filename)
		if base == "errors.go" || strings.HasSuffix(base, "_test.go") {
			continue
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, name := range value.Names {
					if !looksLikeSentinelErrorName(name.Name) {
						continue
					}
					obj, ok := pass.TypesInfo.Defs[name].(*types.Var)
					if !ok || obj == nil {
						continue
					}
					if !types.Implements(obj.Type(), errorInterface) {
						continue
					}
					pass.Report(analysis.Diagnostic{
						Pos:     name.Pos(),
						End:     name.End(),
						Message: "code_contract §8: sentinel " + name.Name + " must be declared in errors.go (currently in " + base + ")",
					})
				}
			}
		}
	}
	return nil, nil
}

func looksLikeSentinelErrorName(s string) bool {
	if len(s) <= 3 {
		return false
	}
	if (strings.HasPrefix(s, "Err") || strings.HasPrefix(s, "err")) && isUpper(s[3]) {
		return true
	}
	return false
}

func isUpper(b byte) bool { return b >= 'A' && b <= 'Z' }
