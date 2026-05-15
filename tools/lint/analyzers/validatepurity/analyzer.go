// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package validatepurity enforces code_contract.md §7: Validate* methods are
// pure — they must not mutate receiver state. The contract reserves Ensure*
// for any function that may mutate state to satisfy a condition; mixing the
// two pollutes the naming convention readers rely on.
//
// The analyzer detects assignments whose left-hand side reaches the
// receiver identifier (s.f = ..., s.m[k] = ..., s.x.y = ...). It does not
// attempt to track aliasing or hidden mutation via helper calls.
package validatepurity

import (
	"go/ast"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports Validate* methods that assign into their receiver.
var Analyzer = &analysis.Analyzer{
	Name: "validatepurity",
	Doc:  "Validate* methods must be pure; rename to Ensure* if mutation is intentional (code_contract §7)",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		base := filepath.Base(pass.Fset.Position(file.Pos()).Filename)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil || fn.Name == nil {
				continue
			}
			if !isValidateName(fn.Name.Name) {
				continue
			}
			recvName := receiverIdent(fn.Recv)
			if recvName == "" {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch stmt := n.(type) {
				case *ast.AssignStmt:
					for _, lhs := range stmt.Lhs {
						if rootIdent(lhs) != recvName {
							continue
						}
						pass.Report(analysis.Diagnostic{
							Pos:     stmt.Pos(),
							End:     stmt.End(),
							Message: "code_contract §7: " + fn.Name.Name + " mutates receiver " + recvName + "; rename to Ensure* or remove the mutation",
						})
						return true
					}
				case *ast.IncDecStmt:
					if rootIdent(stmt.X) == recvName {
						pass.Report(analysis.Diagnostic{
							Pos:     stmt.Pos(),
							End:     stmt.End(),
							Message: "code_contract §7: " + fn.Name.Name + " mutates receiver " + recvName + "; rename to Ensure* or remove the mutation",
						})
					}
				}
				return true
			})
		}
	}
	return nil, nil
}

func isValidateName(name string) bool {
	const prefix = "Validate"
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	if len(name) == len(prefix) {
		return true
	}
	c := name[len(prefix)]
	return c >= 'A' && c <= 'Z'
}

func receiverIdent(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) == 0 {
		return ""
	}
	field := recv.List[0]
	if len(field.Names) == 0 {
		return ""
	}
	return field.Names[0].Name
}

func rootIdent(expr ast.Expr) string {
	for {
		switch e := expr.(type) {
		case *ast.Ident:
			return e.Name
		case *ast.SelectorExpr:
			expr = e.X
		case *ast.IndexExpr:
			expr = e.X
		case *ast.IndexListExpr:
			expr = e.X
		case *ast.StarExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		default:
			return ""
		}
	}
}
