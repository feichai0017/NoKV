// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package errstringmatch enforces code_contract.md §8: callers must branch on
// errors.Is, errors.As, or nokverrors.KindOf — never match against error
// message strings. The analyzer flags both direct comparisons and the most
// common strings-package shapes.
package errstringmatch

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports comparisons or strings.* calls that consume err.Error().
var Analyzer = &analysis.Analyzer{
	Name: "errstringmatch",
	Doc:  "do not match error message strings; use errors.Is/As or nokverrors.KindOf (code_contract §8)",
	Run:  run,
}

var errorInterface = types.Universe.Lookup("error").Type().Underlying().(*types.Interface)

var stringsMatchFns = map[string]struct{}{
	"Contains":    {},
	"HasPrefix":   {},
	"HasSuffix":   {},
	"EqualFold":   {},
	"Index":       {},
	"LastIndex":   {},
	"IndexAny":    {},
	"IndexByte":   {},
	"Compare":     {},
	"ContainsAny": {},
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		base := filepath.Base(pass.Fset.Position(file.Pos()).Filename)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.BinaryExpr:
				inspectComparison(pass, node)
			case *ast.CallExpr:
				inspectStringsCall(pass, node)
			}
			return true
		})
	}
	return nil, nil
}

func inspectComparison(pass *analysis.Pass, expr *ast.BinaryExpr) {
	if expr.Op != token.EQL && expr.Op != token.NEQ {
		return
	}
	if !isErrErrorCall(expr.X, pass.TypesInfo) && !isErrErrorCall(expr.Y, pass.TypesInfo) {
		return
	}
	pass.Report(analysis.Diagnostic{
		Pos:     expr.Pos(),
		End:     expr.End(),
		Message: "code_contract §8: do not compare err.Error() strings; use errors.Is/As or nokverrors.KindOf instead",
	})
}

func inspectStringsCall(pass *analysis.Pass, call *ast.CallExpr) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "strings" {
		return
	}
	if _, matched := stringsMatchFns[sel.Sel.Name]; !matched {
		return
	}
	for _, arg := range call.Args {
		if !isErrErrorCall(arg, pass.TypesInfo) {
			continue
		}
		pass.Report(analysis.Diagnostic{
			Pos:     call.Pos(),
			End:     call.End(),
			Message: "code_contract §8: strings." + sel.Sel.Name + " against err.Error() is a stringly-typed error match; use errors.Is/As or nokverrors.KindOf instead",
		})
		return
	}
}

func isErrErrorCall(expr ast.Expr, info *types.Info) bool {
	if info == nil {
		return false
	}
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 0 {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Error" {
		return false
	}
	t := info.TypeOf(sel.X)
	if t == nil {
		return false
	}
	return types.Implements(t, errorInterface)
}
