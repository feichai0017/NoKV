// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package mapanyboundary enforces code_contract.md §9: map[string]any is
// reserved for export / diagnostics boundaries. Inside business logic the
// shape must be typed.
//
// Allowed boundaries (heuristic): CLI binaries under cmd/, files named
// stats.go / *_stats.go, files named diagnostics.go / *_diagnostics.go, and
// generated proto sources.
package mapanyboundary

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports map[string]any (and the equivalent map[string]interface{})
// expressions that appear outside the allowed boundary files.
var Analyzer = &analysis.Analyzer{
	Name: "mapanyboundary",
	Doc:  "map[string]any is reserved for diagnostics/export boundaries (code_contract §9)",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		filename := pass.Fset.Position(file.Pos()).Filename
		if isAllowedBoundary(filename, pass.Pkg) {
			continue
		}
		// Any function signature that mentions map[string]any (return or
		// parameter) is itself the diagnostics boundary, so the entire
		// signature plus body is exempt. This also covers interface method
		// declarations and helper functions like copyStats(dst, src
		// map[string]any) that operate on already-typed diagnostics maps.
		var exempt []posRange
		ast.Inspect(file, func(n ast.Node) bool {
			ft, ok := n.(*ast.FuncType)
			if !ok {
				return true
			}
			if signatureMentionsMapAny(ft) {
				exempt = append(exempt, posRange{start: ft.Pos(), end: ft.End()})
			}
			return true
		})
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Type == nil {
				continue
			}
			if signatureMentionsMapAny(fn.Type) {
				exempt = append(exempt, posRange{start: fn.Body.Pos(), end: fn.Body.End()})
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			mt, ok := n.(*ast.MapType)
			if !ok {
				return true
			}
			if !isStringKey(mt.Key) || !isAnyValue(mt.Value) {
				return true
			}
			if withinRanges(mt.Pos(), exempt) {
				return true
			}
			pass.Report(analysis.Diagnostic{
				Pos:     mt.Pos(),
				End:     mt.End(),
				Message: "code_contract §9: map[string]any is reserved for diagnostics/export boundaries; use a typed struct or named map here",
			})
			return true
		})
	}
	return nil, nil
}

type posRange struct {
	start, end token.Pos
}

func withinRanges(p token.Pos, ranges []posRange) bool {
	for _, r := range ranges {
		if p >= r.start && p < r.end {
			return true
		}
	}
	return false
}

func signatureMentionsMapAny(ft *ast.FuncType) bool {
	if ft == nil {
		return false
	}
	return fieldListMentionsMapAny(ft.Params) || fieldListMentionsMapAny(ft.Results)
}

func fieldListMentionsMapAny(list *ast.FieldList) bool {
	if list == nil {
		return false
	}
	for _, field := range list.List {
		mt, ok := field.Type.(*ast.MapType)
		if !ok {
			continue
		}
		if isStringKey(mt.Key) && isAnyValue(mt.Value) {
			return true
		}
	}
	return false
}

func isStringKey(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == "string"
}

func isAnyValue(expr ast.Expr) bool {
	if ident, ok := expr.(*ast.Ident); ok && ident.Name == "any" {
		return true
	}
	if it, ok := expr.(*ast.InterfaceType); ok && it.Methods != nil && len(it.Methods.List) == 0 {
		return true
	}
	return false
}

func isAllowedBoundary(filename string, pkg *types.Package) bool {
	base := filepath.Base(filename)
	if strings.HasSuffix(base, "_test.go") {
		return true
	}
	// Only the canonical diagnostics file names are allowed; the
	// `<thing>_stats.go` / `<thing>_metrics.go` variants are forbidden so the
	// per-package diagnostics surface stays in one obvious file.
	if base == "stats.go" || base == "metrics.go" || base == "diagnostics.go" || base == "service.go" {
		return true
	}
	if strings.HasSuffix(base, ".pb.go") || strings.HasSuffix(base, ".gen.go") {
		return true
	}
	if pkg != nil {
		path := pkg.Path()
		if strings.HasPrefix(path, "github.com/feichai0017/NoKV/cmd/") {
			return true
		}
		if strings.HasPrefix(path, "github.com/feichai0017/NoKV/benchmark") {
			return true
		}
		if strings.HasSuffix(path, "/stats") || strings.Contains(path, "/stats/") {
			return true
		}
	}
	return false
}
