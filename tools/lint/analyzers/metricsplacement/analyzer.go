// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package metricsplacement enforces code_contract.md §9: runtime metric
// updates use `recordX` helpers, and those helpers must live in `metrics.go`
// (or `*_metrics.go`). Detecting misplacement at lint time stops the codebase
// from re-growing the pre-contract pattern where every package sprinkled
// recordX functions across random files.
package metricsplacement

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports `func ... recordX(...)` declarations outside metrics.go.
var Analyzer = &analysis.Analyzer{
	Name: "metricsplacement",
	Doc:  "recordX functions must live in metrics.go (code_contract §9)",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	if pass.Pkg == nil {
		return nil, nil
	}
	if isExemptPackage(pass.Pkg.Path()) {
		return nil, nil
	}
	for _, file := range pass.Files {
		base := filepath.Base(pass.Fset.Position(file.Pos()).Filename)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}
		if isMetricsFile(base) {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name == nil {
				continue
			}
			if !isRecordName(fn.Name.Name) {
				continue
			}
			// recordX is reserved for metric updates. We only flag the
			// function when its body actually touches a sync/atomic value;
			// state-tracking helpers that happen to use the record* prefix
			// (data-plane topology transitions, scheduler health
			// logs) are tagged by name but never mutate atomics. The naming
			// drift is real, but enforcing it via this analyzer would
			// produce more noise than signal.
			if !bodyTouchesAtomic(fn.Body, pass.TypesInfo) {
				continue
			}
			pass.Report(analysis.Diagnostic{
				Pos:     fn.Name.Pos(),
				End:     fn.Name.End(),
				Message: "code_contract §9: " + fn.Name.Name + " is a recordX metric helper but lives in " + base + " (move to metrics.go)",
			})
		}
	}
	return nil, nil
}

func isMetricsFile(name string) bool {
	// The contract uses one canonical metrics file per package; the
	// `<thing>_metrics.go` variant is forbidden because it splits the metric
	// surface across files without any benefit a reader can see.
	return name == "metrics.go"
}

func isRecordName(name string) bool {
	const prefix = "record"
	if !strings.HasPrefix(name, prefix) || len(name) <= len(prefix) {
		return false
	}
	c := name[len(prefix)]
	return c >= 'A' && c <= 'Z'
}

// bodyTouchesAtomic returns true when the function body contains either a
// call into sync/atomic (e.g. atomic.AddUint64) or a method call whose
// receiver type is one of the atomic.Uint64/Int64/Bool/Pointer wrappers.
func bodyTouchesAtomic(body *ast.BlockStmt, info *types.Info) bool {
	if body == nil || info == nil {
		return false
	}
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		// atomic.AddUint64(...) — selector base is the imported package.
		if ident, ok := sel.X.(*ast.Ident); ok {
			if obj := info.ObjectOf(ident); obj != nil {
				if pkgName, ok := obj.(*types.PkgName); ok && pkgName.Imported().Path() == "sync/atomic" {
					found = true
					return false
				}
			}
		}
		// receiver.Add(1), receiver.Store(...) etc. — check the receiver type.
		if t := info.TypeOf(sel.X); t != nil {
			if strings.Contains(t.String(), "sync/atomic.") {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

func isExemptPackage(path string) bool {
	// Benchmarks own their own metric reporting cadence and are scoped out.
	if strings.HasPrefix(path, "github.com/feichai0017/NoKV/benchmark") {
		return true
	}
	// metrics/* and */metrics own metrics directly; recordX inside them is fine.
	if strings.Contains(path, "/metrics") || strings.HasSuffix(path, "/metrics") {
		return true
	}
	return false
}
