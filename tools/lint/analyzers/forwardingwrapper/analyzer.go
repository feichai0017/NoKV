// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package forwardingwrapper enforces code_contract.md §7: a method whose body
// is a single `return s.field.SameName(args...)` (or the void equivalent) is
// dead weight — readers must follow one extra hop just to find the real
// implementation. Authors who genuinely need the wrapper (RPC/CLI adapters,
// migration shims) can opt out with a `//forwarding-ok: <reason>` comment
// directly above the method.
package forwardingwrapper

import (
	"go/ast"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports one-line forwarding methods that match the receiver's
// field method 1-to-1 in name and arguments.
var Analyzer = &analysis.Analyzer{
	Name: "forwardingwrapper",
	Doc:  "flag one-line forwarding methods unless tagged //forwarding-ok (code_contract §7)",
	Run:  run,
}

const forwardingOKMarker = "forwarding-ok"

// interfaceForwardingMethods are method names whose forwarding is almost
// always a legitimate interface-adapter pattern (io.Closer/Reader/Writer/
// Seeker/Syncer, hash.Hash sums, iterator surfaces). Wrappers that exist only
// to satisfy a third-party interface should not pay the //forwarding-ok tax.
var interfaceForwardingMethods = map[string]struct{}{
	"Close":      {},
	"Read":       {},
	"Write":      {},
	"Seek":       {},
	"Sync":       {},
	"Truncate":   {},
	"Stat":       {},
	"Name":       {},
	"Flush":      {},
	"Reset":      {},
	"Sum32":      {},
	"Sum64":      {},
	"Item":       {},
	"Key":        {},
	"Valid":      {},
	"Next":       {},
	"Prev":       {},
	"Rewind":     {},
	"Bytes":      {},
	"View":       {},
	"Size":       {},
	"Len":        {},
	"String":     {},
	"Error":      {},
	"Unwrap":     {},
	"Header":     {},
	"Trailer":    {},
	"Context":    {},
	"SendMsg":    {},
	"RecvMsg":    {},
	"SendHeader": {},
	"SetHeader":  {},
	"SetTrailer": {},
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		filename := pass.Fset.Position(file.Pos()).Filename
		base := filepath.Base(filename)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}
		if isGenerated(base) {
			continue
		}
		if pass.Pkg != nil && strings.HasPrefix(pass.Pkg.Path(), "github.com/feichai0017/NoKV/cmd/") {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil || fn.Name == nil {
				continue
			}
			if hasForwardingOK(fn) {
				continue
			}
			if _, exempt := interfaceForwardingMethods[fn.Name.Name]; exempt {
				continue
			}
			if !isForwardingBody(fn) {
				continue
			}
			pass.Report(analysis.Diagnostic{
				Pos:     fn.Pos(),
				End:     fn.End(),
				Message: "code_contract §7: " + fn.Name.Name + " is a one-line forwarding wrapper; inline the field call or add `//forwarding-ok: <reason>` to keep it",
			})
		}
	}
	return nil, nil
}

func hasForwardingOK(fn *ast.FuncDecl) bool {
	if fn.Doc == nil {
		return false
	}
	for _, comment := range fn.Doc.List {
		if strings.Contains(comment.Text, forwardingOKMarker) {
			return true
		}
	}
	return false
}

func isForwardingBody(fn *ast.FuncDecl) bool {
	if len(fn.Body.List) != 1 {
		return false
	}
	var call *ast.CallExpr
	switch stmt := fn.Body.List[0].(type) {
	case *ast.ReturnStmt:
		if len(stmt.Results) != 1 {
			return false
		}
		c, ok := stmt.Results[0].(*ast.CallExpr)
		if !ok {
			return false
		}
		call = c
	case *ast.ExprStmt:
		c, ok := stmt.X.(*ast.CallExpr)
		if !ok {
			return false
		}
		call = c
	default:
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel == nil {
		return false
	}
	if sel.Sel.Name != fn.Name.Name {
		return false
	}
	if _, isPlainIdent := sel.X.(*ast.Ident); isPlainIdent {
		// `s.Method()` would be self-recursion, not forwarding.
		return false
	}
	recvName := receiverIdent(fn.Recv)
	if recvName == "" {
		return false
	}
	if rootIdent(sel.X) != recvName {
		return false
	}
	return argsMatchParams(call.Args, fn.Type.Params)
}

func argsMatchParams(args []ast.Expr, params *ast.FieldList) bool {
	expected := paramNames(params)
	if len(args) != len(expected) {
		return false
	}
	for i, arg := range args {
		ident, ok := arg.(*ast.Ident)
		if !ok || ident.Name != expected[i] {
			return false
		}
	}
	return true
}

func paramNames(params *ast.FieldList) []string {
	if params == nil {
		return nil
	}
	var out []string
	for _, field := range params.List {
		for _, name := range field.Names {
			out = append(out, name.Name)
		}
	}
	return out
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
		case *ast.StarExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		default:
			return ""
		}
	}
}

func isGenerated(name string) bool {
	return strings.HasSuffix(name, ".pb.go") ||
		strings.HasSuffix(name, "_grpc.pb.go") ||
		strings.HasSuffix(name, ".gen.go")
}
