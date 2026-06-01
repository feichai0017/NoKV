// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package importboundary enforces NoKV's package-import direction rules from
// docs/guide/development/code_contract.md. Ownership of truth, not convenience,
// decides which package may import which.
package importboundary

import "strings"

// ModulePath is the canonical Go module path used by all import-direction
// rules. Keeping it public lets tests construct synthetic packages without
// relying on the go list driver.
const ModulePath = "github.com/feichai0017/NoKV"

// Rule describes one import-direction constraint. PackagePrefix selects which
// packages the rule applies to; Forbidden lists package paths that those
// packages may not import. Exempt opts specific sub-paths out of the rule.
type Rule struct {
	Name          string
	PackagePrefix string
	PackageExact  bool
	Forbidden     []string
	Exempt        []string
}

func (r Rule) MatchesPackage(pkgPath string) bool {
	if r.PackageExact {
		return pkgPath == r.PackagePrefix
	}
	return PathMatches(pkgPath, r.PackagePrefix)
}

func (r Rule) IsExempt(pkgPath string) bool {
	return exempted(pkgPath, r.Exempt)
}

// Rules is the full ordered list of import-direction constraints. The order is
// significant only for deterministic diagnostic output.
var Rules = []Rule{
	{
		Name:          "root package stays an architecture anchor",
		PackagePrefix: ModulePath,
		PackageExact:  true,
		Forbidden: []string{
			ModulePath + "/fsmeta",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/pb",
		},
	},
	{
		Name:          "fsmeta model stays semantic-only",
		PackagePrefix: ModulePath + "/fsmeta/model",
		Forbidden: []string{
			ModulePath + "/fsmeta/layout",
			ModulePath + "/fsmeta/backend",
			ModulePath + "/fsmeta/exec",
			ModulePath + "/fsmeta/runtime",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/pb",
		},
	},
	{
		Name:          "fsmeta layout stays below execution",
		PackagePrefix: ModulePath + "/fsmeta/layout",
		Forbidden: []string{
			ModulePath + "/fsmeta/backend",
			ModulePath + "/fsmeta/exec",
			ModulePath + "/fsmeta/runtime",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/pb",
		},
	},
	{
		Name:          "fsmeta backend stays runtime-neutral",
		PackagePrefix: ModulePath + "/fsmeta/backend",
		Forbidden: []string{
			ModulePath + "/fsmeta/runtime",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/pb",
		},
	},
	{
		Name:          "fsmeta executor stays runtime-neutral",
		PackagePrefix: ModulePath + "/fsmeta/exec",
		Forbidden: []string{
			ModulePath + "/fsmeta/runtime",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/pb",
		},
	},
	{
		Name:          "meta root does not depend on coordinator service layer",
		PackagePrefix: ModulePath + "/meta/root",
		Forbidden: []string{
			ModulePath + "/coordinator",
		},
	},
	{
		Name:          "coordinator does not import fsmeta execution",
		PackagePrefix: ModulePath + "/coordinator",
		Forbidden: []string{
			ModulePath + "/fsmeta/exec",
			ModulePath + "/fsmeta/runtime",
		},
	},
	{
		Name:          "coordinator scheduling stays policy-only",
		PackagePrefix: ModulePath + "/coordinator/scheduling",
		Forbidden: []string{
			ModulePath + "/coordinator/client",
			ModulePath + "/coordinator/server",
		},
	},
	{
		Name:          "utils stays domain-neutral",
		PackagePrefix: ModulePath + "/utils",
		Forbidden: []string{
			ModulePath + "/errors",
			ModulePath + "/fsmeta",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/pb",
		},
	},
}

// Violation describes one (package, import) pair that broke a Rule.
type Violation struct {
	Rule    string
	Package string
	Import  string
}

// Evaluate reports every Rule that the package at pkgPath violates given its
// declared importPaths. The pure function exists so unit tests can exercise the
// matching logic without spinning up a Go analyzer Pass.
func Evaluate(pkgPath string, importPaths []string) []Violation {
	var violations []Violation
	for _, rule := range Rules {
		if !rule.MatchesPackage(pkgPath) || rule.IsExempt(pkgPath) {
			continue
		}
		for _, imp := range importPaths {
			for _, forbidden := range rule.Forbidden {
				if PathMatches(imp, forbidden) {
					violations = append(violations, Violation{
						Rule:    rule.Name,
						Package: pkgPath,
						Import:  imp,
					})
				}
			}
		}
	}
	return violations
}

// PathMatches reports whether path is exactly prefix or under prefix as a Go
// import path segment. It deliberately rejects sibling strings that merely
// share leading characters.
func PathMatches(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func exempted(pkgPath string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if PathMatches(pkgPath, prefix) {
			return true
		}
	}
	return false
}
