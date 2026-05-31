// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package importboundary

import "testing"

func TestEvaluateCatchesForbiddenBoundaries(t *testing.T) {
	cases := []struct {
		name        string
		pkg         string
		imports     []string
		wantRule    string
		wantImport  string
		wantPackage string
	}{
		{
			name:        "root package importing fsmeta",
			pkg:         ModulePath,
			imports:     []string{ModulePath + "/fsmeta/model"},
			wantRule:    "root package stays an architecture anchor",
			wantImport:  ModulePath + "/fsmeta/model",
			wantPackage: ModulePath,
		},
		{
			name:        "fsmeta model importing layout",
			pkg:         ModulePath + "/fsmeta/model",
			imports:     []string{ModulePath + "/fsmeta/layout"},
			wantRule:    "fsmeta model stays semantic-only",
			wantImport:  ModulePath + "/fsmeta/layout",
			wantPackage: ModulePath + "/fsmeta/model",
		},
		{
			name:        "fsmeta layout importing executor",
			pkg:         ModulePath + "/fsmeta/layout",
			imports:     []string{ModulePath + "/fsmeta/exec"},
			wantRule:    "fsmeta layout stays below execution",
			wantImport:  ModulePath + "/fsmeta/exec",
			wantPackage: ModulePath + "/fsmeta/layout",
		},
		{
			name:        "fsmeta backend importing runtime",
			pkg:         ModulePath + "/fsmeta/backend",
			imports:     []string{ModulePath + "/fsmeta/runtime/local"},
			wantRule:    "fsmeta backend stays runtime-neutral",
			wantImport:  ModulePath + "/fsmeta/runtime/local",
			wantPackage: ModulePath + "/fsmeta/backend",
		},
		{
			name:        "fsmeta exec importing protobuf",
			pkg:         ModulePath + "/fsmeta/exec",
			imports:     []string{ModulePath + "/pb/metadata"},
			wantRule:    "fsmeta executor stays runtime-neutral",
			wantImport:  ModulePath + "/pb/metadata",
			wantPackage: ModulePath + "/fsmeta/exec",
		},
		{
			name:        "meta root reaching into coordinator",
			pkg:         ModulePath + "/meta/root/server",
			imports:     []string{ModulePath + "/coordinator/client"},
			wantRule:    "meta root does not depend on coordinator service layer",
			wantImport:  ModulePath + "/coordinator/client",
			wantPackage: ModulePath + "/meta/root/server",
		},
		{
			name:        "coordinator importing fsmeta runtime",
			pkg:         ModulePath + "/coordinator/server",
			imports:     []string{ModulePath + "/fsmeta/runtime/local"},
			wantRule:    "coordinator does not import fsmeta execution",
			wantImport:  ModulePath + "/fsmeta/runtime/local",
			wantPackage: ModulePath + "/coordinator/server",
		},
		{
			name:        "scheduling reaching into service layer",
			pkg:         ModulePath + "/coordinator/scheduling",
			imports:     []string{ModulePath + "/coordinator/server"},
			wantRule:    "coordinator scheduling stays policy-only",
			wantImport:  ModulePath + "/coordinator/server",
			wantPackage: ModulePath + "/coordinator/scheduling",
		},
		{
			name:        "storecontrol reaching into scheduling",
			pkg:         ModulePath + "/coordinator/storecontrol",
			imports:     []string{ModulePath + "/coordinator/scheduling"},
			wantRule:    "coordinator storecontrol stays out of scheduling and service",
			wantImport:  ModulePath + "/coordinator/scheduling",
			wantPackage: ModulePath + "/coordinator/storecontrol",
		},
		{
			name:        "utils importing fsmeta",
			pkg:         ModulePath + "/utils",
			imports:     []string{ModulePath + "/fsmeta/model"},
			wantRule:    "utils stays domain-neutral",
			wantImport:  ModulePath + "/fsmeta/model",
			wantPackage: ModulePath + "/utils",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			violations := Evaluate(tc.pkg, tc.imports)
			if !hasViolation(violations, tc.wantRule, tc.wantPackage, tc.wantImport) {
				t.Fatalf("missing violation rule=%q package=%q import=%q in %#v",
					tc.wantRule, tc.wantPackage, tc.wantImport, violations)
			}
		})
	}
}

func TestEvaluateHonorsScopes(t *testing.T) {
	cases := []struct {
		name    string
		pkg     string
		imports []string
	}{
		{
			name:    "exact-match rule does not apply to subpackages",
			pkg:     ModulePath + "/cmd/nokv-fsmeta",
			imports: []string{ModulePath + "/fsmeta/model"},
		},
		{
			name:    "fsmeta client outside fsmeta exec scope",
			pkg:     ModulePath + "/fsmeta/client",
			imports: []string{ModulePath + "/pb/fsmeta"},
		},
		{
			name: "storecontrol importing client is allowed",
			pkg:  ModulePath + "/coordinator/storecontrol",
			imports: []string{
				ModulePath + "/coordinator/client",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Evaluate(tc.pkg, tc.imports); len(got) != 0 {
				t.Fatalf("expected no violations, got %#v", got)
			}
		})
	}
}

func TestPathMatchesIsPrefixSafe(t *testing.T) {
	if !PathMatches(ModulePath+"/fsmeta/client", ModulePath+"/fsmeta") {
		t.Fatal("child package should match prefix")
	}
	if PathMatches(ModulePath+"/fsmetax", ModulePath+"/fsmeta") {
		t.Fatal("sibling prefix sharing leading characters must not match")
	}
	if !PathMatches(ModulePath+"/fsmeta", ModulePath+"/fsmeta") {
		t.Fatal("exact package should match")
	}
}

func hasViolation(violations []Violation, rule, pkg, imp string) bool {
	for _, v := range violations {
		if v.Rule == rule && v.Package == pkg && v.Import == imp {
			return true
		}
	}
	return false
}
