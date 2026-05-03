package architecture

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckImportRulesCatchesForbiddenBoundaries(t *testing.T) {
	packages := []GoPackage{
		{
			ImportPath: modulePath,
			Imports:    []string{modulePath + "/raftstore/mvcc"},
		},
		{
			ImportPath: modulePath + "/fsmeta/exec",
			Imports:    []string{modulePath + "/raftstore/client"},
		},
		{
			ImportPath: modulePath + "/meta/root/server",
			Imports:    []string{modulePath + "/coordinator/client"},
		},
	}

	violations := CheckImportRules(packages)
	assertViolation(t, violations, "root package stays free of distributed assembly", modulePath, modulePath+"/raftstore/mvcc")
	assertViolation(t, violations, "fsmeta executor stays runtime-neutral", modulePath+"/fsmeta/exec", modulePath+"/raftstore/client")
	assertViolation(t, violations, "meta root does not depend on coordinator service layer", modulePath+"/meta/root/server", modulePath+"/coordinator/client")
}

func TestCheckImportRulesHonorsExactAndPrefixScopes(t *testing.T) {
	packages := []GoPackage{
		{
			ImportPath: modulePath + "/runtime",
			Imports:    []string{modulePath + "/raftstore/mvcc"},
		},
		{
			ImportPath: modulePath + "/fsmeta/client",
			Imports:    []string{modulePath + "/raftstore/client"},
		},
	}

	violations := CheckImportRules(packages)
	if len(violations) != 0 {
		t.Fatalf("expected no violations outside guarded scopes, got %#v", violations)
	}
}

func TestCheckCombinedImportRulesAllowsOnlyDistributedAssembly(t *testing.T) {
	required := []string{
		modulePath + "/fsmeta/exec",
		modulePath + "/coordinator/client",
		modulePath + "/raftstore/client",
	}
	packages := []GoPackage{
		{
			ImportPath: modulePath + "/fsmeta/runtime/raftstore",
			Imports:    required,
		},
		{
			ImportPath: modulePath + "/cmd/nokv-fsmeta",
			Imports:    required,
		},
		{
			ImportPath: modulePath + "/fsmeta/runtime/local",
			Imports: []string{
				modulePath + "/fsmeta/exec",
				modulePath + "/raftstore/client",
			},
		},
	}

	violations := CheckCombinedImportRules(packages)
	if len(violations) != 1 {
		t.Fatalf("expected one combined import violation, got %#v", violations)
	}
	assertViolation(t, violations, "only raftstore fsmeta runtime combines fsmeta exec with coordinator and raftstore clients", modulePath+"/cmd/nokv-fsmeta", "")
}

func TestCheckRemovedPathRules(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{
		"raftstore/mode",
		"coordinator/protocol/eunomia",
	} {
		if err := os.MkdirAll(filepath.Join(root, path), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "raftstore/migrate"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "raftstore/migrate/mode.go"), []byte("package migrate\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	violations := CheckRemovedPathRules(root)
	assertViolation(t, violations, "raftstore mode package stays moved to runtime/mode", "raftstore/mode", "")
	assertViolation(t, violations, "coordinator eunomia package stays removed", "coordinator/protocol/eunomia", "")
	assertViolation(t, violations, "raftstore migrate mode alias stays removed", "raftstore/migrate/mode.go", "")
}

func TestPathMatchesPackageBoundaries(t *testing.T) {
	if !pathMatches(modulePath+"/raftstore/client", modulePath+"/raftstore") {
		t.Fatal("expected child package to match prefix")
	}
	if pathMatches(modulePath+"/raftstorex", modulePath+"/raftstore") {
		t.Fatal("expected sibling prefix with shared text not to match")
	}
	if !pathMatches(modulePath+"/raftstore", modulePath+"/raftstore") {
		t.Fatal("expected exact package to match")
	}
}

func assertViolation(t *testing.T, violations []Violation, rule, pkg, imp string) {
	t.Helper()
	for _, violation := range violations {
		if violation.Rule != rule || violation.Package != pkg {
			continue
		}
		if imp == "" || violation.Import == imp {
			return
		}
	}
	t.Fatalf("missing violation rule=%q package=%q import=%q in %#v", rule, pkg, imp, violations)
}
