package architecture

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDependencyBoundaries(t *testing.T) {
	root, err := ModuleRoot()
	if err != nil {
		t.Fatal(err)
	}
	packages, err := LoadPackages(root)
	if err != nil {
		t.Fatal(err)
	}

	var violations []Violation
	violations = append(violations, CheckImportRules(packages)...)
	violations = append(violations, CheckCombinedImportRules(packages)...)
	violations = append(violations, CheckRemovedPathRules(root)...)
	if len(violations) == 0 {
		return
	}

	var b strings.Builder
	b.WriteString("dependency boundary violations:")
	for _, violation := range violations {
		b.WriteString("\n- ")
		b.WriteString(violation.Rule)
		b.WriteString(": ")
		b.WriteString(violation.Package)
		if violation.Import != "" {
			b.WriteString(" imports ")
			b.WriteString(violation.Import)
		}
	}
	t.Fatal(b.String())
}

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
			ImportPath: modulePath + "/local",
			Imports:    []string{modulePath + "/raftstore/client"},
		},
		{
			ImportPath: modulePath + "/local/stats",
			Imports:    []string{modulePath + "/raftstore/stats"},
		},
		{
			ImportPath: modulePath + "/local/internal/commit",
			Imports:    []string{modulePath + "/errors"},
		},
		{
			ImportPath: modulePath + "/meta/root/server",
			Imports:    []string{modulePath + "/coordinator/client"},
		},
		{
			ImportPath: modulePath + "/engine/lsm",
			Imports:    []string{modulePath + "/errors"},
		},
		{
			ImportPath: modulePath + "/coordinator/balancer",
			Imports:    []string{modulePath + "/raftstore/store"},
		},
		{
			ImportPath: modulePath + "/coordinator/scheduling",
			Imports:    []string{modulePath + "/coordinator/server"},
		},
		{
			ImportPath: modulePath + "/coordinator/storecontrol",
			Imports:    []string{modulePath + "/coordinator/scheduling"},
		},
	}

	violations := CheckImportRules(packages)
	assertViolation(t, violations, "root package stays free of distributed assembly", modulePath, modulePath+"/raftstore/mvcc")
	assertViolation(t, violations, "fsmeta executor stays runtime-neutral", modulePath+"/fsmeta/exec", modulePath+"/raftstore/client")
	assertViolation(t, violations, "local db stays free of distributed assembly", modulePath+"/local", modulePath+"/raftstore/client")
	assertViolation(t, violations, "local db stays free of distributed assembly", modulePath+"/local/stats", modulePath+"/raftstore/stats")
	assertViolation(t, violations, "local runtime stays free of global error taxonomy", modulePath+"/local/internal/commit", modulePath+"/errors")
	assertViolation(t, violations, "meta root does not depend on coordinator service layer", modulePath+"/meta/root/server", modulePath+"/coordinator/client")
	assertViolation(t, violations, "embedded engine stays free of global error taxonomy", modulePath+"/engine/lsm", modulePath+"/errors")
	assertViolation(t, violations, "coordinator stays free of raftstore execution packages", modulePath+"/coordinator/balancer", modulePath+"/raftstore/store")
	assertViolation(t, violations, "coordinator scheduling stays policy-only", modulePath+"/coordinator/scheduling", modulePath+"/coordinator/server")
	assertViolation(t, violations, "coordinator storecontrol stays out of scheduling and service", modulePath+"/coordinator/storecontrol", modulePath+"/coordinator/scheduling")
}

func TestCheckImportRulesHonorsExactAndPrefixScopes(t *testing.T) {
	packages := []GoPackage{
		{
			ImportPath: modulePath + "/cmd/nokv",
			Imports:    []string{modulePath + "/raftstore/mvcc"},
		},
		{
			ImportPath: modulePath + "/fsmeta/client",
			Imports:    []string{modulePath + "/raftstore/client"},
		},
		{
			ImportPath: modulePath + "/local/errkind",
			Imports:    []string{modulePath + "/errors"},
		},
		{
			ImportPath: modulePath + "/coordinator/storecontrol",
			Imports: []string{
				modulePath + "/coordinator/client",
			},
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
		"runtime",
		"scheduler",
		"local/internal/background",
		"raftstore/mode",
		"raftstore/scheduler",
		"coordinator/adapter",
		"coordinator/view",
		"coordinator/protocol/eunomia",
		"dbcore",
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
	assertViolation(t, violations, "old db runtime package stays removed", "runtime", "")
	assertViolation(t, violations, "local background package stays folded into local", "local/internal/background", "")
	assertViolation(t, violations, "top-level scheduler package stays folded into coordinator/storecontrol", "scheduler", "")
	assertViolation(t, violations, "raftstore mode package stays moved to local/workdir", "raftstore/mode", "")
	assertViolation(t, violations, "raftstore scheduler package stays moved to coordinator/storecontrol", "raftstore/scheduler", "")
	assertViolation(t, violations, "coordinator adapter package stays folded into coordinator/storecontrol", "coordinator/adapter", "")
	assertViolation(t, violations, "coordinator view package stays folded into coordinator/catalog", "coordinator/view", "")
	assertViolation(t, violations, "coordinator eunomia package stays removed", "coordinator/protocol/eunomia", "")
	assertViolation(t, violations, "dbcore package stays folded into local/internal and utils", "dbcore", "")
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
