package scripts_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShellScriptEntrypointsAreStrictExecutableBash(t *testing.T) {
	root := repoRoot(t)
	scriptsDir := filepath.Join(root, "scripts")
	err := filepath.WalkDir(scriptsDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".sh" {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(data)
		if !strings.HasPrefix(text, "#!/usr/bin/env bash\n") {
			t.Fatalf("%s must use the portable bash shebang", rel)
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}
		isLibrary := strings.HasPrefix(rel, "scripts/lib/")
		executable := info.Mode()&0o111 != 0
		if isLibrary {
			if executable {
				t.Fatalf("%s is a sourced library and should not be executable", rel)
			}
			return nil
		}
		if !executable {
			t.Fatalf("%s is an entrypoint and must be executable", rel)
		}
		if !strings.Contains(text, "set -e") && !strings.Contains(text, "set -E") {
			t.Fatalf("%s must enable shell error handling", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCorrectnessWorkflowsWireSmokeNightlyAndChaosTargets(t *testing.T) {
	root := repoRoot(t)
	goWorkflow := readRepoFile(t, root, ".github/workflows/go.yml")
	requireContains(t, goWorkflow, "make test-correctness-smoke", "go workflow should run bounded correctness smoke before the full suite")
	requireContains(t, goWorkflow, "make test", "go workflow should run the full package suite")
	if strings.Index(goWorkflow, "make test-correctness-smoke") > strings.Index(goWorkflow, "make test") {
		t.Fatal("go workflow should run correctness smoke before the full test suite")
	}

	nightlyWorkflow := readRepoFile(t, root, ".github/workflows/nightly-correctness.yml")
	requireContains(t, nightlyWorkflow, "make test-correctness-nightly", "nightly correctness workflow should call the nightly matrix")

	tlaWorkflow := readRepoFile(t, root, ".github/workflows/tla.yml")
	requireContains(t, tlaWorkflow, "make test-tla-smoke", "TLA PR workflow should run smoke model checks")
	requireContains(t, tlaWorkflow, "make test-tla-nightly", "TLA scheduled workflow should run the full model matrix")

	chaosWorkflow := readRepoFile(t, root, ".github/workflows/docker-chaos.yml")
	requireContains(t, chaosWorkflow, "./scripts/chaos/docker_fsmeta_history.sh", "Docker chaos workflow should run the fsmeta history checker")

	soakWorkflow := readRepoFile(t, root, ".github/workflows/soak-correctness.yml")
	requireContains(t, soakWorkflow, "./scripts/soak/fsmeta_soak.sh", "soak workflow should run the fsmeta soak harness")
}

func TestMakefileCorrectnessTargetsStayReachable(t *testing.T) {
	root := repoRoot(t)
	makefile := readRepoFile(t, root, "Makefile")
	for _, target := range []string{
		"test-contract-smoke:",
		"test-raftstore-contract-smoke:",
		"test-history-smoke:",
		"test-model-smoke:",
		"test-crash-matrix-smoke:",
		"test-deterministic-simulation-smoke:",
		"test-correctness-smoke:",
		"test-correctness-nightly:",
		"test-docker-chaos:",
		"test-soak-smoke:",
		"test-tla-smoke:",
		"test-tla-nightly:",
	} {
		requireContains(t, makefile, target, "missing Makefile target")
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(wd)
}

func readRepoFile(t *testing.T, root, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func requireContains(t *testing.T, text, needle, message string) {
	t.Helper()
	if !strings.Contains(text, needle) {
		t.Fatalf("%s: missing %q", message, needle)
	}
}
