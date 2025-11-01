package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestServeFromConfigDockerScopePeers(t *testing.T) {
	t.Helper()
	tempBin := t.TempDir()
	recordPath := filepath.Join(tempBin, "nokv_args.txt")

	nokvConfig := `#!/usr/bin/env bash
cmd=$1
shift
case "$cmd" in
  stores)
    cat <<'EOF'
1 127.0.0.1:20170 127.0.0.1:20170 0.0.0.0:30170 store1-docker
2 127.0.0.1:20171 127.0.0.1:20171 0.0.0.0:30171 store2-docker
EOF
    ;;
  regions)
    cat <<'EOF'
1 - - 1 1 1:101,2:201 1
EOF
    ;;
  tso)
    echo "- -"
    ;;
esac
`
	writeExecutable(t, filepath.Join(tempBin, "nokv-config"), nokvConfig)

	nokvStub := fmt.Sprintf(`#!/usr/bin/env bash
echo "$*">"%s"
`, recordPath)
	writeExecutable(t, filepath.Join(tempBin, "nokv"), nokvStub)

	configPath := filepath.Join(tempBin, "raft_config.json")
	if err := os.WriteFile(configPath, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	workdir := filepath.Join(tempBin, "cluster")
	cmd := exec.Command("bash", "serve_from_config.sh",
		"--config", configPath,
		"--store-id", "1",
		"--workdir", workdir,
		"--scope", "docker",
		"--no-raft-debug-log",
	)
	cmd.Dir = "."
	env := os.Environ()
	env = append(env, fmt.Sprintf("PATH=%s:%s", tempBin, os.Getenv("PATH")))
	cmd.Env = env

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("serve_from_config failed: %v\n%s", err, string(output))
	}

	data, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read nokv args: %v", err)
	}
	args := string(data)
	if !strings.Contains(args, "--addr 0.0.0.0:30170") {
		t.Fatalf("expected docker listen addr, got %q", args)
	}
	if !strings.Contains(args, "--peer 201=store2-docker") {
		t.Fatalf("expected peer mapping for store2, got %q", args)
	}
	if strings.Contains(args, "127.0.0.1:20171") {
		t.Fatalf("unexpected host address in args: %q", args)
	}
}

func writeExecutable(t *testing.T, path, script string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
