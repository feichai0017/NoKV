package gcp_test

import (
	"os"
	"strings"
	"testing"
)

func TestCreateClusterPassesStableMetaRootTickInterval(t *testing.T) {
	common, err := os.ReadFile("lib/common.sh")
	if err != nil {
		t.Fatalf("read common.sh: %v", err)
	}
	createCluster, err := os.ReadFile("create-cluster.sh")
	if err != nil {
		t.Fatalf("read create-cluster.sh: %v", err)
	}

	if !strings.Contains(string(common), `: "${NOKV_META_ROOT_TICK_INTERVAL:=1000ms}"`) {
		t.Fatalf("common.sh must default NOKV_META_ROOT_TICK_INTERVAL to 1000ms")
	}
	if !strings.Contains(string(createCluster), `NOKV_META_ROOT_TICK_INTERVAL="$NOKV_META_ROOT_TICK_INTERVAL"`) {
		t.Fatalf("startup script must propagate NOKV_META_ROOT_TICK_INTERVAL")
	}
	if !strings.Contains(string(createCluster), `--tick-interval="\$NOKV_META_ROOT_TICK_INTERVAL"`) {
		t.Fatalf("meta-root container must pass --tick-interval from NOKV_META_ROOT_TICK_INTERVAL")
	}
}
