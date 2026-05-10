package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

// TestAtomicFastPathAdmittedForWorkspaceCreates locks in the testcluster
// wiring that fsmeta integration depends on: with the
// fsmeta.UserKeyShape extractor configured (production parity), Create
// operations under a workspace directory consistently land both the
// dentry and the new inode key in the same LSM shard, so the Percolator
// 1PC fast path is admitted on every attempt.
//
// Without the extractor, workspace-parented Creates routed dentry and
// inode entries to different LSM shards, falling back to 2PC ~75% of
// the time and inflating Create latency by ~2.35x. This test is the
// regression guard for that wiring.
func TestAtomicFastPathAdmittedForWorkspaceCreates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rt := openRealClusterRuntime(t, ctx)
	cli, cleanup := openFSMetadataClient(t, ctx, rt.executor)
	defer cleanup()

	ws, err := cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "workspace-fastpath",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory, Mode: 0o755},
	})
	require.NoError(t, err)

	const ops = 64
	for i := range ops {
		_, err := cli.Create(ctx, fsmeta.CreateRequest{
			Mount:  "vol",
			Parent: ws.Inode.Inode,
			Name:   fmt.Sprintf("entry-%04d", i),
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 4096, Mode: 0o644},
		})
		require.NoError(t, err)
	}

	stats := rt.executor.Stats()
	fastPath := stats["atomic_fastpath"].(map[string]any)
	createStats := fastPath["create"].(map[string]uint64)
	attempts := createStats["attempt_total"]
	successes := createStats["success_total"]
	fallbacks := createStats["fallback_total"]

	// Workspace-parented Creates must hit the fast path on every attempt.
	// The extra +1 attempt accounts for the workspace dir Create itself,
	// which is root-parented and intentionally falls back.
	require.GreaterOrEqual(t, attempts, uint64(ops),
		"expected at least %d attempts, got %d", ops, attempts)
	require.GreaterOrEqual(t, successes, uint64(ops-1),
		"expected at least %d 1PC successes, got %d (fallbacks=%d) — "+
			"is testcluster.NodeConfig.UserKeyShapeExtractor wired?",
		ops-1, successes, fallbacks)
}
