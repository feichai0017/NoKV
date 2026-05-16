// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/contract"
	"github.com/stretchr/testify/require"
)

func TestOpenCreateLookupSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rt, err := Open(ctx, Options{WorkDir: dir, Mount: testMount()})
	require.NoError(t, err)
	require.NotNil(t, rt.Peras)

	created, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "agent-state",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	})
	require.NoError(t, err)
	require.NoError(t, rt.Close())

	rt, err = Open(ctx, Options{WorkDir: dir, Mount: testMount()})
	require.NoError(t, err)
	require.NotNil(t, rt.Peras)
	defer func() { require.NoError(t, rt.Close()) }()

	got, err := rt.Executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "agent-state",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry, got)

	next, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "next",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Greater(t, next.Inode.Inode, created.Inode.Inode)
}

func TestOpenUsesShardedLocalDBByDefault(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	require.Len(t, rt.DB.LSMWALs(), 4)
}

func TestLocalInodeAllocatorChoosesWorkspaceShard(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	created, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "workspace-a",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory},
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ChooseWorkspaceBucket(testMount(), "workspace-a"), fsmeta.BucketForInodeID(created.Inode.Inode))
}

func TestOpenCanDisablePeras(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{
		WorkDir:   t.TempDir(),
		Mount:     testMount(),
		PerasMode: PerasModeDisabled,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()
	require.Nil(t, rt.Peras)

	created, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "direct",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	got, err := rt.Executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "direct",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry, got)
}

func TestLocalRuntimePassesFSMetaContract(t *testing.T) {
	ctx := context.Background()
	model := contract.NewModel("vol")
	rt, err := Open(ctx, Options{
		WorkDir: t.TempDir(),
		Mount:   testMount(),
		Clock: func() time.Time {
			return time.Unix(0, model.NowUnixNs)
		},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	mapped, err := contract.NewInodeMappingExecutor(rt.Executor)
	require.NoError(t, err)
	err = contract.Run(ctx, mapped, model, contract.GenerateScript(3, 60))
	require.NoError(t, err)
}

func TestLocalRuntimePublishesWatchEvents(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	sub, err := rt.Watcher.Subscribe(ctx, fsmeta.WatchRequest{
		Mount:              "vol",
		RootInode:          fsmeta.RootInode,
		BackPressureWindow: 4,
	})
	require.NoError(t, err)
	defer sub.Close()

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "watched",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	wantKey, err := fsmeta.EncodeDentryKey(testMount(), fsmeta.RootInode, "watched")
	require.NoError(t, err)
	evt := requireWatchEvent(t, sub)
	require.Equal(t, wantKey, evt.Key)
	require.Equal(t, fsmeta.WatchEventSourcePerasVisible, evt.Source)
	require.Equal(t, localPerasTerm, evt.Cursor.Term)
	require.NotZero(t, evt.Cursor.Index)

	stats := rt.Watcher.Stats()
	require.GreaterOrEqual(t, stats["events_total"].(uint64), uint64(1))
	require.Equal(t, uint64(1), stats["delivered_total"])
}

func TestLocalRuntimeWatchReplaysAfterResumeCursor(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	firstSub, err := rt.Watcher.Subscribe(ctx, fsmeta.WatchRequest{
		Mount:     "vol",
		RootInode: fsmeta.RootInode,
	})
	require.NoError(t, err)
	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "first",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	first := requireWatchEvent(t, firstSub)
	firstSub.Close()

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "second",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	resumed, err := rt.Watcher.Subscribe(ctx, fsmeta.WatchRequest{
		Mount:        "vol",
		RootInode:    fsmeta.RootInode,
		ResumeCursor: first.Cursor,
	})
	require.NoError(t, err)
	defer resumed.Close()

	wantKey, err := fsmeta.EncodeDentryKey(testMount(), fsmeta.RootInode, "second")
	require.NoError(t, err)
	replayed := requireWatchEvent(t, resumed)
	require.Equal(t, wantKey, replayed.Key)
	require.Greater(t, replayed.Cursor.Index, first.Cursor.Index)
}

func TestLocalRuntimePublishesAndRetiresSnapshots(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	token, err := rt.Executor.SnapshotSubtree(ctx, fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: fsmeta.RootInode,
	})
	require.NoError(t, err)
	require.NoError(t, rt.Snapshots.PublishSnapshotSubtree(ctx, token))
	require.Equal(t, 1, rt.Snapshots.Stats()["active_snapshots"])

	resolved, err := rt.Executor.ResolveSnapshotSubtreeToken(ctx, token)
	require.NoError(t, err)
	require.NoError(t, rt.Snapshots.RetireSnapshotSubtree(ctx, resolved))
	require.Equal(t, 0, rt.Snapshots.Stats()["active_snapshots"])
}

func TestLocalRuntimeMaintainsQuotaUsage(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "quota-file",
		Attrs: fsmeta.CreateAttrs{
			Type: fsmeta.InodeTypeFile,
			Size: 4096,
		},
	})
	require.NoError(t, err)

	rootUsage, err := rt.Executor.GetQuotaUsage(ctx, fsmeta.QuotaUsageRequest{
		Mount: "vol",
		Scope: fsmeta.RootInode,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.UsageRecord{Bytes: 4096, Inodes: 1}, rootUsage)

	mountUsage, err := rt.Executor.GetQuotaUsage(ctx, fsmeta.QuotaUsageRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, rootUsage, mountUsage)
}

func TestLocalRuntimePerasReadDirPlusSeesDirectCreateAfterEmptyRead(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rt, err := Open(ctx, Options{
		WorkDir:            filepath.Join(dir, "db"),
		Mount:              testMount(),
		PerasHolderID:      "local-holder",
		PerasVisibleLogDir: filepath.Join(dir, "visible-log"),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	pairs, err := rt.Executor.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Empty(t, pairs)

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "after-empty-read",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	pairs, err = rt.Executor.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	require.Equal(t, "after-empty-read", pairs[0].Dentry.Name)
}

func TestLocalRuntimeDefaultPerasKeepsQuotaWritesOnVisiblePath(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()
	require.NotNil(t, rt.Peras)

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "before",
		Attrs: fsmeta.CreateAttrs{
			Type: fsmeta.InodeTypeFile,
			Size: 4096,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), perasVisibleSuccessTotal(t, rt.Executor.Stats()), "local quota is read-derived, so create can use Peras visible commit")

	require.NoError(t, rt.Executor.Rename(ctx, fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   "before",
		ToParent:   fsmeta.RootInode,
		ToName:     "after",
	}))
	require.Equal(t, uint64(2), perasVisibleSuccessTotal(t, rt.Executor.Stats()))

	require.NoError(t, rt.Executor.Unlink(ctx, fsmeta.UnlinkRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "after",
	}))
	require.Equal(t, uint64(3), perasVisibleSuccessTotal(t, rt.Executor.Stats()))
	_, err = rt.Executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "after",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	usage, err := rt.Executor.GetQuotaUsage(ctx, fsmeta.QuotaUsageRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.UsageRecord{}, usage)
}

func TestLocalRuntimePerasBypassesSegmentWitness(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()
	require.NotNil(t, rt.Peras)

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "local-visible",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.NoError(t, rt.Peras.FlushDurable(ctx))

	stats := rt.Peras.Stats()
	require.Equal(t, "bypass", stats["witness_mode"])
	require.Equal(t, 0, stats["witness_count"])
	require.Equal(t, 0, stats["quorum"])
	require.Equal(t, uint64(0), stats["witness_batch_total"])
	require.Equal(t, uint64(0), stats["witness_quorum_total"])
	require.Equal(t, uint64(1), stats["visible_log_apply_marker_total"])
	require.Equal(t, 0, stats["pending"])
}

func TestLocalRuntimePerasVisibleCommitRecoversInstalledCatalog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	opts := Options{
		WorkDir:            filepath.Join(dir, "db"),
		Mount:              testMount(),
		PerasHolderID:      "local-holder",
		PerasVisibleLogDir: filepath.Join(dir, "visible-log"),
	}
	rt, err := Open(ctx, opts)
	require.NoError(t, err)
	require.NotNil(t, rt.Peras)

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "before",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), perasVisibleSuccessTotal(t, rt.Executor.Stats()), "local quota is read-derived, so create can use Peras visible commit")

	require.NoError(t, rt.Executor.Rename(ctx, fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   "before",
		ToParent:   fsmeta.RootInode,
		ToName:     "after",
	}))
	require.Equal(t, uint64(2), perasVisibleSuccessTotal(t, rt.Executor.Stats()))
	got, err := rt.Executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "after",
	})
	require.NoError(t, err)
	require.Equal(t, "after", got.Name)
	require.NoError(t, rt.Peras.FlushDurable(ctx))
	require.NoError(t, rt.Close())

	reopened, err := Open(ctx, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	got, err = reopened.Executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "after",
	})
	require.NoError(t, err)
	require.Equal(t, "after", got.Name)
	_, err = reopened.Executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "before",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
}

func perasVisibleSuccessTotal(t *testing.T, stats map[string]any) uint64 {
	t.Helper()
	visible, ok := stats["peras_visible_commit"].(map[string]any)
	require.True(t, ok)
	total, ok := visible["success_total"].(uint64)
	require.True(t, ok)
	return total
}

func requireWatchEvent(t *testing.T, sub fsmeta.WatchSubscription) fsmeta.WatchEvent {
	t.Helper()
	select {
	case evt, ok := <-sub.Events():
		require.True(t, ok, "watch subscription closed: %v", sub.Err())
		return evt
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch event")
		return fsmeta.WatchEvent{}
	}
}
