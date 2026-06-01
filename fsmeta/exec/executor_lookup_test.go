// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestExecutorCreateAndLookup(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	result, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(22), result.Inode.Inode)

	record, err := executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{
		Parent: model.RootInode,
		Name:   "file",
		Inode:  22,
		Type:   model.InodeTypeFile,
	}, record)

	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 4)
	require.True(t, runner.mutations[0][1].AssertionNotExist)
	require.True(t, runner.mutations[0][2].AssertionNotExist)
	require.True(t, runner.mutations[0][3].AssertionNotExist)
	parentKind, err := layout.KeyKindOf(runner.mutations[0][3].Key)
	require.NoError(t, err)
	require.Equal(t, layout.KeyKindParent, parentKind)
}

func TestExecutorLookupReturnsNotFound(t *testing.T) {
	executor, err := newTestExecutor(newFakeRunner())
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "missing",
	})
	require.ErrorIs(t, err, model.ErrNotFound)
}

func TestExecutorReadDirConsumesPlanCursorAndLimit(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedDentry(t, runner, "vol", 7, "b", 22)
	seedDentry(t, runner, "vol", 7, "c", 23)
	seedDentry(t, runner, "vol", 8, "outside", 99)

	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	records, err := executor.ReadDir(context.Background(), model.ReadDirRequest{
		Mount:      "vol",
		Parent:     7,
		StartAfter: "a",
		Limit:      1,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryRecord{{
		Parent: 7,
		Name:   "b",
		Inode:  22,
		Type:   model.InodeTypeFile,
	}}, records)
}

func TestExecutorReadDirRetriesLiveLock(t *testing.T) {
	runner := newFakeRunner()
	runner.scanErrs = []error{metadataLockedError("vol", 7, "a")}
	seedDentry(t, runner, "vol", 7, "a", 21)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	records, err := executor.ReadDir(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, []uint64{1, 2}, runner.scanVersions)
	requireStatUint(t, executor.Stats(), "read_retries_total", 1)
	requireStatUint(t, executor.Stats(), "read_retry_exhausted_total", 0)
}

func TestExecutorReadDirExhaustsRetriesOnLiveLock(t *testing.T) {
	runner := newFakeRunner()
	for range maxReadContentionRetries + 1 {
		runner.scanErrs = append(runner.scanErrs, metadataLockedError("vol", 7, "a"))
	}
	seedDentry(t, runner, "vol", 7, "a", 21)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	_, err = executor.ReadDir(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.Error(t, err)
	wantVersions := make([]uint64, maxReadContentionRetries+1)
	for i := range wantVersions {
		wantVersions[i] = uint64(i + 1)
	}
	require.Equal(t, wantVersions, runner.scanVersions)
	requireStatUint(t, executor.Stats(), "read_retries_total", uint64(maxReadContentionRetries))
	requireStatUint(t, executor.Stats(), "read_retry_exhausted_total", 1)
}

func TestExecutorReadDirPlusReturnsDentriesAndAttrs(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     21,
		Type:      model.InodeTypeFile,
		Size:      4096,
		Mode:      0o644,
		LinkCount: 1,
	})
	seedDentryType(t, runner, "vol", 7, "b", 22, model.InodeTypeDirectory)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     22,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 2,
	})

	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	pairs, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryAttrPair{
		{
			Dentry: model.DentryRecord{Parent: 7, Name: "a", Inode: 21, Type: model.InodeTypeFile},
			Inode: model.InodeRecord{
				Inode:     21,
				Type:      model.InodeTypeFile,
				Size:      4096,
				Mode:      0o644,
				LinkCount: 1,
			},
		},
		{
			Dentry: model.DentryRecord{Parent: 7, Name: "b", Inode: 22, Type: model.InodeTypeDirectory},
			Inode: model.InodeRecord{
				Inode:     22,
				Type:      model.InodeTypeDirectory,
				Mode:      0o755,
				LinkCount: 2,
			},
		},
	}, pairs)
}

func TestExecutorReadDirPlusUsesDentryProjectionForSingleLinkFiles(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{21}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact",
		Attrs: model.CreateAttrs{
			Type:          model.InodeTypeFile,
			Size:          4096,
			Mode:          0o644,
			CreatedUnixNs: 10,
			UpdatedUnixNs: 20,
			OpaqueAttrs:   []byte(`{"body":"manifest"}`),
		},
	})
	require.NoError(t, err)
	runner.batchVersions = nil

	pairs, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryAttrPair{{
		Dentry: model.DentryRecord{Parent: model.RootInode, Name: "artifact", Inode: 21, Type: model.InodeTypeFile},
		Inode: model.InodeRecord{
			Inode:         21,
			Type:          model.InodeTypeFile,
			Size:          4096,
			Mode:          0o644,
			LinkCount:     1,
			CreatedUnixNs: 10,
			UpdatedUnixNs: 20,
			OpaqueAttrs:   []byte(`{"body":"manifest"}`),
		},
	}}, pairs)
	require.Empty(t, runner.batchVersions)
	requireStatUint(t, executor.Stats(), "readdirplus_dentry_count", 1)
	requireStatUint(t, executor.Stats(), "readdirplus_inode_batch_count", 0)
	requireStatUint(t, executor.Stats(), "readdirplus_projection_hit_total", 1)
}

func TestExecutorReadDirPlusFallsBackForDirectoryProjection(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{21}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "dir",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeDirectory, Mode: 0o755},
	})
	require.NoError(t, err)
	runner.batchVersions = nil

	pairs, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	require.Equal(t, model.InodeTypeDirectory, pairs[0].Inode.Type)
	require.Len(t, runner.batchVersions, 1)
	requireStatUint(t, executor.Stats(), "readdirplus_dentry_count", 1)
	requireStatUint(t, executor.Stats(), "readdirplus_inode_batch_count", 1)
	requireStatUint(t, executor.Stats(), "readdirplus_projection_hit_total", 0)
}

func TestExecutorLookupPlusReturnsDentryAndAttrs(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     21,
		Type:      model.InodeTypeFile,
		Size:      4096,
		Mode:      0o644,
		LinkCount: 1,
	})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	pair, err := executor.LookupPlus(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "a",
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryAttrPair{
		Dentry: model.DentryRecord{Parent: 7, Name: "a", Inode: 21, Type: model.InodeTypeFile},
		Inode: model.InodeRecord{
			Inode:     21,
			Type:      model.InodeTypeFile,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}, pair)
}

func TestExecutorReadDirPlusMissingInodeReturnsNotFound(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	_, err = executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.ErrorIs(t, err, model.ErrNotFound)
}
