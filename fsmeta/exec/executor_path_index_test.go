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

func TestExecutorLookupPathUsesDerivedIndexForRootChild(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithPathIndexMaintenance(true),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact.json",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096, Mode: 0o644},
	})
	require.NoError(t, err)

	key, err := layout.EncodePathIndexKey(testMountIdentity, model.RootInode, "artifact.json")
	require.NoError(t, err)
	value, ok := runner.data[string(key)]
	require.True(t, ok)
	record, err := layout.DecodePathIndexValue(value)
	require.NoError(t, err)
	require.Equal(t, model.PathIndexRecord{
		RootInode:     model.RootInode,
		Path:          "artifact.json",
		Parent:        model.RootInode,
		Name:          "artifact.json",
		Inode:         22,
		Type:          model.InodeTypeFile,
		DentryVersion: 2,
	}, record)

	pair, err := executor.LookupPath(context.Background(), model.LookupPathRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
		Path:      "artifact.json",
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{Parent: model.RootInode, Name: "artifact.json", Inode: 22, Type: model.InodeTypeFile}, pair.Dentry)
	require.Equal(t, model.InodeID(22), pair.Inode.Inode)
}

func TestExecutorLookupPathFallsBackWhenDerivedIndexDisabled(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact.json",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096, Mode: 0o644},
	})
	require.NoError(t, err)

	key, err := layout.EncodePathIndexKey(testMountIdentity, model.RootInode, "artifact.json")
	require.NoError(t, err)
	_, ok := runner.data[string(key)]
	require.False(t, ok)

	pair, err := executor.LookupPath(context.Background(), model.LookupPathRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
		Path:      "artifact.json",
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{Parent: model.RootInode, Name: "artifact.json", Inode: 22, Type: model.InodeTypeFile}, pair.Dentry)
}

func TestExecutorRenameMaintainsRootPathIndex(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithPathIndexMaintenance(true),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "old",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: model.RootInode,
		FromName:   "old",
		ToParent:   model.RootInode,
		ToName:     "new",
	})
	require.NoError(t, err)

	oldKey, err := layout.EncodePathIndexKey(testMountIdentity, model.RootInode, "old")
	require.NoError(t, err)
	_, ok := runner.data[string(oldKey)]
	require.False(t, ok)
	newKey, err := layout.EncodePathIndexKey(testMountIdentity, model.RootInode, "new")
	require.NoError(t, err)
	_, ok = runner.data[string(newKey)]
	require.True(t, ok)

	_, err = executor.LookupPath(context.Background(), model.LookupPathRequest{Mount: "vol", RootInode: model.RootInode, Path: "old"})
	require.ErrorIs(t, err, model.ErrNotFound)
	pair, err := executor.LookupPath(context.Background(), model.LookupPathRequest{Mount: "vol", RootInode: model.RootInode, Path: "new"})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(22), pair.Inode.Inode)
}

func TestExecutorRemoveDeletesRootPathIndex(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithPathIndexMaintenance(true),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	key, err := layout.EncodePathIndexKey(testMountIdentity, model.RootInode, "artifact")
	require.NoError(t, err)
	_, ok := runner.data[string(key)]
	require.True(t, ok)

	_, err = executor.Remove(context.Background(), model.RemoveRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "artifact",
	})
	require.NoError(t, err)
	_, ok = runner.data[string(key)]
	require.False(t, ok)

	_, err = executor.LookupPath(context.Background(), model.LookupPathRequest{Mount: "vol", RootInode: model.RootInode, Path: "artifact"})
	require.ErrorIs(t, err, model.ErrNotFound)
}

func TestExecutorLookupPathRejectsStaleDescendantIndexAfterDirectoryRename(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{21, 22}}),
		WithPathIndexMaintenance(true),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "dir",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeDirectory, Mode: 0o755},
	})
	require.NoError(t, err)
	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 21,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	_, err = executor.LookupPath(context.Background(), model.LookupPathRequest{Mount: "vol", RootInode: model.RootInode, Path: "dir/file"})
	require.NoError(t, err)

	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: model.RootInode,
		FromName:   "dir",
		ToParent:   model.RootInode,
		ToName:     "renamed",
	})
	require.NoError(t, err)

	staleKey, err := layout.EncodePathIndexKey(testMountIdentity, model.RootInode, "dir/file")
	require.NoError(t, err)
	_, ok := runner.data[string(staleKey)]
	require.True(t, ok, "descendant path_index is intentionally derived and may lag subtree rename")
	_, err = executor.LookupPath(context.Background(), model.LookupPathRequest{Mount: "vol", RootInode: model.RootInode, Path: "dir/file"})
	require.ErrorIs(t, err, model.ErrNotFound)
	pair, err := executor.LookupPath(context.Background(), model.LookupPathRequest{Mount: "vol", RootInode: model.RootInode, Path: "renamed/file"})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(22), pair.Inode.Inode)
}
