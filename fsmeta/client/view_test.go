// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	"github.com/stretchr/testify/require"
)

type viewFake struct {
	attrs   map[model.InodeID]model.InodeRecord
	dentry  map[string]model.DentryAttrPair
	readdir map[model.InodeID][]model.DentryAttrPair

	getAttrVersions    []uint64
	lookupVersions     []uint64
	readDirVersions    []uint64
	created            []model.CreateRequest
	removed            []model.RemoveRequest
	removedDirectories []model.RemoveDirectoryRequest
	renamed            []model.RenameRequest
	renameReplaced     []model.RenameReplaceRequest
	watched            []observe.WatchRequest
}

func newViewFake() *viewFake {
	f := &viewFake{
		attrs:   make(map[model.InodeID]model.InodeRecord),
		dentry:  make(map[string]model.DentryAttrPair),
		readdir: make(map[model.InodeID][]model.DentryAttrPair),
	}
	f.attrs[1] = model.InodeRecord{Inode: 1, Type: model.InodeTypeDirectory}
	f.putDentry(1, "input", 2, model.InodeTypeDirectory)
	f.putDentry(1, "output", 3, model.InodeTypeDirectory)
	f.putDentry(1, "secret", 4, model.InodeTypeDirectory)
	f.putDentry(2, "file", 5, model.InodeTypeFile)
	return f
}

func (f *viewFake) putDentry(parent model.InodeID, name string, inode model.InodeID, typ model.InodeType) {
	record := model.DentryRecord{Parent: parent, Name: name, Inode: inode, Type: typ}
	attr := model.InodeRecord{Inode: inode, Type: typ}
	f.attrs[inode] = attr
	pair := model.DentryAttrPair{Dentry: record, Inode: attr}
	f.dentry[viewDentryKey(parent, name)] = pair
	f.readdir[parent] = append(f.readdir[parent], pair)
}

func (f *viewFake) LookupPlus(_ context.Context, req model.LookupRequest) (model.DentryAttrPair, error) {
	f.lookupVersions = append(f.lookupVersions, req.SnapshotVersion)
	pair, ok := f.dentry[viewDentryKey(req.Parent, req.Name)]
	if !ok {
		return model.DentryAttrPair{}, model.ErrNotFound
	}
	return pair, nil
}

func (f *viewFake) GetAttr(_ context.Context, req model.GetAttrRequest) (model.InodeRecord, error) {
	f.getAttrVersions = append(f.getAttrVersions, req.SnapshotVersion)
	attr, ok := f.attrs[req.Inode]
	if !ok {
		return model.InodeRecord{}, model.ErrNotFound
	}
	return attr, nil
}

func (f *viewFake) BatchGetAttr(_ context.Context, req model.BatchGetAttrRequest) ([]model.InodeRecord, error) {
	out := make([]model.InodeRecord, 0, len(req.Inodes))
	for _, inode := range req.Inodes {
		attr, ok := f.attrs[inode]
		if !ok {
			return nil, model.ErrNotFound
		}
		out = append(out, attr)
	}
	return out, nil
}

func (f *viewFake) ReadDirPlus(_ context.Context, req model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	f.readDirVersions = append(f.readDirVersions, req.SnapshotVersion)
	return append([]model.DentryAttrPair(nil), f.readdir[req.Parent]...), nil
}

func (f *viewFake) WatchSubtree(_ context.Context, req observe.WatchRequest) (WatchSubscription, error) {
	f.watched = append(f.watched, req)
	return nil, nil
}

func (f *viewFake) Create(_ context.Context, req model.CreateRequest) (model.CreateResult, error) {
	f.created = append(f.created, req)
	inode := model.InodeID(100 + len(f.created))
	attr := req.Attrs.InodeRecord(inode)
	record := model.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: inode, Type: attr.Type}
	return model.CreateResult{Dentry: record, Inode: attr}, nil
}

func (f *viewFake) Rename(_ context.Context, req model.RenameRequest) error {
	f.renamed = append(f.renamed, req)
	return nil
}

func (f *viewFake) RenameReplace(_ context.Context, req model.RenameReplaceRequest) (model.RenameReplaceResult, error) {
	f.renameReplaced = append(f.renameReplaced, req)
	return model.RenameReplaceResult{}, nil
}

func (f *viewFake) Remove(_ context.Context, req model.RemoveRequest) (model.RemoveResult, error) {
	f.removed = append(f.removed, req)
	return model.RemoveResult{}, nil
}

func (f *viewFake) RemoveDirectory(_ context.Context, req model.RemoveDirectoryRequest) error {
	f.removedDirectories = append(f.removedDirectories, req)
	return nil
}

type pathLookupViewFake struct {
	*viewFake
	lookupPathReqs []model.LookupPathRequest
}

func (f *pathLookupViewFake) LookupPath(_ context.Context, req model.LookupPathRequest) (model.DentryAttrPair, error) {
	f.lookupPathReqs = append(f.lookupPathReqs, req)
	parent := req.RootInode
	var current model.DentryAttrPair
	for _, part := range strings.Split(req.Path, "/") {
		pair, ok := f.dentry[viewDentryKey(parent, part)]
		if !ok {
			return model.DentryAttrPair{}, model.ErrNotFound
		}
		current = pair
		parent = pair.Inode.Inode
	}
	return current, nil
}

func TestCreateViewEnforcesScopedReadAndWriteRules(t *testing.T) {
	backend := newViewFake()
	view, err := CreateView(context.Background(), backend, model.CreateViewRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
		AccessRules: []model.ViewAccessRule{
			{Prefix: "input", Mode: model.ViewAccessReadOnly},
			{Prefix: "output", Mode: model.ViewAccessReadWrite},
		},
	})
	require.NoError(t, err)
	require.Len(t, view.Descriptor().AccessRules, 2)

	pair, err := view.LookupPlus(context.Background(), "input/file")
	require.NoError(t, err)
	require.Equal(t, model.InodeID(5), pair.Inode.Inode)

	attr, err := view.GetAttr(context.Background(), pair.Inode.Inode)
	require.NoError(t, err)
	require.Equal(t, model.InodeID(5), attr.Inode)

	_, err = view.LookupPlus(context.Background(), "secret/hidden")
	require.ErrorIs(t, err, model.ErrViewAccessDenied)

	_, err = view.Create(context.Background(), "input/new", model.CreateAttrs{Type: model.InodeTypeFile})
	require.ErrorIs(t, err, model.ErrViewAccessDenied)
	require.Empty(t, backend.created)

	created, err := view.Create(context.Background(), "output/new", model.CreateAttrs{Type: model.InodeTypeFile})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(3), backend.created[0].Parent)
	require.Equal(t, "new", backend.created[0].Name)
	require.Equal(t, created.Inode.Inode, backend.created[0].Attrs.InodeRecord(created.Inode.Inode).Inode)
}

func TestViewUsesLookupPathWhenBackendSupportsIt(t *testing.T) {
	backend := &pathLookupViewFake{viewFake: newViewFake()}
	view, err := CreateView(context.Background(), backend, model.CreateViewRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
		AccessRules: []model.ViewAccessRule{{
			Prefix: "input",
			Mode:   model.ViewAccessReadOnly,
		}},
	})
	require.NoError(t, err)

	pair, err := view.LookupPlus(context.Background(), "input/file")
	require.NoError(t, err)
	require.Equal(t, model.InodeID(5), pair.Inode.Inode)
	require.NotEmpty(t, backend.lookupPathReqs)
	require.Equal(t, model.LookupPathRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
		Path:      "input/file",
	}, backend.lookupPathReqs[len(backend.lookupPathReqs)-1])
	require.Empty(t, backend.lookupVersions)
}

func TestSnapshotViewBindsReadsAndRejectsMutations(t *testing.T) {
	backend := newViewFake()
	view, err := CreateReadOnlySnapshotView(context.Background(), backend, model.SnapshotSubtreeToken{
		Mount:       "vol",
		RootInode:   model.RootInode,
		ReadVersion: 99,
	})
	require.NoError(t, err)

	_, err = view.LookupPlus(context.Background(), "input/file")
	require.NoError(t, err)
	require.Equal(t, []uint64{99, 99}, backend.lookupVersions)

	entries, err := view.ReadDirPlus(context.Background(), ViewReadDirRequest{Path: "input"})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(99), backend.readDirVersions[len(backend.readDirVersions)-1])

	_, err = view.Create(context.Background(), "output/new", model.CreateAttrs{Type: model.InodeTypeFile})
	require.ErrorIs(t, err, model.ErrViewAccessDenied)
	require.Empty(t, backend.created)
}

func TestViewRejectsUnknownInodeGetAttr(t *testing.T) {
	view, err := CreateView(context.Background(), newViewFake(), model.CreateViewRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
		AccessRules: []model.ViewAccessRule{{
			Prefix: "",
			Mode:   model.ViewAccessReadOnly,
		}},
	})
	require.NoError(t, err)

	_, err = view.GetAttr(context.Background(), 999)
	require.ErrorIs(t, err, model.ErrViewAccessDenied)
}

func TestViewUsesLongestPrefixRule(t *testing.T) {
	backend := newViewFake()
	backend.putDentry(2, "scratch", 6, model.InodeTypeDirectory)
	view, err := CreateView(context.Background(), backend, model.CreateViewRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
		AccessRules: []model.ViewAccessRule{
			{Prefix: "input", Mode: model.ViewAccessReadOnly},
			{Prefix: "input/scratch", Mode: model.ViewAccessReadWrite},
		},
	})
	require.NoError(t, err)

	_, err = view.Create(context.Background(), "input/file2", model.CreateAttrs{Type: model.InodeTypeFile})
	require.ErrorIs(t, err, model.ErrViewAccessDenied)

	_, err = view.Create(context.Background(), "input/scratch/file2", model.CreateAttrs{Type: model.InodeTypeFile})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(6), backend.created[0].Parent)
}

func viewDentryKey(parent model.InodeID, name string) string {
	return fmt.Sprintf("%d/%s", parent, name)
}
