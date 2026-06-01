// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

const pathIndexScanLimit uint32 = 8

func (e *Executor) pathIndexPutMutations(ctx context.Context, mount model.MountIdentity, dentry model.DentryRecord, startVersion, commitVersion uint64) ([]*backend.Mutation, error) {
	record, ok, err := e.pathIndexRecordForDentry(ctx, mount, dentry, startVersion, commitVersion)
	if err != nil || !ok {
		return nil, err
	}
	return pathIndexPutMutationsForRecord(mount, record)
}

func (e *Executor) pathIndexDeleteMutations(ctx context.Context, mount model.MountIdentity, dentry model.DentryRecord, startVersion uint64) ([]*backend.Mutation, error) {
	record, ok, err := e.pathIndexRecordForDentry(ctx, mount, dentry, startVersion, 0)
	if err != nil || !ok {
		return nil, err
	}
	pathKey, inodeKey, err := pathIndexKeysForRecord(mount, record)
	if err != nil {
		return nil, err
	}
	return []*backend.Mutation{
		{Op: backend.MutationDelete, Key: pathKey},
		{Op: backend.MutationDelete, Key: inodeKey},
	}, nil
}

func (e *Executor) pathIndexRecordForDentry(ctx context.Context, mount model.MountIdentity, dentry model.DentryRecord, version, dentryVersion uint64) (model.PathIndexRecord, bool, error) {
	parentPath, ok, err := e.pathIndexPathForInode(ctx, mount, dentry.Parent, version)
	if err != nil || !ok {
		return model.PathIndexRecord{}, ok, err
	}
	path := pathIndexJoin(parentPath, dentry.Name)
	return model.PathIndexRecord{
		RootInode:     model.RootInode,
		Path:          path,
		Parent:        dentry.Parent,
		Name:          dentry.Name,
		Inode:         dentry.Inode,
		Type:          dentry.Type,
		DentryVersion: dentryVersion,
	}, true, nil
}

func (e *Executor) pathIndexPathForInode(ctx context.Context, mount model.MountIdentity, inode model.InodeID, version uint64) (string, bool, error) {
	if inode == model.RootInode {
		return "", true, nil
	}
	prefix, err := layout.EncodePathIndexInodePrefix(mount, inode)
	if err != nil {
		return "", false, err
	}
	kvs, err := e.runner.Scan(ctx, prefix, pathIndexScanLimit, version)
	if err != nil {
		return "", false, err
	}
	for _, kv := range kvs {
		if !bytes.HasPrefix(kv.Key, prefix) {
			break
		}
		record, err := layout.DecodePathIndexValue(kv.Value)
		if err != nil {
			return "", false, err
		}
		if record.Inode == inode && record.Type == model.InodeTypeDirectory {
			return record.Path, true, nil
		}
	}
	return "", false, nil
}

func pathIndexPutMutationsForRecord(mount model.MountIdentity, record model.PathIndexRecord) ([]*backend.Mutation, error) {
	pathKey, inodeKey, err := pathIndexKeysForRecord(mount, record)
	if err != nil {
		return nil, err
	}
	value, err := layout.EncodePathIndexValue(record)
	if err != nil {
		return nil, err
	}
	return []*backend.Mutation{
		{Op: backend.MutationPut, Key: pathKey, Value: value, Family: backend.MetadataFamilyPathIndex},
		{Op: backend.MutationPut, Key: inodeKey, Value: value, Family: backend.MetadataFamilyPathIndex},
	}, nil
}

func pathIndexKeysForRecord(mount model.MountIdentity, record model.PathIndexRecord) ([]byte, []byte, error) {
	pathKey, err := layout.EncodePathIndexKey(mount, record.RootInode, record.Path)
	if err != nil {
		return nil, nil, err
	}
	inodeKey, err := layout.EncodePathIndexInodeKey(mount, record.Inode, record.RootInode, record.Path)
	if err != nil {
		return nil, nil, err
	}
	return pathKey, inodeKey, nil
}

func pathIndexJoin(parentPath, name string) string {
	if parentPath == "" {
		return name
	}
	return parentPath + "/" + name
}

// LookupPath resolves a slash-delimited path relative to RootInode. It first
// tries the derived path index and validates the result against canonical
// dentry/inode state; stale or missing index entries fall back to ordinary
// component-by-component lookup.
func (e *Executor) LookupPath(ctx context.Context, req model.LookupPathRequest) (model.DentryAttrPair, error) {
	if err := model.ValidateLookupPathRequest(req); err != nil {
		return model.DentryAttrPair{}, err
	}
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	mount := mountRecord.Identity()
	var pair model.DentryAttrPair
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		if req.RootInode == model.RootInode {
			indexed, ok, err := e.lookupPathIndex(ctx, mount, req.Path, version)
			if err != nil {
				return err
			}
			if ok {
				pair = indexed
				return nil
			}
		}
		fallback, err := e.lookupPathCanonical(ctx, mount, req.RootInode, req.Path, version)
		if err != nil {
			return err
		}
		pair = fallback
		return nil
	})
	return pair, err
}

func (e *Executor) lookupPathIndex(ctx context.Context, mount model.MountIdentity, path string, version uint64) (model.DentryAttrPair, bool, error) {
	key, err := layout.EncodePathIndexKey(mount, model.RootInode, path)
	if err != nil {
		return model.DentryAttrPair{}, false, err
	}
	value, ok, err := e.runner.Get(ctx, key, version)
	if err != nil || !ok {
		return model.DentryAttrPair{}, false, err
	}
	record, err := layout.DecodePathIndexValue(value)
	if err != nil {
		return model.DentryAttrPair{}, false, err
	}
	if record.RootInode != model.RootInode || record.Path != path {
		return model.DentryAttrPair{}, false, nil
	}
	pair, err := e.lookupPathIndexRecord(ctx, mount, record, version)
	if err != nil {
		return model.DentryAttrPair{}, false, err
	}
	if pair.Dentry.Parent != record.Parent ||
		pair.Dentry.Name != record.Name ||
		pair.Dentry.Inode != record.Inode ||
		pair.Dentry.Type != record.Type {
		return model.DentryAttrPair{}, false, nil
	}
	if strings.Contains(path, "/") {
		canonical, err := e.lookupPathCanonical(ctx, mount, model.RootInode, path, version)
		if err != nil {
			if errors.Is(err, model.ErrNotFound) {
				return model.DentryAttrPair{}, false, nil
			}
			return model.DentryAttrPair{}, false, err
		}
		if canonical.Dentry.Parent != record.Parent ||
			canonical.Dentry.Name != record.Name ||
			canonical.Dentry.Inode != record.Inode ||
			canonical.Dentry.Type != record.Type {
			return model.DentryAttrPair{}, false, nil
		}
		return canonical, true, nil
	}
	return pair, true, nil
}

func (e *Executor) lookupPathIndexRecord(ctx context.Context, mount model.MountIdentity, record model.PathIndexRecord, version uint64) (model.DentryAttrPair, error) {
	key, err := layout.EncodeDentryKey(mount, record.Parent, record.Name)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	snapshot, err := e.readDentrySnapshot(ctx, key, version)
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			return model.DentryAttrPair{}, nil
		}
		return model.DentryAttrPair{}, err
	}
	return e.readLookupPlusInode(ctx, mount, snapshot.record, version)
}

func (e *Executor) lookupPathCanonical(ctx context.Context, mount model.MountIdentity, root model.InodeID, path string, version uint64) (model.DentryAttrPair, error) {
	normalized, err := model.NormalizeViewPath(path)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if normalized == "" {
		return model.DentryAttrPair{}, model.ErrInvalidRequest
	}
	parent := root
	var current model.DentryAttrPair
	parts := strings.Split(normalized, "/")
	for idx, part := range parts {
		key, err := layout.EncodeDentryKey(mount, parent, part)
		if err != nil {
			return model.DentryAttrPair{}, err
		}
		snapshot, err := e.readDentrySnapshot(ctx, key, version)
		if err != nil {
			return model.DentryAttrPair{}, err
		}
		current, err = e.readLookupPlusInode(ctx, mount, snapshot.record, version)
		if err != nil {
			return model.DentryAttrPair{}, err
		}
		if idx != len(parts)-1 {
			if current.Inode.Type != model.InodeTypeDirectory {
				return model.DentryAttrPair{}, model.ErrInvalidRequest
			}
			parent = current.Inode.Inode
		}
	}
	return current, nil
}
