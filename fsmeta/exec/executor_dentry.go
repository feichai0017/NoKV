// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

const directoryParentIndexScanLimit uint32 = 2

func encodeDentryValueForCommit(dentry model.DentryRecord, inode model.InodeRecord, haveInode bool, commitVersion uint64) ([]byte, error) {
	if haveInode && dentryProjectionWritable(dentry, inode) {
		return layout.EncodeDentryValueWithProjection(dentry, inode, commitVersion)
	}
	return layout.EncodeDentryValue(dentry)
}

func dentryProjectionWritable(dentry model.DentryRecord, inode model.InodeRecord) bool {
	return dentry.Inode == inode.Inode &&
		dentry.Type == inode.Type &&
		inode.LinkCount <= 1
}

func (e *Executor) directoryDentryProjectionMutation(ctx context.Context, mount model.MountIdentity, inode model.InodeRecord, version, commitVersion uint64) (*backend.Mutation, *backend.Predicate, error) {
	if inode.Inode == model.RootInode {
		return nil, nil, nil
	}
	if inode.Type != model.InodeTypeDirectory {
		return nil, nil, model.ErrInvalidRequest
	}
	prefix, err := layout.EncodeParentIndexPrefix(mount, inode.Inode)
	if err != nil {
		return nil, nil, err
	}
	kvs, err := e.runner.Scan(ctx, prefix, prefix, directoryParentIndexScanLimit, version)
	if err != nil {
		return nil, nil, err
	}
	var link model.ParentLinkRecord
	found := false
	for _, kv := range kvs {
		if !bytes.HasPrefix(kv.Key, prefix) {
			break
		}
		record, err := layout.DecodeParentLinkValue(kv.Value)
		if err != nil {
			return nil, nil, err
		}
		if record.Child != inode.Inode || record.Type != model.InodeTypeDirectory {
			continue
		}
		if found {
			return nil, nil, model.ErrInvalidValue
		}
		link = record
		found = true
	}
	if !found {
		return nil, nil, nil
	}
	dentryKey, err := layout.EncodeDentryKey(mount, link.Parent, link.Name)
	if err != nil {
		return nil, nil, err
	}
	dentry, err := e.readDentrySnapshot(ctx, dentryKey, version)
	if err != nil {
		return nil, nil, err
	}
	if dentry.record.Inode != inode.Inode || dentry.record.Type != model.InodeTypeDirectory {
		return nil, nil, model.ErrInvalidValue
	}
	value, err := encodeDentryValueForCommit(dentry.record, inode, true, commitVersion)
	if err != nil {
		return nil, nil, err
	}
	return &backend.Mutation{
		Family: backend.MetadataFamilyDentry,
		Op:     backend.MutationPut,
		Key:    dentryKey,
		Value:  value,
	}, metadataValueEqualsPredicate(dentryKey, dentry.value), nil
}

func parentIndexPutMutation(mount model.MountIdentity, dentry model.DentryRecord, assertionNotExist bool) (*backend.Mutation, error) {
	key, err := layout.EncodeParentIndexKey(mount, dentry.Inode, dentry.Parent, dentry.Name)
	if err != nil {
		return nil, err
	}
	value, err := layout.EncodeParentLinkValue(model.ParentLinkRecord{
		Child:  dentry.Inode,
		Parent: dentry.Parent,
		Name:   dentry.Name,
		Type:   dentry.Type,
	})
	if err != nil {
		return nil, err
	}
	return &backend.Mutation{
		Family:            backend.MetadataFamilyParent,
		Op:                backend.MutationPut,
		Key:               key,
		Value:             value,
		AssertionNotExist: assertionNotExist,
	}, nil
}

func parentIndexDeleteMutation(mount model.MountIdentity, dentry model.DentryRecord) (*backend.Mutation, error) {
	key, err := layout.EncodeParentIndexKey(mount, dentry.Inode, dentry.Parent, dentry.Name)
	if err != nil {
		return nil, err
	}
	return &backend.Mutation{Family: backend.MetadataFamilyParent, Op: backend.MutationDelete, Key: key}, nil
}
