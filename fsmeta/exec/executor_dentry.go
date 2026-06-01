// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

func encodeDentryValueForCommit(dentry model.DentryRecord, inode model.InodeRecord, haveInode bool, commitVersion uint64) ([]byte, error) {
	if haveInode && dentryProjectionWritable(dentry, inode) {
		return layout.EncodeDentryValueWithProjection(dentry, inode, commitVersion)
	}
	return layout.EncodeDentryValue(dentry)
}

func dentryProjectionWritable(dentry model.DentryRecord, inode model.InodeRecord) bool {
	return dentry.Inode == inode.Inode &&
		dentry.Type == inode.Type &&
		inode.Type != model.InodeTypeDirectory &&
		inode.LinkCount <= 1
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
	return &backend.Mutation{Op: backend.MutationDelete, Key: key}, nil
}
