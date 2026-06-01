// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

func dentryWatchEvent(mount model.MountIdentity, op backend.WatchOperation, dentry model.DentryRecord) (backend.WatchEvent, error) {
	key, err := layout.EncodeDentryKey(mount, dentry.Parent, dentry.Name)
	if err != nil {
		return backend.WatchEvent{}, err
	}
	return backend.WatchEvent{
		Family:    backend.MetadataFamilyDentry,
		Key:       key,
		Operation: op,
		Parent:    uint64(dentry.Parent),
		Name:      dentry.Name,
		Inode:     uint64(dentry.Inode),
	}, nil
}

func dentryUpdateWatchEvent(mount model.MountIdentity, parent model.InodeID, name string, inode model.InodeID) (backend.WatchEvent, error) {
	key, err := layout.EncodeDentryKey(mount, parent, name)
	if err != nil {
		return backend.WatchEvent{}, err
	}
	return backend.WatchEvent{
		Family:    backend.MetadataFamilyDentry,
		Key:       key,
		Operation: backend.WatchOperationUpdate,
		Parent:    uint64(parent),
		Name:      name,
		Inode:     uint64(inode),
	}, nil
}

func dentryRenameWatchEvents(mount model.MountIdentity, op backend.WatchOperation, oldDentry, newDentry model.DentryRecord) ([]backend.WatchEvent, error) {
	oldEvent, err := dentryWatchEvent(mount, op, oldDentry)
	if err != nil {
		return nil, err
	}
	newEvent, err := dentryWatchEvent(mount, op, newDentry)
	if err != nil {
		return nil, err
	}
	for _, event := range []*backend.WatchEvent{&oldEvent, &newEvent} {
		event.OldParent = uint64(oldDentry.Parent)
		event.OldName = oldDentry.Name
		event.NewParent = uint64(newDentry.Parent)
		event.NewName = newDentry.Name
		event.Inode = uint64(newDentry.Inode)
	}
	if oldDentry.Parent == newDentry.Parent && oldDentry.Name == newDentry.Name {
		return []backend.WatchEvent{newEvent}, nil
	}
	return []backend.WatchEvent{oldEvent, newEvent}, nil
}
