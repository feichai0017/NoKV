// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

type dentrySnapshot struct {
	record       model.DentryRecord
	value        []byte
	projection   model.InodeRecord
	projectionOK bool
}

// Lookup returns the dentry record for parent/name.
func (e *Executor) Lookup(ctx context.Context, req model.LookupRequest) (model.DentryRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.DentryRecord{}, err
	}
	program, err := compile.CompileLookupReadProgram(req, mountRecord.Identity())
	if err != nil {
		return model.DentryRecord{}, err
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return model.DentryRecord{}, err
	}
	value, ok, err := e.runner.Get(ctx, program.Plan.PrimaryKey, version)
	if err != nil {
		return model.DentryRecord{}, err
	}
	if !ok {
		return model.DentryRecord{}, model.ErrNotFound
	}
	return layout.DecodeDentryValue(value)
}

// LookupPlus returns one dentry and its inode attributes at the same read
// version.
func (e *Executor) LookupPlus(ctx context.Context, req model.LookupRequest) (model.DentryAttrPair, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileLookupReadProgram(req, mount)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	value, ok, err := e.runner.Get(ctx, program.Plan.PrimaryKey, version)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if !ok {
		return model.DentryAttrPair{}, model.ErrNotFound
	}
	dentry, projection, _, projectionOK, err := layout.DecodeDentryValueWithProjection(value)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if projectionOK && dentryProjectionUsable(dentry, projection) {
		return model.DentryAttrPair{Dentry: dentry, Inode: projection}, nil
	}
	return e.readLookupPlusInode(ctx, mount, dentry, version)
}

func (e *Executor) readLookupPlusInode(ctx context.Context, mount model.MountIdentity, dentry model.DentryRecord, version uint64) (model.DentryAttrPair, error) {
	program, err := compile.CompileGetAttrReadProgram(mount, dentry.Inode)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	value, ok, err := e.runner.Get(ctx, program.Key, version)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if !ok {
		return model.DentryAttrPair{}, fmt.Errorf("%w: inode %d", model.ErrNotFound, dentry.Inode)
	}
	inode, err := layout.DecodeInodeValue(value)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if inode.Inode != dentry.Inode {
		return model.DentryAttrPair{}, fmt.Errorf("%w: dentry inode=%d value inode=%d", model.ErrInvalidValue, dentry.Inode, inode.Inode)
	}
	return model.DentryAttrPair{Dentry: dentry, Inode: inode}, nil
}

// ReadDir returns one directory page from a dentry prefix scan.
func (e *Executor) ReadDir(ctx context.Context, req model.ReadDirRequest) ([]model.DentryRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return nil, err
	}
	plan, err := compile.CompileDirectoryReadPlan(req, mountRecord.Identity(), false, false)
	if err != nil {
		return nil, err
	}
	var out []model.DentryRecord
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		var err error
		out, err = e.scanDentries(ctx, plan, version)
		return err
	})
	return out, err
}

// ReadDirPlus returns one directory page fused with inode attributes at the
// same snapshot version.
func (e *Executor) ReadDirPlus(ctx context.Context, req model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return nil, err
	}
	mount := mountRecord.Identity()
	plan, err := compile.CompileDirectoryReadPlan(req, mount, false, false)
	if err != nil {
		return nil, err
	}
	var out []model.DentryAttrPair
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		dentries, err := e.scanDentrySnapshots(ctx, plan, version)
		if err != nil {
			return err
		}
		if len(dentries) == 0 {
			out = []model.DentryAttrPair{}
			return nil
		}
		e.readDirPlusDentryCount.Add(uint64(len(dentries)))
		pairs := make([]model.DentryAttrPair, len(dentries))
		fallbackDentries := make([]model.DentryRecord, 0)
		fallbackIndexes := make([]int, 0)
		for i, dentry := range dentries {
			if dentry.projectionOK && dentryProjectionUsable(dentry.record, dentry.projection) {
				pairs[i] = model.DentryAttrPair{Dentry: dentry.record, Inode: dentry.projection}
				e.readDirPlusProjectionHitTotal.Add(1)
				continue
			}
			fallbackDentries = append(fallbackDentries, dentry.record)
			fallbackIndexes = append(fallbackIndexes, i)
		}
		if len(fallbackDentries) != 0 {
			e.readDirPlusInodeBatchCount.Add(uint64(len(fallbackDentries)))
			inodeKeys, err := compile.CompileReadDirPlusInodeKeys(mount, fallbackDentries)
			if err != nil {
				return err
			}
			values, err := e.runner.BatchGet(ctx, inodeKeys, version)
			if err != nil {
				return err
			}
			for i, dentry := range fallbackDentries {
				key, err := layout.EncodeInodeKey(mount, dentry.Inode)
				if err != nil {
					return err
				}
				value, ok := values[string(key)]
				if !ok {
					return fmt.Errorf("%w: inode %d", model.ErrNotFound, dentry.Inode)
				}
				inode, err := layout.DecodeInodeValue(value)
				if err != nil {
					return err
				}
				if inode.Inode != dentry.Inode {
					return fmt.Errorf("%w: dentry inode=%d value inode=%d", model.ErrInvalidValue, dentry.Inode, inode.Inode)
				}
				pairs[fallbackIndexes[i]] = model.DentryAttrPair{
					Dentry: dentry,
					Inode:  inode,
				}
			}
		}
		out = pairs
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (e *Executor) scanDentries(ctx context.Context, plan compile.DirectoryReadPlan, version uint64) ([]model.DentryRecord, error) {
	snapshots, err := e.scanDentrySnapshots(ctx, plan, version)
	if err != nil {
		return nil, err
	}
	out := make([]model.DentryRecord, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, snapshot.record)
	}
	return out, nil
}

func (e *Executor) scanDentrySnapshots(ctx context.Context, plan compile.DirectoryReadPlan, version uint64) ([]dentrySnapshot, error) {
	kvs, err := e.runner.Scan(ctx, plan.StartKey, plan.Limit, version)
	if err != nil {
		return nil, err
	}
	out := make([]dentrySnapshot, 0, len(kvs))
	for _, kv := range kvs {
		if !bytes.HasPrefix(kv.Key, plan.Prefix) {
			break
		}
		record, projection, _, projectionOK, err := layout.DecodeDentryValueWithProjection(kv.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, dentrySnapshot{
			record:       record,
			value:        cloneBytes(kv.Value),
			projection:   projection,
			projectionOK: projectionOK,
		})
	}
	return out, nil
}

func (e *Executor) readDentry(ctx context.Context, key []byte, version uint64) (model.DentryRecord, error) {
	snapshot, err := e.readDentrySnapshot(ctx, key, version)
	if err != nil {
		return model.DentryRecord{}, err
	}
	return snapshot.record, nil
}

func (e *Executor) readDentrySnapshot(ctx context.Context, key []byte, version uint64) (dentrySnapshot, error) {
	value, ok, err := e.runner.Get(ctx, key, version)
	if err != nil {
		return dentrySnapshot{}, err
	}
	if !ok {
		return dentrySnapshot{}, model.ErrNotFound
	}
	record, projection, _, projectionOK, err := layout.DecodeDentryValueWithProjection(value)
	if err != nil {
		return dentrySnapshot{}, err
	}
	return dentrySnapshot{
		record:       record,
		value:        cloneBytes(value),
		projection:   projection,
		projectionOK: projectionOK,
	}, nil
}

func (e *Executor) readInode(ctx context.Context, mount model.MountIdentity, inodeID model.InodeID, version uint64) (model.InodeRecord, bool, error) {
	key, err := layout.EncodeInodeKey(mount, inodeID)
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	value, ok, err := e.runner.Get(ctx, key, version)
	if err != nil || !ok {
		return model.InodeRecord{}, ok, err
	}
	inode, err := layout.DecodeInodeValue(value)
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	return inode, true, nil
}

func (e *Executor) readSessionByKey(ctx context.Context, mount model.MountIdentity, key []byte, version uint64) (model.SessionRecord, bool, error) {
	parts, ok := layout.InspectKey(key)
	if !ok || parts.Kind != layout.KeyKindSession {
		return model.SessionRecord{}, false, layout.ErrInvalidKey
	}
	if parts.MountKeyID != mount.MountKeyID {
		return model.SessionRecord{}, false, model.ErrInvalidRequest
	}
	value, ok, err := e.runner.Get(ctx, key, version)
	if err != nil || !ok {
		return model.SessionRecord{}, ok, err
	}
	record, err := layout.DecodeSessionValue(value)
	if err != nil {
		return model.SessionRecord{}, false, err
	}
	return record, true, nil
}

func dentryProjectionUsable(dentry model.DentryRecord, projection model.InodeRecord) bool {
	return dentry.Inode == projection.Inode &&
		dentry.Type == projection.Type &&
		projection.Type != model.InodeTypeDirectory &&
		projection.LinkCount <= 1
}
