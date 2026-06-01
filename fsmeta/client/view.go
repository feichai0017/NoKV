// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
)

// ViewBackend is the fsmeta surface required by a scoped view. GRPCClient
// satisfies it; tests can provide a narrower in-memory backend.
type ViewBackend interface {
	LookupPlus(context.Context, model.LookupRequest) (model.DentryAttrPair, error)
	GetAttr(context.Context, model.GetAttrRequest) (model.InodeRecord, error)
	BatchGetAttr(context.Context, model.BatchGetAttrRequest) ([]model.InodeRecord, error)
	ReadDirPlus(context.Context, model.ReadDirRequest) ([]model.DentryAttrPair, error)
	WatchSubtree(context.Context, observe.WatchRequest) (WatchSubscription, error)
	Create(context.Context, model.CreateRequest) (model.CreateResult, error)
	Rename(context.Context, model.RenameRequest) error
	RenameReplace(context.Context, model.RenameReplaceRequest) (model.RenameReplaceResult, error)
	Remove(context.Context, model.RemoveRequest) (model.RemoveResult, error)
	RemoveDirectory(context.Context, model.RemoveDirectoryRequest) error
}

// ViewReadDirRequest addresses one directory path relative to a view root.
type ViewReadDirRequest struct {
	Path       string
	StartAfter string
	Limit      uint32
}

// ScopedView is a client-side namespace capability for sub-agents. It enforces
// relative path containment and read-only snapshot rules before calling fsmeta.
type ScopedView struct {
	backend ViewBackend
	desc    model.ViewDescriptor

	mu          sync.RWMutex
	knownInodes map[model.InodeID]struct{}
}

// CreateView resolves a scoped view and its access rules. The returned view is
// the capability; fsmeta/root do not persist separate view truth.
func CreateView(ctx context.Context, backend ViewBackend, req model.CreateViewRequest) (*ScopedView, error) {
	if backend == nil {
		return nil, errRPCClientNotConfigured
	}
	if err := model.ValidateCreateViewRequest(req); err != nil {
		return nil, err
	}
	root, err := backend.GetAttr(ctx, model.GetAttrRequest{
		Mount:           req.Mount,
		Inode:           req.RootInode,
		SnapshotVersion: req.SnapshotVersion,
	})
	if err != nil {
		return nil, err
	}
	if root.Type != model.InodeTypeDirectory {
		return nil, model.ErrInvalidRequest
	}
	view := &ScopedView{
		backend:     backend,
		knownInodes: map[model.InodeID]struct{}{req.RootInode: {}},
	}
	view.desc = model.ViewDescriptor{
		Ref: model.NamespaceRef{
			Mount:     req.Mount,
			ViewToken: newViewToken(),
		},
		RootInode:       req.RootInode,
		SnapshotVersion: req.SnapshotVersion,
	}
	for _, rule := range req.AccessRules {
		resolved, err := view.resolveRule(ctx, rule)
		if err != nil {
			return nil, err
		}
		view.desc.AccessRules = append(view.desc.AccessRules, resolved)
	}
	return view, nil
}

// CreateReadOnlySnapshotView creates a read-only view over a published
// SnapshotSubtree token.
func CreateReadOnlySnapshotView(ctx context.Context, backend ViewBackend, token model.SnapshotSubtreeToken) (*ScopedView, error) {
	if err := model.ValidateMountID(token.Mount); err != nil {
		return nil, err
	}
	if err := model.ValidateInodeID(token.RootInode); err != nil {
		return nil, err
	}
	if token.ReadVersion == 0 {
		return nil, model.ErrInvalidValue
	}
	return CreateView(ctx, backend, model.CreateViewRequest{
		Mount:           token.Mount,
		RootInode:       token.RootInode,
		SnapshotVersion: token.ReadVersion,
		AccessRules: []model.ViewAccessRule{{
			Prefix: "",
			Mode:   model.ViewAccessReadOnly,
		}},
	})
}

// Descriptor returns a detached view descriptor.
func (v *ScopedView) Descriptor() model.ViewDescriptor {
	if v == nil {
		return model.ViewDescriptor{}
	}
	return v.desc.Clone()
}

// GetAttr reads a known inode inside the view. Inodes become known when they
// are resolved through the view root, returned by ReadDirPlus, or created by
// this view.
func (v *ScopedView) GetAttr(ctx context.Context, inode model.InodeID) (model.InodeRecord, error) {
	if err := v.requireKnownInode(inode); err != nil {
		return model.InodeRecord{}, err
	}
	return v.backend.GetAttr(ctx, model.GetAttrRequest{
		Mount:           v.desc.Ref.Mount,
		Inode:           inode,
		SnapshotVersion: v.desc.SnapshotVersion,
	})
}

// BatchGetAttr reads known inodes inside the view.
func (v *ScopedView) BatchGetAttr(ctx context.Context, inodes []model.InodeID) ([]model.InodeRecord, error) {
	for _, inode := range inodes {
		if err := v.requireKnownInode(inode); err != nil {
			return nil, err
		}
	}
	return v.backend.BatchGetAttr(ctx, model.BatchGetAttrRequest{
		Mount:           v.desc.Ref.Mount,
		Inodes:          append([]model.InodeID(nil), inodes...),
		SnapshotVersion: v.desc.SnapshotVersion,
	})
}

// LookupPlus resolves one path relative to the view root.
func (v *ScopedView) LookupPlus(ctx context.Context, relPath string) (model.DentryAttrPair, error) {
	normalized, err := model.NormalizeViewPath(relPath)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if normalized == "" {
		return model.DentryAttrPair{}, model.ErrInvalidRequest
	}
	if err := v.checkAccess(normalized, false); err != nil {
		return model.DentryAttrPair{}, err
	}
	return v.resolvePath(ctx, normalized)
}

// ReadDirPlus reads one directory page relative to the view root.
func (v *ScopedView) ReadDirPlus(ctx context.Context, req ViewReadDirRequest) ([]model.DentryAttrPair, error) {
	normalized, err := model.NormalizeViewPath(req.Path)
	if err != nil {
		return nil, err
	}
	if err := v.checkAccess(normalized, false); err != nil {
		return nil, err
	}
	dir, err := v.resolveDirectory(ctx, normalized)
	if err != nil {
		return nil, err
	}
	entries, err := v.backend.ReadDirPlus(ctx, model.ReadDirRequest{
		Mount:           v.desc.Ref.Mount,
		Parent:          dir,
		StartAfter:      req.StartAfter,
		Limit:           req.Limit,
		SnapshotVersion: v.desc.SnapshotVersion,
	})
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		v.rememberInode(entry.Inode.Inode)
	}
	return entries, nil
}

// WatchDirectory subscribes to non-recursive changes for one directory inside
// the view.
func (v *ScopedView) WatchDirectory(ctx context.Context, path string, resume observe.WatchCursor, window uint32) (WatchSubscription, error) {
	normalized, err := model.NormalizeViewPath(path)
	if err != nil {
		return nil, err
	}
	if v.desc.SnapshotVersion != 0 {
		return nil, model.ErrInvalidRequest
	}
	if err := v.checkAccess(normalized, false); err != nil {
		return nil, err
	}
	dir, err := v.resolveDirectory(ctx, normalized)
	if err != nil {
		return nil, err
	}
	return v.backend.WatchSubtree(ctx, observe.WatchRequest{
		Mount:              v.desc.Ref.Mount,
		RootInode:          dir,
		DescendRecursively: false,
		ResumeCursor:       resume,
		BackPressureWindow: window,
	})
}

// Create creates one entry relative to the view root.
func (v *ScopedView) Create(ctx context.Context, relPath string, attrs model.CreateAttrs) (model.CreateResult, error) {
	parentPath, name, err := splitViewParent(relPath)
	if err != nil {
		return model.CreateResult{}, err
	}
	if err := v.checkAccess(relPath, true); err != nil {
		return model.CreateResult{}, err
	}
	if err := v.checkAccess(parentPath, false); err != nil {
		return model.CreateResult{}, err
	}
	parent, err := v.resolveDirectory(ctx, parentPath)
	if err != nil {
		return model.CreateResult{}, err
	}
	result, err := v.backend.Create(ctx, model.CreateRequest{
		Mount:  v.desc.Ref.Mount,
		Parent: parent,
		Name:   name,
		Attrs:  attrs,
	})
	if err != nil {
		return model.CreateResult{}, err
	}
	v.rememberInode(result.Inode.Inode)
	return result, nil
}

// Remove removes one non-directory entry relative to the view root.
func (v *ScopedView) Remove(ctx context.Context, relPath string) (model.RemoveResult, error) {
	parentPath, name, err := splitViewParent(relPath)
	if err != nil {
		return model.RemoveResult{}, err
	}
	if err := v.checkAccess(relPath, true); err != nil {
		return model.RemoveResult{}, err
	}
	if err := v.checkAccess(parentPath, false); err != nil {
		return model.RemoveResult{}, err
	}
	parent, err := v.resolveDirectory(ctx, parentPath)
	if err != nil {
		return model.RemoveResult{}, err
	}
	return v.backend.Remove(ctx, model.RemoveRequest{Mount: v.desc.Ref.Mount, Parent: parent, Name: name})
}

// RemoveDirectory removes one empty directory relative to the view root.
func (v *ScopedView) RemoveDirectory(ctx context.Context, relPath string) error {
	parentPath, name, err := splitViewParent(relPath)
	if err != nil {
		return err
	}
	if err := v.checkAccess(relPath, true); err != nil {
		return err
	}
	if err := v.checkAccess(parentPath, false); err != nil {
		return err
	}
	parent, err := v.resolveDirectory(ctx, parentPath)
	if err != nil {
		return err
	}
	return v.backend.RemoveDirectory(ctx, model.RemoveDirectoryRequest{Mount: v.desc.Ref.Mount, Parent: parent, Name: name})
}

// Rename moves one entry inside the view.
func (v *ScopedView) Rename(ctx context.Context, fromPath, toPath string) error {
	fromParent, fromName, err := splitViewParent(fromPath)
	if err != nil {
		return err
	}
	toParent, toName, err := splitViewParent(toPath)
	if err != nil {
		return err
	}
	if err := v.checkAccess(fromPath, true); err != nil {
		return err
	}
	if err := v.checkAccess(toPath, true); err != nil {
		return err
	}
	fromDir, err := v.resolveDirectory(ctx, fromParent)
	if err != nil {
		return err
	}
	toDir, err := v.resolveDirectory(ctx, toParent)
	if err != nil {
		return err
	}
	return v.backend.Rename(ctx, model.RenameRequest{
		Mount:      v.desc.Ref.Mount,
		FromParent: fromDir,
		FromName:   fromName,
		ToParent:   toDir,
		ToName:     toName,
	})
}

// RenameReplace moves one entry inside the view and atomically replaces a file
// destination.
func (v *ScopedView) RenameReplace(ctx context.Context, fromPath, toPath string) (model.RenameReplaceResult, error) {
	fromParent, fromName, err := splitViewParent(fromPath)
	if err != nil {
		return model.RenameReplaceResult{}, err
	}
	toParent, toName, err := splitViewParent(toPath)
	if err != nil {
		return model.RenameReplaceResult{}, err
	}
	if err := v.checkAccess(fromPath, true); err != nil {
		return model.RenameReplaceResult{}, err
	}
	if err := v.checkAccess(toPath, true); err != nil {
		return model.RenameReplaceResult{}, err
	}
	fromDir, err := v.resolveDirectory(ctx, fromParent)
	if err != nil {
		return model.RenameReplaceResult{}, err
	}
	toDir, err := v.resolveDirectory(ctx, toParent)
	if err != nil {
		return model.RenameReplaceResult{}, err
	}
	return v.backend.RenameReplace(ctx, model.RenameReplaceRequest{
		Mount:      v.desc.Ref.Mount,
		FromParent: fromDir,
		FromName:   fromName,
		ToParent:   toDir,
		ToName:     toName,
	})
}

func (v *ScopedView) resolveRule(ctx context.Context, rule model.ViewAccessRule) (model.ResolvedViewAccessRule, error) {
	prefix, err := model.NormalizeViewPath(rule.Prefix)
	if err != nil {
		return model.ResolvedViewAccessRule{}, err
	}
	root := v.desc.RootInode
	if prefix != "" {
		root, err = v.resolveDirectory(ctx, prefix)
		if err != nil {
			return model.ResolvedViewAccessRule{}, err
		}
	}
	return model.ResolvedViewAccessRule{Prefix: prefix, RootInode: root, Mode: rule.Mode}, nil
}

func (v *ScopedView) resolveDirectory(ctx context.Context, relPath string) (model.InodeID, error) {
	normalized, err := model.NormalizeViewPath(relPath)
	if err != nil {
		return 0, err
	}
	if normalized == "" {
		return v.desc.RootInode, nil
	}
	pair, err := v.resolvePath(ctx, normalized)
	if err != nil {
		return 0, err
	}
	if pair.Inode.Type != model.InodeTypeDirectory {
		return 0, model.ErrInvalidRequest
	}
	return pair.Inode.Inode, nil
}

func (v *ScopedView) resolvePath(ctx context.Context, relPath string) (model.DentryAttrPair, error) {
	normalized, err := model.NormalizeViewPath(relPath)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	if normalized == "" {
		return model.DentryAttrPair{}, model.ErrInvalidRequest
	}
	parent := v.desc.RootInode
	var current model.DentryAttrPair
	parts := strings.Split(normalized, "/")
	for idx, part := range parts {
		current, err = v.backend.LookupPlus(ctx, model.LookupRequest{
			Mount:           v.desc.Ref.Mount,
			Parent:          parent,
			Name:            part,
			SnapshotVersion: v.desc.SnapshotVersion,
		})
		if err != nil {
			return model.DentryAttrPair{}, err
		}
		v.rememberInode(current.Inode.Inode)
		if idx != len(parts)-1 {
			if current.Inode.Type != model.InodeTypeDirectory {
				return model.DentryAttrPair{}, model.ErrInvalidRequest
			}
			parent = current.Inode.Inode
		}
	}
	return current, nil
}

func (v *ScopedView) checkAccess(relPath string, write bool) error {
	if v == nil {
		return errRPCClientNotConfigured
	}
	normalized, err := model.NormalizeViewPath(relPath)
	if err != nil {
		return err
	}
	if write && v.desc.SnapshotVersion != 0 {
		return model.ErrViewAccessDenied
	}
	var matched model.ViewAccessMode
	best := -1
	for _, rule := range v.desc.AccessRules {
		if !viewPathMatchesRule(normalized, rule.Prefix) {
			continue
		}
		if len(rule.Prefix) > best {
			best = len(rule.Prefix)
			matched = rule.Mode
		}
	}
	if best < 0 {
		return model.ErrViewAccessDenied
	}
	if write && matched != model.ViewAccessReadWrite {
		return model.ErrViewAccessDenied
	}
	return nil
}

func viewPathMatchesRule(path, prefix string) bool {
	if prefix == "" {
		return true
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func (v *ScopedView) rememberInode(inode model.InodeID) {
	if inode == 0 {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.knownInodes[inode] = struct{}{}
}

func (v *ScopedView) requireKnownInode(inode model.InodeID) error {
	if err := model.ValidateInodeID(inode); err != nil {
		return err
	}
	if v == nil {
		return errRPCClientNotConfigured
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if _, ok := v.knownInodes[inode]; ok {
		return nil
	}
	return model.ErrViewAccessDenied
}

func splitViewParent(relPath string) (string, string, error) {
	normalized, err := model.NormalizeViewPath(relPath)
	if err != nil {
		return "", "", err
	}
	if normalized == "" {
		return "", "", model.ErrInvalidRequest
	}
	idx := strings.LastIndexByte(normalized, '/')
	if idx < 0 {
		return "", normalized, nil
	}
	return normalized[:idx], normalized[idx+1:], nil
}

func newViewToken() model.ViewToken {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return model.ViewToken("view")
	}
	return model.ViewToken(hex.EncodeToString(raw[:]))
}
