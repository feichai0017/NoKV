// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package model

import "strings"

// ViewToken identifies one scoped namespace capability issued by the fsmeta
// client layer. It is not a rooted truth object.
type ViewToken string

// NamespaceRef addresses either a full mount or a client-scoped view over that
// mount. Existing mount-only APIs continue to use MountID directly; view APIs
// use this object so agent code does not confuse a mount with a capability.
type NamespaceRef struct {
	Mount     MountID
	ViewToken ViewToken
}

// ViewAccessMode is the access granted by one relative path rule.
type ViewAccessMode string

const (
	ViewAccessReadOnly  ViewAccessMode = "read_only"
	ViewAccessReadWrite ViewAccessMode = "read_write"
)

// ViewAccessRule is a caller-supplied relative path prefix rule. Empty prefix
// means the view root. Longest matching prefix wins; no match means deny.
type ViewAccessRule struct {
	Prefix string
	Mode   ViewAccessMode
}

// ResolvedViewAccessRule is a ViewAccessRule after the client has resolved the
// prefix to an inode. It is diagnostic and helps FUSE-style callers reason
// about the mounted capability without exposing full mount access.
type ResolvedViewAccessRule struct {
	Prefix    string
	RootInode InodeID
	Mode      ViewAccessMode
}

// CreateViewRequest describes a scoped view rooted at RootInode. When
// SnapshotVersion is non-zero, the view is read-only by construction.
type CreateViewRequest struct {
	Mount           MountID
	RootInode       InodeID
	SnapshotVersion uint64
	AccessRules     []ViewAccessRule
}

// ViewDescriptor is the runtime-neutral description returned after view
// creation. The descriptor is not persisted by fsmeta/root; callers must keep
// using the returned view object as their capability.
type ViewDescriptor struct {
	Ref             NamespaceRef
	RootInode       InodeID
	SnapshotVersion uint64
	AccessRules     []ResolvedViewAccessRule
}

// ValidateCreateViewRequest validates a scoped view request.
func ValidateCreateViewRequest(req CreateViewRequest) error {
	if err := ValidateMountID(req.Mount); err != nil {
		return err
	}
	if err := ValidateInodeID(req.RootInode); err != nil {
		return err
	}
	for _, rule := range req.AccessRules {
		if err := ValidateViewAccessRule(rule); err != nil {
			return err
		}
		if req.SnapshotVersion != 0 && rule.Mode != ViewAccessReadOnly {
			return ErrInvalidRequest
		}
	}
	return nil
}

// ValidateViewAccessRule validates one relative prefix access rule.
func ValidateViewAccessRule(rule ViewAccessRule) error {
	if _, err := NormalizeViewPath(rule.Prefix); err != nil {
		return err
	}
	switch rule.Mode {
	case ViewAccessReadOnly, ViewAccessReadWrite:
		return nil
	default:
		return ErrInvalidValue
	}
}

// NormalizeViewPath normalizes a slash-delimited path relative to one view
// root. It rejects absolute paths, empty interior segments, and parent escapes.
func NormalizeViewPath(path string) (string, error) {
	if path == "" || path == "." {
		return "", nil
	}
	if strings.HasPrefix(path, "/") {
		return "", ErrInvalidName
	}
	parts := strings.Split(path, "/")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", ErrInvalidName
		}
		if err := ValidateName(part); err != nil {
			return "", err
		}
		out = append(out, part)
	}
	return strings.Join(out, "/"), nil
}

// Clone returns a detached descriptor.
func (d ViewDescriptor) Clone() ViewDescriptor {
	if len(d.AccessRules) != 0 {
		d.AccessRules = append([]ResolvedViewAccessRule(nil), d.AccessRules...)
	}
	return d
}
