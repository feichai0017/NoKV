// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/model"
)

// ArtifactClient is the namespace surface needed to publish an already-written
// body descriptor into fsmeta. GRPCClient satisfies it.
type ArtifactClient interface {
	Create(context.Context, model.CreateRequest) (model.CreateResult, error)
	Rename(context.Context, model.RenameRequest) error
	RenameReplace(context.Context, model.RenameReplaceRequest) (model.RenameReplaceResult, error)
	Remove(context.Context, model.RemoveRequest) (model.RemoveResult, error)
}

// PublishArtifactRequest describes a staged artifact metadata publish. The
// external body must already be durable enough for the caller before this
// helper writes fsmeta metadata.
type PublishArtifactRequest struct {
	Mount       model.MountID
	StageParent model.InodeID
	StageName   string
	FinalParent model.InodeID
	FinalName   string
	Attrs       model.CreateAttrs
	Body        model.BodyDescriptor
	Replace     bool
}

// PublishArtifactResult reports the final publish and any replaced body
// descriptor that should be scheduled for external GC.
type PublishArtifactResult struct {
	Staged           model.CreateResult
	Replaced         bool
	OldDentry        model.DentryRecord
	OldInode         model.InodeRecord
	OldBody          model.BodyDescriptor
	OldBodyPresent   bool
	OldBodyMalformed bool
	OldInodeDeleted  bool
}

// RemoveArtifactResult reports a removed artifact namespace entry and any body
// descriptor that should be scheduled for external GC.
type RemoveArtifactResult struct {
	model.RemoveResult
	OldBody          model.BodyDescriptor
	OldBodyPresent   bool
	OldBodyMalformed bool
}

// PublishArtifact creates a staged metadata entry with a canonical body
// descriptor, then atomically renames it to the final location. When Replace is
// true, the final rename uses fsmeta RenameReplace and returns the old body
// descriptor for caller-managed GC.
func PublishArtifact(ctx context.Context, cli ArtifactClient, req PublishArtifactRequest) (PublishArtifactResult, error) {
	if cli == nil {
		return PublishArtifactResult{}, errArtifactClientRequired
	}
	create, err := req.createRequest()
	if err != nil {
		return PublishArtifactResult{}, err
	}
	if err := model.ValidateCreateRequest(create); err != nil {
		return PublishArtifactResult{}, err
	}
	if req.Replace {
		if err := model.ValidateRenameReplaceRequest(req.renameReplaceRequest()); err != nil {
			return PublishArtifactResult{}, err
		}
	} else if err := model.ValidateRenameRequest(req.renameRequest()); err != nil {
		return PublishArtifactResult{}, err
	}

	staged, err := cli.Create(ctx, create)
	if err != nil {
		return PublishArtifactResult{}, err
	}
	result := PublishArtifactResult{Staged: staged}
	if !req.Replace {
		if err := cli.Rename(ctx, req.renameRequest()); err != nil {
			return PublishArtifactResult{}, err
		}
		return result, nil
	}
	replaced, err := cli.RenameReplace(ctx, req.renameReplaceRequest())
	if err != nil {
		return PublishArtifactResult{}, err
	}
	result.Replaced = replaced.Replaced
	result.OldDentry = replaced.OldDentry
	result.OldInode = replaced.OldInode
	result.OldInodeDeleted = replaced.OldInodeDeleted
	if replaced.Replaced {
		body, ok, malformed := optionalBodyDescriptor(replaced.OldInode)
		result.OldBody = body
		result.OldBodyPresent = ok
		result.OldBodyMalformed = malformed
	}
	return result, nil
}

// RemoveArtifact removes a file artifact and decodes the old body descriptor
// from the removed inode for caller-managed GC retry.
func RemoveArtifact(ctx context.Context, cli ArtifactClient, req model.RemoveRequest) (RemoveArtifactResult, error) {
	if cli == nil {
		return RemoveArtifactResult{}, errArtifactClientRequired
	}
	removed, err := cli.Remove(ctx, req)
	if err != nil {
		return RemoveArtifactResult{}, err
	}
	result := RemoveArtifactResult{RemoveResult: removed}
	body, ok, malformed := optionalBodyDescriptor(removed.OldInode)
	result.OldBody = body
	result.OldBodyPresent = ok
	result.OldBodyMalformed = malformed
	return result, nil
}

func (r PublishArtifactRequest) createRequest() (model.CreateRequest, error) {
	attrs := r.Attrs
	if attrs.Type == "" {
		attrs.Type = model.InodeTypeFile
	}
	encoded, err := model.EncodeBodyDescriptor(r.Body)
	if err != nil {
		return model.CreateRequest{}, err
	}
	attrs.OpaqueAttrs = encoded
	return model.CreateRequest{
		Mount:  r.Mount,
		Parent: r.StageParent,
		Name:   r.StageName,
		Attrs:  attrs,
	}, nil
}

func (r PublishArtifactRequest) renameRequest() model.RenameRequest {
	return model.RenameRequest{
		Mount:      r.Mount,
		FromParent: r.StageParent,
		FromName:   r.StageName,
		ToParent:   r.FinalParent,
		ToName:     r.FinalName,
	}
}

func (r PublishArtifactRequest) renameReplaceRequest() model.RenameReplaceRequest {
	return model.RenameReplaceRequest(r.renameRequest())
}

func optionalBodyDescriptor(inode model.InodeRecord) (model.BodyDescriptor, bool, bool) {
	desc, ok, err := model.InodeBodyDescriptor(inode)
	if err != nil {
		return model.BodyDescriptor{}, false, true
	}
	return desc, ok, false
}
