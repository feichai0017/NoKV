// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

type artifactFake struct {
	createErr        error
	renameErr        error
	renameReplaceErr error
	removeErr        error
	renameReplaceRes model.RenameReplaceResult
	removeRes        model.RemoveResult

	calls          []string
	created        model.CreateRequest
	renamed        model.RenameRequest
	renameReplaced model.RenameReplaceRequest
	removed        model.RemoveRequest
}

func (f *artifactFake) Create(_ context.Context, req model.CreateRequest) (model.CreateResult, error) {
	f.calls = append(f.calls, "create")
	f.created = req
	if f.createErr != nil {
		return model.CreateResult{}, f.createErr
	}
	inode := req.Attrs.InodeRecord(101)
	return model.CreateResult{
		Dentry: model.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: inode.Inode, Type: inode.Type},
		Inode:  inode,
	}, nil
}

func (f *artifactFake) Rename(_ context.Context, req model.RenameRequest) error {
	f.calls = append(f.calls, "rename")
	f.renamed = req
	return f.renameErr
}

func (f *artifactFake) RenameReplace(_ context.Context, req model.RenameReplaceRequest) (model.RenameReplaceResult, error) {
	f.calls = append(f.calls, "rename_replace")
	f.renameReplaced = req
	return f.renameReplaceRes, f.renameReplaceErr
}

func (f *artifactFake) Remove(_ context.Context, req model.RemoveRequest) (model.RemoveResult, error) {
	f.calls = append(f.calls, "remove")
	f.removed = req
	return f.removeRes, f.removeErr
}

func TestPublishArtifactWritesCanonicalBodyDescriptorAndRenames(t *testing.T) {
	cli := &artifactFake{}
	req := publishArtifactRequest()

	result, err := PublishArtifact(context.Background(), cli, req)
	require.NoError(t, err)
	require.Equal(t, []string{"create", "rename"}, cli.calls)
	require.Equal(t, model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   ".stage-artifact",
		Attrs: model.CreateAttrs{
			Type:        model.InodeTypeFile,
			Mode:        0o644,
			OpaqueAttrs: mustBodyAttrs(t, req.Body),
		},
	}, cli.created)
	require.Equal(t, model.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   ".stage-artifact",
		ToParent:   8,
		ToName:     "artifact.json",
	}, cli.renamed)
	require.Equal(t, model.InodeID(101), result.Staged.Inode.Inode)
	require.False(t, result.Replaced)
}

func TestPublishArtifactReplaceReturnsOldBodyDescriptor(t *testing.T) {
	oldBody := model.BodyDescriptor{BodyRef: "cas://old", DigestURI: "sha256:old", Size: 12, Generation: 3}
	cli := &artifactFake{
		renameReplaceRes: model.RenameReplaceResult{
			Replaced:        true,
			OldDentry:       model.DentryRecord{Parent: 8, Name: "artifact.json", Inode: 9, Type: model.InodeTypeFile},
			OldInode:        model.InodeRecord{Inode: 9, Type: model.InodeTypeFile, OpaqueAttrs: mustBodyAttrs(t, oldBody)},
			OldInodeDeleted: true,
		},
	}
	req := publishArtifactRequest()
	req.Replace = true

	result, err := PublishArtifact(context.Background(), cli, req)
	require.NoError(t, err)
	require.Equal(t, []string{"create", "rename_replace"}, cli.calls)
	require.Equal(t, model.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   ".stage-artifact",
		ToParent:   8,
		ToName:     "artifact.json",
	}, cli.renameReplaced)
	require.True(t, result.Replaced)
	require.True(t, result.OldBodyPresent)
	require.False(t, result.OldBodyMalformed)
	require.Equal(t, oldBody, result.OldBody)
	require.True(t, result.OldInodeDeleted)
}

func TestPublishArtifactReplaceDoesNotFailAfterCommittedMalformedOldBody(t *testing.T) {
	cli := &artifactFake{
		renameReplaceRes: model.RenameReplaceResult{
			Replaced: true,
			OldInode: model.InodeRecord{
				Inode:       9,
				Type:        model.InodeTypeFile,
				OpaqueAttrs: []byte("legacy attrs"),
			},
		},
	}
	req := publishArtifactRequest()
	req.Replace = true

	result, err := PublishArtifact(context.Background(), cli, req)
	require.NoError(t, err)
	require.True(t, result.OldBodyMalformed)
	require.False(t, result.OldBodyPresent)
}

func TestPublishArtifactRejectsInvalidBodyBeforeCreate(t *testing.T) {
	cli := &artifactFake{}
	req := publishArtifactRequest()
	req.Body.BodyRef = ""

	_, err := PublishArtifact(context.Background(), cli, req)
	require.ErrorIs(t, err, model.ErrInvalidValue)
	require.Empty(t, cli.calls)
}

func TestPublishArtifactPropagatesFinalRenameFailureAndLeavesStage(t *testing.T) {
	renameErr := errors.New("rename failed")
	cli := &artifactFake{renameErr: renameErr}

	_, err := PublishArtifact(context.Background(), cli, publishArtifactRequest())
	require.ErrorIs(t, err, renameErr)
	require.Equal(t, []string{"create", "rename"}, cli.calls)
}

func TestRemoveArtifactReturnsOldBodyDescriptor(t *testing.T) {
	body := model.BodyDescriptor{BodyRef: "cas://old", Size: 99}
	cli := &artifactFake{
		removeRes: model.RemoveResult{
			RemovedDentry: model.DentryRecord{Parent: 8, Name: "artifact.json", Inode: 9, Type: model.InodeTypeFile},
			OldInode:      model.InodeRecord{Inode: 9, Type: model.InodeTypeFile, OpaqueAttrs: mustBodyAttrs(t, body)},
			InodeDeleted:  true,
		},
	}

	result, err := RemoveArtifact(context.Background(), cli, model.RemoveRequest{Mount: "vol", Parent: 8, Name: "artifact.json"})
	require.NoError(t, err)
	require.Equal(t, []string{"remove"}, cli.calls)
	require.Equal(t, model.RemoveRequest{Mount: "vol", Parent: 8, Name: "artifact.json"}, cli.removed)
	require.True(t, result.OldBodyPresent)
	require.Equal(t, body, result.OldBody)
	require.True(t, result.InodeDeleted)
}

func publishArtifactRequest() PublishArtifactRequest {
	return PublishArtifactRequest{
		Mount:       "vol",
		StageParent: 7,
		StageName:   ".stage-artifact",
		FinalParent: 8,
		FinalName:   "artifact.json",
		Attrs:       model.CreateAttrs{Mode: 0o644},
		Body: model.BodyDescriptor{
			Producer:    "agent",
			DigestURI:   "sha256:new",
			Size:        42,
			ContentType: "application/json",
			BodyRef:     "cas://new",
			Generation:  4,
		},
	}
}

func mustBodyAttrs(t *testing.T, desc model.BodyDescriptor) []byte {
	t.Helper()
	encoded, err := model.EncodeBodyDescriptor(desc)
	require.NoError(t, err)
	return encoded
}
