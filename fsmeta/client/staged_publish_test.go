package client

import (
	"context"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

type stagedPublishFake struct {
	createErr error
	renameErr error
	unlinkErr error

	calls        []string
	created      fsmeta.CreateRequest
	renamed      fsmeta.RenameSubtreeRequest
	unlinked     fsmeta.UnlinkRequest
	unlinkCtxErr error
}

func (f *stagedPublishFake) Create(_ context.Context, req fsmeta.CreateRequest, _ fsmeta.InodeRecord) error {
	f.calls = append(f.calls, "create")
	f.created = req
	return f.createErr
}

func (f *stagedPublishFake) RenameSubtree(_ context.Context, req fsmeta.RenameSubtreeRequest) error {
	f.calls = append(f.calls, "rename")
	f.renamed = req
	return f.renameErr
}

func (f *stagedPublishFake) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	f.calls = append(f.calls, "unlink")
	f.unlinked = req
	f.unlinkCtxErr = ctx.Err()
	return f.unlinkErr
}

func TestPublishStagedNamespaceEntryCommitsAfterPrepare(t *testing.T) {
	cli := &stagedPublishFake{}
	var prepared fsmeta.CreateRequest
	req := stagedPublishRequest()

	err := PublishStagedNamespaceEntry(context.Background(), cli, req, func(_ context.Context, stage fsmeta.CreateRequest) error {
		cli.calls = append(cli.calls, "prepare")
		prepared = stage
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"create", "prepare", "rename"}, cli.calls)
	require.Equal(t, fsmeta.CreateRequest{Mount: "vol", Parent: 7, Name: ".stage-artifact", Inode: 99}, cli.created)
	require.Equal(t, cli.created, prepared)
	require.Equal(t, fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   ".stage-artifact",
		ToParent:   8,
		ToName:     "artifact",
	}, cli.renamed)
}

func TestPublishStagedNamespaceEntryCleansUpWhenPrepareFails(t *testing.T) {
	cli := &stagedPublishFake{}
	prepareErr := errors.New("body upload failed")
	req := stagedPublishRequest()

	err := PublishStagedNamespaceEntry(context.Background(), cli, req, func(context.Context, fsmeta.CreateRequest) error {
		cli.calls = append(cli.calls, "prepare")
		return prepareErr
	})
	require.ErrorIs(t, err, prepareErr)
	require.Equal(t, []string{"create", "prepare", "unlink"}, cli.calls)
	require.Equal(t, fsmeta.UnlinkRequest{Mount: "vol", Parent: 7, Name: ".stage-artifact"}, cli.unlinked)
}

func TestPublishStagedNamespaceEntryReportsCleanupFailure(t *testing.T) {
	cli := &stagedPublishFake{unlinkErr: errors.New("unlink failed")}
	prepareErr := errors.New("prepare failed")

	err := PublishStagedNamespaceEntry(context.Background(), cli, stagedPublishRequest(), func(context.Context, fsmeta.CreateRequest) error {
		return prepareErr
	})
	require.ErrorIs(t, err, prepareErr)
	require.ErrorIs(t, err, cli.unlinkErr)
}

func TestPublishStagedNamespaceEntryCleanupIgnoresCanceledRequestContext(t *testing.T) {
	cli := &stagedPublishFake{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := PublishStagedNamespaceEntry(ctx, cli, stagedPublishRequest(), func(context.Context, fsmeta.CreateRequest) error {
		return context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"create", "unlink"}, cli.calls)
	require.NoError(t, cli.unlinkCtxErr)
}

func TestPublishStagedNamespaceEntryLeavesStageOnRenameFailure(t *testing.T) {
	renameErr := errors.New("rename failed")
	cli := &stagedPublishFake{renameErr: renameErr}

	err := PublishStagedNamespaceEntry(context.Background(), cli, stagedPublishRequest(), nil)
	require.ErrorIs(t, err, renameErr)
	require.Equal(t, []string{"create", "rename"}, cli.calls)
	require.Zero(t, cli.unlinked)
}

func TestPublishStagedNamespaceEntryRejectsInvalidRequestBeforeCreate(t *testing.T) {
	cli := &stagedPublishFake{}
	req := stagedPublishRequest()
	req.StageName = ""

	err := PublishStagedNamespaceEntry(context.Background(), cli, req, nil)
	require.Error(t, err)
	require.Empty(t, cli.calls)
}

func stagedPublishRequest() StagedPublishRequest {
	return StagedPublishRequest{
		Mount:       "vol",
		StageParent: 7,
		StageName:   ".stage-artifact",
		FinalParent: 8,
		FinalName:   "artifact",
		InodeID:     99,
		Inode: fsmeta.InodeRecord{
			Type:      fsmeta.InodeTypeFile,
			Mode:      0o644,
			LinkCount: 1,
		},
	}
}
