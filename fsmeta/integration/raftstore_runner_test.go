package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestRaftstoreRunnerExecutorContractOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	executor := openRealClusterExecutor(t, ctx)

	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "checkpoint-0001",
		Inode:  42,
	}
	err := executor.Create(ctx, req, fsmeta.InodeRecord{
		Type:      fsmeta.InodeTypeFile,
		Size:      4096,
		LinkCount: 1,
		Mode:      0o644,
	})
	require.NoError(t, err)

	record, err := executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Name:   req.Name,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  req.Inode,
		Type:   fsmeta.InodeTypeFile,
	}, record)

	entries, err := executor.ReadDir(ctx, fsmeta.ReadDirRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryRecord{{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  req.Inode,
		Type:   fsmeta.InodeTypeFile,
	}}, entries)

	pairs, err := executor.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryAttrPair{{
		Dentry: fsmeta.DentryRecord{
			Parent: req.Parent,
			Name:   req.Name,
			Inode:  req.Inode,
			Type:   fsmeta.InodeTypeFile,
		},
		Inode: fsmeta.InodeRecord{
			Inode:     req.Inode,
			Type:      fsmeta.InodeTypeFile,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}}, pairs)

	err = executor.Create(ctx, req, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.True(t, errors.Is(err, fsmeta.ErrExists), "duplicate create error = %v", err)
}

func TestRaftstoreRunnerRenameAcrossRegionsOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	executor := openSplitRealClusterExecutor(t, ctx)

	err := executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "alpha",
		Inode:  61,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)

	err = executor.RenameSubtree(ctx, fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   "alpha",
		ToParent:   fsmeta.RootInode,
		ToName:     "zulu",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "alpha",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)

	record, err := executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "zulu",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: fsmeta.RootInode,
		Name:   "zulu",
		Inode:  61,
		Type:   fsmeta.InodeTypeFile,
	}, record)

	require.NoError(t, executor.Unlink(ctx, fsmeta.UnlinkRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "zulu",
	}))
	_, err = executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "zulu",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
}
