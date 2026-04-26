package integration

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/stretchr/testify/require"
)

func TestFSMetadataNamespaceChaosSurvivesGatewayRestartAndMixedMutations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	runtime := openRealClusterRuntime(t, ctx)
	cli, cleanup := openFSMetadataClient(t, ctx, runtime.executor)
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()

	mount := fsmeta.MountID("vol")
	require.NoError(t, cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "alpha",
		Inode:  1001,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, Size: 64, Mode: 0o644, LinkCount: 1}))
	require.NoError(t, cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "beta",
		Inode:  1002,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, Size: 128, Mode: 0o644, LinkCount: 1}))
	require.NoError(t, cli.Link(ctx, fsmeta.LinkRequest{
		Mount:      mount,
		FromParent: fsmeta.RootInode,
		FromName:   "alpha",
		ToParent:   fsmeta.RootInode,
		ToName:     "alpha-link",
	}))
	require.NoError(t, cli.RenameSubtree(ctx, fsmeta.RenameSubtreeRequest{
		Mount:      mount,
		FromParent: fsmeta.RootInode,
		FromName:   "beta",
		ToParent:   fsmeta.RootInode,
		ToName:     "zeta",
	}))

	// Simulate an fsmeta gateway restart while the raftstore and coordinator
	// stay alive. The new gateway must observe the same persisted namespace
	// state and preserve inode/link invariants after more mutations.
	cleanup()
	cleanup = nil
	cli, cleanup = openFSMetadataClient(t, ctx, runtime.executor)

	require.NoError(t, cli.Unlink(ctx, fsmeta.UnlinkRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "alpha",
	}))
	require.NoError(t, cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "gamma",
		Inode:  1003,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, Size: 256, Mode: 0o644, LinkCount: 1}))

	pairs, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Limit:  16,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"alpha-link", "gamma", "zeta"}, dentryNames(pairs))
	assertNamespaceInvariants(t, pairs)
}

func TestFSMetadataRenameSubtreePublishesSingleHandoffOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	publisher := &recordingSubtreeHandoffPublisher{}
	runtime := openRealClusterRuntimeWithOptions(t, ctx, fsmetaexec.WithSubtreeHandoffPublisher(publisher))
	cli, cleanup := openFSMetadataClient(t, ctx, runtime.executor)
	defer cleanup()

	mount := fsmeta.MountID("vol")
	require.NoError(t, cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "source",
		Inode:  1101,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, Size: 512, Mode: 0o644, LinkCount: 1}))
	require.NoError(t, cli.RenameSubtree(ctx, fsmeta.RenameSubtreeRequest{
		Mount:      mount,
		FromParent: fsmeta.RootInode,
		FromName:   "source",
		ToParent:   fsmeta.RootInode,
		ToName:     "target",
	}))

	require.Equal(t, 1, publisher.starts)
	require.Equal(t, 1, publisher.completes)
	require.Equal(t, mount, publisher.mount)
	require.Equal(t, fsmeta.RootInode, publisher.root)
	require.NotZero(t, publisher.startFrontier)
	require.Equal(t, publisher.startFrontier, publisher.completeFrontier)
}

func assertNamespaceInvariants(t *testing.T, pairs []fsmeta.DentryAttrPair) {
	t.Helper()
	seenNames := make(map[string]struct{}, len(pairs))
	linkRefs := make(map[fsmeta.InodeID]uint32)
	linkCounts := make(map[fsmeta.InodeID]uint32)
	for _, pair := range pairs {
		require.NotEmpty(t, pair.Dentry.Name)
		if _, ok := seenNames[pair.Dentry.Name]; ok {
			t.Fatalf("duplicate dentry name %q", pair.Dentry.Name)
		}
		seenNames[pair.Dentry.Name] = struct{}{}
		require.Equal(t, pair.Dentry.Inode, pair.Inode.Inode)
		require.NotZero(t, pair.Inode.LinkCount)
		linkRefs[pair.Inode.Inode]++
		linkCounts[pair.Inode.Inode] = pair.Inode.LinkCount
	}
	for inode, refs := range linkRefs {
		require.Equalf(t, refs, linkCounts[inode], "inode %d link count", inode)
	}
}

type recordingSubtreeHandoffPublisher struct {
	starts           int
	completes        int
	mount            fsmeta.MountID
	root             fsmeta.InodeID
	startFrontier    uint64
	completeFrontier uint64
}

func (p *recordingSubtreeHandoffPublisher) StartSubtreeHandoff(_ context.Context, mount fsmeta.MountID, root fsmeta.InodeID, frontier uint64) error {
	p.starts++
	p.mount = mount
	p.root = root
	p.startFrontier = frontier
	return nil
}

func (p *recordingSubtreeHandoffPublisher) CompleteSubtreeHandoff(_ context.Context, mount fsmeta.MountID, root fsmeta.InodeID, frontier uint64) error {
	p.completes++
	p.mount = mount
	p.root = root
	p.completeFrontier = frontier
	return nil
}
