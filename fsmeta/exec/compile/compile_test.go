// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import (
	"slices"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

var testMount = model.MountIdentity{MountID: "vol", MountKeyID: 7}
var benchmarkCompiledOp CompiledOp

func testParentInSameBucket(t *testing.T, base model.InodeID) model.InodeID {
	t.Helper()
	want := layout.BucketForInodeID(base)
	for candidate := base + 1; candidate < base+4096; candidate++ {
		if layout.BucketForInodeID(candidate) == want {
			return candidate
		}
	}
	t.Fatalf("no same-bucket parent found for inode %d", base)
	return 0
}

func testParentInDifferentBucket(t *testing.T, base model.InodeID) model.InodeID {
	t.Helper()
	want := layout.BucketForInodeID(base)
	for candidate := base + 1; candidate < base+4096; candidate++ {
		if layout.BucketForInodeID(candidate) != want {
			return candidate
		}
	}
	t.Fatalf("no different-bucket parent found for inode %d", base)
	return 0
}

func testCompileAOT(tb testing.TB, delta SemanticDelta) CompiledOp {
	tb.Helper()
	compiled, err := compileAOTDelta(delta)
	require.NoError(tb, err)
	return compiled
}

func testCreateDelta(tb testing.TB, req model.CreateRequest, mount model.MountIdentity, inodeID model.InodeID, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileCreateProgram(req, mount, inodeID, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testUpdateInodeDelta(tb testing.TB, req model.UpdateInodeRequest, mount model.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileUpdateInodeProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testRenameDelta(tb testing.TB, req model.RenameRequest, mount model.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileRenameProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testRenameReplaceDelta(tb testing.TB, req model.RenameReplaceRequest, mount model.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileRenameReplaceProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testSnapshotSubtreeDelta(tb testing.TB, req model.SnapshotSubtreeRequest, mount model.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileSnapshotSubtreeProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testLinkDelta(tb testing.TB, req model.LinkRequest, mount model.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileLinkProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testUnlinkDelta(tb testing.TB, req model.UnlinkRequest, mount model.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileUnlinkProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testRemoveDelta(tb testing.TB, req model.RemoveRequest, mount model.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileRemoveProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testOpenWriteSessionDelta(tb testing.TB, req model.OpenWriteSessionRequest, mount model.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileOpenWriteSessionProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testHeartbeatWriteSessionDelta(tb testing.TB, req model.HeartbeatWriteSessionRequest, mount model.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileHeartbeatWriteSessionProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testCloseWriteSessionDelta(tb testing.TB, req model.CloseWriteSessionRequest, mount model.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileCloseWriteSessionProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testExpireWriteSessionsDelta(tb testing.TB, req model.ExpireWriteSessionsRequest, mount model.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileExpireWriteSessionsProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func TestCreateCompilesVisibleCommitDelta(t *testing.T) {
	req := model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 128, Mode: 0o644},
	}
	delta, err := testCreateDelta(t, req, testMount, 22)
	require.NoError(t, err)

	dentryKey, err := layout.EncodeDentryKey(testMount, model.RootInode, "file")
	require.NoError(t, err)
	parentKey, err := layout.EncodeInodeKey(testMount, model.RootInode)
	require.NoError(t, err)
	inodeKey, err := layout.EncodeInodeKey(testMount, 22)
	require.NoError(t, err)

	require.Equal(t, model.OperationCreate, delta.Kind)
	require.Equal(t, EligibilityVisibleCommit, delta.Eligibility)
	require.Equal(t, SlowReasonNone, delta.SlowReason)
	require.Equal(t, []Predicate{
		{Kind: PredicateObservedValue, Key: parentKey},
		{Kind: PredicateNotExists, Key: dentryKey},
		{Kind: PredicateNotExists, Key: inodeKey},
	}, delta.ReadPredicates)
	require.Len(t, delta.WriteEffects, 3)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[0].Kind)
	require.Equal(t, parentKey, delta.WriteEffects[0].Key)
	require.Equal(t, EffectPut, delta.WriteEffects[1].Kind)
	require.Equal(t, dentryKey, delta.WriteEffects[1].Key)
	require.Equal(t, EffectPut, delta.WriteEffects[2].Kind)
	require.Equal(t, inodeKey, delta.WriteEffects[2].Key)

	dentry, err := layout.DecodeDentryValue(delta.WriteEffects[1].Value)
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{Parent: model.RootInode, Name: "file", Inode: 22, Type: model.InodeTypeFile}, dentry)
	inode, err := layout.DecodeInodeValue(delta.WriteEffects[2].Value)
	require.NoError(t, err)
	require.Equal(t, model.InodeID(22), inode.Inode)
	require.Equal(t, uint64(128), inode.Size)

	require.Equal(t, []model.InodeID{model.RootInode}, delta.Authority.Parents)
	require.Equal(t, []model.InodeID{22}, delta.Authority.Inodes)
	require.True(t, slices.Contains(delta.Authority.Buckets, layout.BucketForInodeID(model.RootInode)))
	require.True(t, slices.Contains(delta.Authority.Buckets, layout.BucketForInodeID(22)))
}

func TestCreateCompilesMetadataCommandDescriptor(t *testing.T) {
	inodeID := testParentInDifferentBucket(t, model.RootInode)
	delta, err := testCreateDelta(t, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	}, testMount, inodeID)
	require.NoError(t, err)
	dentryKey, err := layout.EncodeDentryKey(testMount, model.RootInode, "file")
	require.NoError(t, err)

	op := testCompileAOT(t, delta)
	require.Equal(t, delta.Kind, op.Delta.Kind)
	require.True(t, op.Authority.Required)
	require.Equal(t, FenceActiveAuthority, op.Authority.Fence)
	require.Equal(t, DurabilityVisibleOnly, op.Durability)
	require.Equal(t, []MutationID{0, 1, 2}, op.Atomicity.Members)
	require.False(t, op.Atomicity.Splittable)
	require.Equal(t, RecoveryReplayAllOrNothing, op.Atomicity.Recovery)
	require.Equal(t, op.DescriptorDigest, op.Atomicity.Digest)

	require.Len(t, op.Predicates, 3)
	require.True(t, op.Predicates[0].NeedValue)
	require.False(t, op.Predicates[0].NeedAbsent)
	require.True(t, op.Predicates[1].NeedAbsent)
	require.True(t, op.Predicates[2].NeedAbsent)
	require.Len(t, op.Effects, 3)
	require.Equal(t, MutationID(0), op.Effects[0].ID)
	require.Equal(t, DerivationRuntimeValue, op.Effects[0].Derivation)
	require.False(t, op.Effects[0].Concrete)
	require.Equal(t, testMount.MountKeyID, op.Effects[0].MountKeyID)
	require.Equal(t, layout.KeyKindInode, op.Effects[0].RecordKind)
	require.Len(t, op.Watch, 1)
	require.Equal(t, WatchEventCreate, op.Watch[0].EventKind)
	require.Equal(t, WatchEmitVisible, op.Watch[0].EmitAt)
	require.Equal(t, dentryKey, op.Watch[0].Key)
	require.Equal(t, model.RootInode, op.Watch[0].Parent)
	require.Equal(t, "file", op.Watch[0].Name)
	require.Equal(t, inodeID, op.Watch[0].Inode)
	require.Len(t, op.Footprint.Reads, 3)
	require.Len(t, op.Footprint.Writes, 3)
	require.Len(t, op.Footprint.ConflictKeys, 6)
	require.False(t, op.Footprint.HasPrefixRead)
	require.False(t, op.Footprint.HasOpaqueKeys)
	require.True(t, op.Footprint.EstimatedBytes > 0)
	require.True(t, op.Completion.RetainCompletion)
	require.Equal(t, CompletionVisible, op.Completion.Kind)
	require.Equal(t, uint32(3), op.Completion.MutationCount)
	require.Equal(t, op.DescriptorDigest, op.Completion.DescriptorDigest)
}

func TestDerivedOperationKeepsRuntimeEffectMetadata(t *testing.T) {
	delta, err := testUpdateInodeDelta(t, model.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  3,
		Inode:   44,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	}, testMount)
	require.NoError(t, err)

	op := testCompileAOT(t, delta)
	require.True(t, op.Authority.Required)
	require.Equal(t, FenceActiveAuthority, op.Authority.Fence)
	require.Equal(t, DurabilityVisibleOnly, op.Durability)
	require.Len(t, op.Predicates, 2)
	require.True(t, op.Predicates[0].NeedValue)
	require.True(t, op.Predicates[1].NeedValue)
	require.Len(t, op.Effects, 1)
	require.Equal(t, DerivationRuntimeValue, op.Effects[0].Derivation)
	require.False(t, op.Effects[0].Concrete)
}

func TestGeneratedCreateProgramMatchesCurrentCompiler(t *testing.T) {
	req := model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 128, Mode: 0o644},
	}
	inodeID := testParentInDifferentBucket(t, model.RootInode)
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{name: "default"},
		{name: "escrow", opts: []Option{WithQuotaMode(QuotaModeEscrow)}},
		{name: "shared", opts: []Option{WithQuotaMode(QuotaModeShared)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delta, err := testCreateDelta(t, req, testMount, inodeID, tc.opts...)
			require.NoError(t, err)

			program, err := CompileCreateProgram(req, testMount, inodeID, tc.opts...)
			require.NoError(t, err)
			require.Equal(t, testCompileAOT(t, delta), program.Compiled)
		})
	}
}

func TestSessionProgramsMatchCurrentCompiler(t *testing.T) {
	openReq := model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	openDelta, err := testOpenWriteSessionDelta(t, openReq, testMount)
	require.NoError(t, err)
	openProgram, err := CompileOpenWriteSessionProgram(openReq, testMount)
	require.NoError(t, err)
	require.Equal(t, testCompileAOT(t, openDelta), openProgram.Compiled)

	heartbeatReq := model.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	heartbeatDelta, err := testHeartbeatWriteSessionDelta(t, heartbeatReq, testMount)
	require.NoError(t, err)
	heartbeatProgram, err := CompileHeartbeatWriteSessionProgram(heartbeatReq, testMount)
	require.NoError(t, err)
	require.Equal(t, testCompileAOT(t, heartbeatDelta), heartbeatProgram.Compiled)

	closeReq := model.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
	}
	closeDelta, err := testCloseWriteSessionDelta(t, closeReq, testMount)
	require.NoError(t, err)
	closeProgram, err := CompileCloseWriteSessionProgram(closeReq, testMount)
	require.NoError(t, err)
	require.Equal(t, testCompileAOT(t, closeDelta), closeProgram.Compiled)
}

func TestGeneratedProgramEntriesAreCanonical(t *testing.T) {
	renameToParent := testParentInSameBucket(t, model.RootInode)
	crossBucketParent := testParentInDifferentBucket(t, model.RootInode)
	for _, tc := range []struct {
		name    string
		compile func() (CompiledOp, error)
	}{
		{name: "update_inode", compile: func() (CompiledOp, error) {
			program, err := CompileUpdateInodeProgram(model.UpdateInodeRequest{Mount: "vol", Parent: model.RootInode, Inode: 44, Name: "file", SetMode: true, Mode: 0o600}, testMount, WithQuotaMode(QuotaModeEscrow))
			return program.Compiled, err
		}},
		{name: "lookup", compile: func() (CompiledOp, error) {
			program, err := CompileLookupProgram(model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: "file"}, testMount)
			return program.Compiled, err
		}},
		{name: "readdir", compile: func() (CompiledOp, error) {
			program, err := CompileReadDirProgram(model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, Limit: 32}, testMount)
			return program.Compiled, err
		}},
		{name: "snapshot_subtree", compile: func() (CompiledOp, error) {
			program, err := CompileSnapshotSubtreeProgram(model.SnapshotSubtreeRequest{Mount: "vol", RootInode: model.RootInode}, testMount)
			return program.Compiled, err
		}},
		{name: "rename", compile: func() (CompiledOp, error) {
			program, err := CompileRenameProgram(model.RenameRequest{Mount: "vol", FromParent: model.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount)
			return program.Compiled, err
		}},
		{name: "rename_replace", compile: func() (CompiledOp, error) {
			program, err := CompileRenameReplaceProgram(model.RenameReplaceRequest{Mount: "vol", FromParent: model.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount)
			return program.Compiled, err
		}},
		{name: "rename_subtree", compile: func() (CompiledOp, error) {
			program, err := CompileRenameSubtreeProgram(model.RenameSubtreeRequest{Mount: "vol", FromParent: model.RootInode, FromName: "old", ToParent: crossBucketParent, ToName: "new"}, testMount)
			return program.Compiled, err
		}},
		{name: "link", compile: func() (CompiledOp, error) {
			program, err := CompileLinkProgram(model.LinkRequest{Mount: "vol", FromParent: model.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount, WithQuotaMode(QuotaModeEscrow))
			return program.Compiled, err
		}},
		{name: "unlink", compile: func() (CompiledOp, error) {
			program, err := CompileUnlinkProgram(model.UnlinkRequest{Mount: "vol", Parent: model.RootInode, Name: "old"}, testMount, WithQuotaMode(QuotaModeEscrow))
			return program.Compiled, err
		}},
		{name: "remove", compile: func() (CompiledOp, error) {
			program, err := CompileRemoveProgram(model.RemoveRequest{Mount: "vol", Parent: model.RootInode, Name: "old"}, testMount, WithQuotaMode(QuotaModeEscrow))
			return program.Compiled, err
		}},
		{name: "remove_directory", compile: func() (CompiledOp, error) {
			program, err := CompileRemoveDirectoryProgram(model.RemoveDirectoryRequest{Mount: "vol", Parent: model.RootInode, Name: "old-dir"}, testMount)
			return program.Compiled, err
		}},
		{name: "open_write_session", compile: func() (CompiledOp, error) {
			program, err := CompileOpenWriteSessionProgram(model.OpenWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}, testMount)
			return program.Compiled, err
		}},
		{name: "heartbeat_write_session", compile: func() (CompiledOp, error) {
			program, err := CompileHeartbeatWriteSessionProgram(model.HeartbeatWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}, testMount)
			return program.Compiled, err
		}},
		{name: "close_write_session", compile: func() (CompiledOp, error) {
			program, err := CompileCloseWriteSessionProgram(model.CloseWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1"}, testMount)
			return program.Compiled, err
		}},
		{name: "expire_write_sessions", compile: func() (CompiledOp, error) {
			program, err := CompileExpireWriteSessionsProgram(model.ExpireWriteSessionsRequest{Mount: "vol", Limit: 16}, testMount)
			return program.Compiled, err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := tc.compile()
			require.NoError(t, err)
			require.Equal(t, testCompileAOT(t, compiled.Delta), compiled)
		})
	}
}

func TestSessionProgramsRejectInvalidTTL(t *testing.T) {
	_, err := CompileOpenWriteSessionProgram(model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
	}, testMount)
	require.ErrorIs(t, err, model.ErrInvalidRequest)

	_, err = CompileHeartbeatWriteSessionProgram(model.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
	}, testMount)
	require.ErrorIs(t, err, model.ErrInvalidRequest)
}

func TestCreateRespectsQuotaMode(t *testing.T) {
	req := model.CreateRequest{
		Mount:  "vol",
		Parent: 3,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	}

	shared, err := testCreateDelta(t, req, testMount, 44, WithQuotaMode(QuotaModeShared))
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, shared.Eligibility)
	require.Equal(t, SlowReasonSharedQuota, shared.SlowReason)

	escrow, err := testCreateDelta(t, req, testMount, 44, WithQuotaMode(QuotaModeEscrow))
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, escrow.Eligibility)
	require.Contains(t, escrow.RuntimeGuards, GuardQuotaCredit)
}

func TestRenameBucketLocalVisibleCrossBucketSlow(t *testing.T) {
	sameParent, err := testRenameDelta(t, model.RenameRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, sameParent.Eligibility)
	require.Equal(t, []model.InodeID{8}, sameParent.Authority.Parents)
	require.Equal(t, []WriteEffect{
		{Kind: EffectDelete, Key: sameParent.Plan.MutateKeys[0]},
		{Kind: EffectDerivedPut, Key: sameParent.Plan.MutateKeys[1]},
		{Kind: EffectDerivedPut, Key: sameParent.Plan.MutateKeys[2]},
		{Kind: EffectDerivedPut, Key: sameParent.Plan.MutateKeys[3]},
	}, sameParent.WriteEffects)

	sameBucketParent := testParentInSameBucket(t, 8)
	crossParentSameBucket, err := testRenameDelta(t, model.RenameRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   "old",
		ToParent:   sameBucketParent,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, crossParentSameBucket.Eligibility)
	require.Equal(t, SlowReasonNone, crossParentSameBucket.SlowReason)
	require.Equal(t, []model.InodeID{8, sameBucketParent}, crossParentSameBucket.Authority.Parents)
	require.Len(t, crossParentSameBucket.Authority.Buckets, 1)

	differentBucketParent := testParentInDifferentBucket(t, 8)
	crossBucket, err := testRenameDelta(t, model.RenameRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   "old",
		ToParent:   differentBucketParent,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, crossBucket.Eligibility)
	require.Equal(t, SlowReasonCrossBucket, crossBucket.SlowReason)
	require.Equal(t, []model.InodeID{8, differentBucketParent}, crossBucket.Authority.Parents)
	require.Len(t, crossBucket.Authority.Buckets, 2)
}

func TestRenameReplaceCompilesSlowPathWithoutBarrier(t *testing.T) {
	delta, err := testRenameReplaceDelta(t, model.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   ".stage-artifact",
		ToParent:   8,
		ToName:     "artifact",
	}, testMount)
	require.NoError(t, err)

	require.Equal(t, model.OperationRenameReplace, delta.Kind)
	require.Equal(t, EligibilitySlowPath, delta.Eligibility)
	require.Equal(t, SlowReasonDynamicWriteSet, delta.SlowReason)
	require.False(t, delta.DurabilityBarrier)
	require.False(t, delta.WatchAtSeal)
	require.Equal(t, []model.InodeID{8}, delta.Authority.Parents)
	require.Len(t, delta.ReadPredicates, 4)
	for _, predicate := range delta.ReadPredicates {
		require.Equal(t, PredicateObservedValue, predicate.Kind)
	}
	require.Len(t, delta.WriteEffects, 4)
	require.Equal(t, EffectDelete, delta.WriteEffects[0].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[1].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[2].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[3].Kind)

	compiled := testCompileAOT(t, delta)
	require.Equal(t, DurabilityVisibleOnly, compiled.Durability)
	require.False(t, compiled.Authority.Required)
	require.Equal(t, FenceNone, compiled.Authority.Fence)
}

func TestSlowPathBoundariesStayExplicit(t *testing.T) {
	snapshot, err := testSnapshotSubtreeDelta(t, model.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 11,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, snapshot.Eligibility)
	require.Equal(t, SlowReasonDurabilityBarrier, snapshot.SlowReason)
	require.True(t, snapshot.DurabilityBarrier)
	compiledSnapshot := testCompileAOT(t, snapshot)
	require.False(t, compiledSnapshot.Authority.Required)
	require.Equal(t, FenceNone, compiledSnapshot.Authority.Fence)
	require.Equal(t, DurabilityNeedsPublishCheckpoint, compiledSnapshot.Durability)

	expire, err := testExpireWriteSessionsDelta(t, model.ExpireWriteSessionsRequest{
		Mount: "vol",
		Limit: 8,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, expire.Eligibility)
	require.Equal(t, SlowReasonMaintenanceScan, expire.SlowReason)
	require.Len(t, expire.ReadPredicates, layout.DefaultAffinityBucketCount)
}

func TestLinkAndUnlinkCompileRuntimeConcreteVisibleCommitDeltas(t *testing.T) {
	link, err := testLinkDelta(t, model.LinkRequest{
		Mount:      "vol",
		FromParent: 4,
		FromName:   "src",
		ToParent:   4,
		ToName:     "dst",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, link.Eligibility)
	require.Empty(t, link.SlowReason)
	require.Contains(t, link.RuntimeGuards, GuardSameAuthority)
	require.Len(t, link.WriteEffects, 3)
	require.Equal(t, EffectDerivedPut, link.WriteEffects[0].Kind)
	require.Equal(t, EffectDerivedPut, link.WriteEffects[1].Kind)
	require.Equal(t, EffectDerivedPut, link.WriteEffects[2].Kind)

	delta, err := testUnlinkDelta(t, model.UnlinkRequest{
		Mount:  "vol",
		Parent: 5,
		Name:   "file",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, delta.Eligibility)
	require.Empty(t, delta.SlowReason)
	require.Equal(t, EffectDelete, delta.WriteEffects[0].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[1].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[2].Kind)

	remove, err := testRemoveDelta(t, model.RemoveRequest{
		Mount:  "vol",
		Parent: 5,
		Name:   "file",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, remove.Eligibility)
	require.Empty(t, remove.SlowReason)
	require.Equal(t, EffectDelete, remove.WriteEffects[0].Kind)
	require.Equal(t, EffectDerivedPut, remove.WriteEffects[1].Kind)
	require.Equal(t, EffectDerivedPut, remove.WriteEffects[2].Kind)
}

func BenchmarkCompileCreateProgram(b *testing.B) {
	req := model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 128, Mode: 0o644},
	}
	inodeID := model.InodeID(22)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		program, err := CompileCreateProgram(req, testMount, inodeID, WithQuotaMode(QuotaModeEscrow))
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCompiledOp = program.Compiled
	}
}

func BenchmarkCompileOpenWriteSessionProgram(b *testing.B) {
	req := model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		program, err := CompileOpenWriteSessionProgram(req, testMount)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCompiledOp = program.Compiled
	}
}
