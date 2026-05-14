package compile

import (
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

var testMount = fsmeta.MountIdentity{MountID: "vol", MountKeyID: 7}
var benchmarkCompiledOp CompiledOp

func testParentInSameBucket(t *testing.T, base fsmeta.InodeID) fsmeta.InodeID {
	t.Helper()
	want := fsmeta.BucketForInodeID(base)
	for candidate := base + 1; candidate < base+4096; candidate++ {
		if fsmeta.BucketForInodeID(candidate) == want {
			return candidate
		}
	}
	t.Fatalf("no same-bucket parent found for inode %d", base)
	return 0
}

func testParentInDifferentBucket(t *testing.T, base fsmeta.InodeID) fsmeta.InodeID {
	t.Helper()
	want := fsmeta.BucketForInodeID(base)
	for candidate := base + 1; candidate < base+4096; candidate++ {
		if fsmeta.BucketForInodeID(candidate) != want {
			return candidate
		}
	}
	t.Fatalf("no different-bucket parent found for inode %d", base)
	return 0
}

func testParentInDifferentBucketAfter(t *testing.T, base, start fsmeta.InodeID) fsmeta.InodeID {
	t.Helper()
	want := fsmeta.BucketForInodeID(base)
	for candidate := start; candidate < start+4096; candidate++ {
		if fsmeta.BucketForInodeID(candidate) != want {
			return candidate
		}
	}
	t.Fatalf("no different-bucket parent found for inode %d after %d", base, start)
	return 0
}

func mustInodeKey(t *testing.T, inode fsmeta.InodeID) []byte {
	t.Helper()
	key, err := fsmeta.EncodeInodeKey(testMount, inode)
	require.NoError(t, err)
	return key
}

func testPredicateProof(key, value []byte, present bool, version uint64, source ReadSource) PredicateProof {
	proof := PredicateProof{
		Key:     cloneBytes(key),
		Present: present,
		Value:   cloneBytes(value),
		Version: version,
		Source:  source,
	}
	proof.Digest = PredicateProofDigest(proof.Key, proof.Value, proof.Present, proof.Version, proof.Source)
	return proof
}

func testCompileAOT(tb testing.TB, delta SemanticDelta) CompiledOp {
	tb.Helper()
	compiled, err := compileAOTDelta(delta)
	require.NoError(tb, err)
	return compiled
}

func testMaterializeAOT(tb testing.TB, delta SemanticDelta, proofs []PredicateProof) MaterializedOp {
	tb.Helper()
	compiled := testCompileAOT(tb, delta)
	return MaterializedOp{
		CompiledOp:      compiled,
		PredicateProofs: clonePredicateProofs(proofs),
	}
}

func testMaterializeAOTWithEffects(tb testing.TB, op CompiledOp, effects []WriteEffect, proofs []PredicateProof) MaterializedOp {
	tb.Helper()
	materialized, err := MaterializeCompiledOpWithEvidence(op, effects, PredicateEvidence{Proofs: proofs}, nil)
	require.NoError(tb, err)
	return materialized
}

func testCreateDelta(tb testing.TB, req fsmeta.CreateRequest, mount fsmeta.MountIdentity, inodeID fsmeta.InodeID, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileCreateProgram(req, mount, inodeID, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testUpdateInodeDelta(tb testing.TB, req fsmeta.UpdateInodeRequest, mount fsmeta.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileUpdateInodeProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testRenameDelta(tb testing.TB, req fsmeta.RenameRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileRenameProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testSnapshotSubtreeDelta(tb testing.TB, req fsmeta.SnapshotSubtreeRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileSnapshotSubtreeProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testLinkDelta(tb testing.TB, req fsmeta.LinkRequest, mount fsmeta.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileLinkProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testUnlinkDelta(tb testing.TB, req fsmeta.UnlinkRequest, mount fsmeta.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileUnlinkProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testOpenWriteSessionDelta(tb testing.TB, req fsmeta.OpenWriteSessionRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileOpenWriteSessionProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testHeartbeatWriteSessionDelta(tb testing.TB, req fsmeta.HeartbeatWriteSessionRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileHeartbeatWriteSessionProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testCloseWriteSessionDelta(tb testing.TB, req fsmeta.CloseWriteSessionRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileCloseWriteSessionProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testExpireWriteSessionsDelta(tb testing.TB, req fsmeta.ExpireWriteSessionsRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileExpireWriteSessionsProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testConcreteUpdateInodeDelta(tb testing.TB, expected []byte) (SemanticDelta, []PredicateProof) {
	tb.Helper()
	delta, err := testUpdateInodeDelta(tb, fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  3,
		Inode:   44,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	}, testMount)
	require.NoError(tb, err)
	dentryValue, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
		Parent: 3,
		Name:   "file",
		Inode:  44,
		Type:   fsmeta.InodeTypeFile,
	})
	require.NoError(tb, err)
	dentryKey := delta.ReadPredicates[0].Key
	inodeKey := delta.ReadPredicates[1].Key
	delta.ReadPredicates = []Predicate{
		{Kind: PredicateObservedValue, Key: dentryKey, ExpectedValue: dentryValue, HasExpectedValue: true, RuntimeChecked: true},
		{Kind: PredicateObservedValue, Key: inodeKey, ExpectedValue: expected, HasExpectedValue: true, RuntimeChecked: true},
	}
	delta.WriteEffects = []WriteEffect{{Kind: EffectPut, Key: inodeKey, Value: []byte("new-inode")}}
	return delta, []PredicateProof{
		testPredicateProof(dentryKey, dentryValue, true, 9, ReadSourceBase),
		testPredicateProof(inodeKey, expected, true, 9, ReadSourceBase),
	}
}

func TestCreateCompilesVisibleCommitDelta(t *testing.T) {
	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 128, Mode: 0o644},
	}
	delta, err := testCreateDelta(t, req, testMount, 22)
	require.NoError(t, err)

	dentryKey, err := fsmeta.EncodeDentryKey(testMount, fsmeta.RootInode, "file")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(testMount, 22)
	require.NoError(t, err)

	require.Equal(t, fsmeta.OperationCreate, delta.Kind)
	require.Equal(t, EligibilityVisibleCommit, delta.Eligibility)
	require.Equal(t, SlowReasonNone, delta.SlowReason)
	require.Equal(t, []Predicate{
		{Kind: PredicateNotExists, Key: dentryKey},
		{Kind: PredicateNotExists, Key: inodeKey},
	}, delta.ReadPredicates)
	require.Len(t, delta.WriteEffects, 2)
	require.Equal(t, EffectPut, delta.WriteEffects[0].Kind)
	require.Equal(t, dentryKey, delta.WriteEffects[0].Key)
	require.Equal(t, EffectPut, delta.WriteEffects[1].Kind)
	require.Equal(t, inodeKey, delta.WriteEffects[1].Key)

	dentry, err := fsmeta.DecodeDentryValue(delta.WriteEffects[0].Value)
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{Parent: fsmeta.RootInode, Name: "file", Inode: 22, Type: fsmeta.InodeTypeFile}, dentry)
	inode, err := fsmeta.DecodeInodeValue(delta.WriteEffects[1].Value)
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), inode.Inode)
	require.Equal(t, uint64(128), inode.Size)

	require.Equal(t, []fsmeta.InodeID{fsmeta.RootInode}, delta.Authority.Parents)
	require.Equal(t, []fsmeta.InodeID{22}, delta.Authority.Inodes)
	require.True(t, slices.Contains(delta.Authority.Buckets, fsmeta.BucketForInodeID(fsmeta.RootInode)))
	require.True(t, slices.Contains(delta.Authority.Buckets, fsmeta.BucketForInodeID(22)))
}

func TestCreateCompilesSegmentInstallableOperation(t *testing.T) {
	inodeID := testParentInDifferentBucket(t, fsmeta.RootInode)
	delta, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, inodeID)
	require.NoError(t, err)
	dentryKey, err := fsmeta.EncodeDentryKey(testMount, fsmeta.RootInode, "file")
	require.NoError(t, err)

	op := testCompileAOT(t, delta)
	require.Equal(t, delta.Kind, op.Delta.Kind)
	require.True(t, op.Authority.Required)
	require.Equal(t, FenceActiveAuthority, op.Authority.Fence)
	require.True(t, op.Placement.CanSegment)
	require.False(t, op.Placement.RequiresMaterialize)
	require.Equal(t, testMount.MountKeyID, op.Placement.MountKeyID)
	require.Equal(t, DurabilityVisibleOnly, op.Durability)
	require.Equal(t, SegmentInstallCatalog, op.Placement.Install)
	require.Equal(t, SegmentInstallCatalog, op.Placement.MergeKey.Install)
	require.Equal(t, segmentFormatVersion, op.Placement.MergeKey.FormatVersion)
	require.Equal(t, []MutationID{0, 1}, op.Atomicity.Members)
	require.False(t, op.Atomicity.Splittable)
	require.Equal(t, RecoveryReplayAllOrNothing, op.Atomicity.Recovery)
	require.Equal(t, op.DescriptorDigest, op.Atomicity.Digest)

	require.Len(t, op.Predicates, 2)
	require.True(t, op.Predicates[0].NeedAbsent)
	require.False(t, op.Predicates[0].NeedValue)
	require.True(t, op.Predicates[1].NeedAbsent)
	require.Len(t, op.Effects, 2)
	require.Equal(t, MutationID(0), op.Effects[0].ID)
	require.Equal(t, DerivationNone, op.Effects[0].Derivation)
	require.True(t, op.Effects[0].Concrete)
	require.Equal(t, testMount.MountKeyID, op.Effects[0].MountKeyID)
	require.Equal(t, fsmeta.KeyKindDentry, op.Effects[0].RecordKind)
	require.Len(t, op.Watch, 1)
	require.Equal(t, WatchEventCreate, op.Watch[0].EventKind)
	require.Equal(t, WatchEmitVisible, op.Watch[0].EmitAt)
	require.Equal(t, dentryKey, op.Watch[0].Key)
	require.Equal(t, fsmeta.RootInode, op.Watch[0].Parent)
	require.Equal(t, "file", op.Watch[0].Name)
	require.Equal(t, inodeID, op.Watch[0].Inode)
	require.Len(t, op.Footprint.Reads, 2)
	require.Len(t, op.Footprint.Writes, 2)
	require.Len(t, op.Footprint.ConflictKeys, 4)
	require.False(t, op.Footprint.HasPrefixRead)
	require.False(t, op.Footprint.HasOpaqueKeys)
	require.True(t, op.Footprint.EstimatedBytes > 0)
	require.True(t, op.Completion.RetainCompletion)
	require.Equal(t, CompletionVisible, op.Completion.Kind)
	require.Equal(t, uint32(2), op.Completion.MutationCount)
	require.Equal(t, op.DescriptorDigest, op.Completion.DescriptorDigest)
	require.Equal(t, op.Placement.MergeKey, op.Segment.MergeKey)
	require.True(t, op.Segment.CanAppend)
	require.Equal(t, uint32(1), op.Segment.OperationCount)
	require.Equal(t, uint32(2), op.Segment.MutationCount)
}

func TestGeneratedCreateProgramMatchesCurrentCompiler(t *testing.T) {
	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 128, Mode: 0o644},
	}
	inodeID := testParentInDifferentBucket(t, fsmeta.RootInode)
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

			defaultMaterialized, err := MaterializeCreate(program, CreateValues{})
			require.NoError(t, err)
			require.Equal(t, testMaterializeAOT(t, delta, nil), defaultMaterialized)

			values := CreateValues{
				DentryValue: append([]byte(nil), program.Compiled.Delta.WriteEffects[0].Value...),
				InodeValue:  append([]byte(nil), program.Compiled.Delta.WriteEffects[1].Value...),
			}
			valueMaterialized, err := MaterializeCreate(program, values)
			require.NoError(t, err)
			require.Equal(t, testMaterializeAOT(t, delta, nil), valueMaterialized)
		})
	}
}

func TestGeneratedCreateMaterializerRejectsMalformedProgram(t *testing.T) {
	_, err := MaterializeCreate(CreateProgram{}, CreateValues{})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)

	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}
	program, err := CompileCreateProgram(req, testMount, 22)
	require.NoError(t, err)
	_, err = MaterializeCreate(program, CreateValues{DentryValue: program.Compiled.Delta.WriteEffects[0].Value})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
}

func TestSessionProgramsMatchCurrentCompiler(t *testing.T) {
	openReq := fsmeta.OpenWriteSessionRequest{
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

	heartbeatReq := fsmeta.HeartbeatWriteSessionRequest{
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

	closeReq := fsmeta.CloseWriteSessionRequest{
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

func TestGeneratedProgramWrappersMatchLowering(t *testing.T) {
	renameToParent := testParentInSameBucket(t, fsmeta.RootInode)
	crossBucketParent := testParentInDifferentBucket(t, fsmeta.RootInode)
	for _, tc := range []struct {
		name    string
		lower   func() (SemanticDelta, error)
		compile func() (CompiledOp, error)
	}{
		{
			name: "update_inode",
			lower: func() (SemanticDelta, error) {
				return lowerUpdateInode(fsmeta.UpdateInodeRequest{Mount: "vol", Parent: fsmeta.RootInode, Inode: 44, Name: "file", SetMode: true, Mode: 0o600}, testMount, WithQuotaMode(QuotaModeEscrow))
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileUpdateInodeProgram(fsmeta.UpdateInodeRequest{Mount: "vol", Parent: fsmeta.RootInode, Inode: 44, Name: "file", SetMode: true, Mode: 0o600}, testMount, WithQuotaMode(QuotaModeEscrow))
				return program.Compiled, err
			},
		},
		{
			name: "lookup",
			lower: func() (SemanticDelta, error) {
				return lowerLookup(fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "file"}, testMount)
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileLookupProgram(fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "file"}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "readdir",
			lower: func() (SemanticDelta, error) {
				return lowerReadDir(fsmeta.ReadDirRequest{Mount: "vol", Parent: fsmeta.RootInode, Limit: 32}, testMount)
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileReadDirProgram(fsmeta.ReadDirRequest{Mount: "vol", Parent: fsmeta.RootInode, Limit: 32}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "snapshot_subtree",
			lower: func() (SemanticDelta, error) {
				return lowerSnapshotSubtree(fsmeta.SnapshotSubtreeRequest{Mount: "vol", RootInode: fsmeta.RootInode}, testMount)
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileSnapshotSubtreeProgram(fsmeta.SnapshotSubtreeRequest{Mount: "vol", RootInode: fsmeta.RootInode}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "rename",
			lower: func() (SemanticDelta, error) {
				return lowerRename(fsmeta.RenameRequest{Mount: "vol", FromParent: fsmeta.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount)
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileRenameProgram(fsmeta.RenameRequest{Mount: "vol", FromParent: fsmeta.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "rename_subtree",
			lower: func() (SemanticDelta, error) {
				return lowerRenameSubtree(fsmeta.RenameSubtreeRequest{Mount: "vol", FromParent: fsmeta.RootInode, FromName: "old", ToParent: crossBucketParent, ToName: "new"}, testMount)
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileRenameSubtreeProgram(fsmeta.RenameSubtreeRequest{Mount: "vol", FromParent: fsmeta.RootInode, FromName: "old", ToParent: crossBucketParent, ToName: "new"}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "link",
			lower: func() (SemanticDelta, error) {
				return lowerLink(fsmeta.LinkRequest{Mount: "vol", FromParent: fsmeta.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount, WithQuotaMode(QuotaModeEscrow))
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileLinkProgram(fsmeta.LinkRequest{Mount: "vol", FromParent: fsmeta.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount, WithQuotaMode(QuotaModeEscrow))
				return program.Compiled, err
			},
		},
		{
			name: "unlink",
			lower: func() (SemanticDelta, error) {
				return lowerUnlink(fsmeta.UnlinkRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "old"}, testMount, WithQuotaMode(QuotaModeEscrow))
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileUnlinkProgram(fsmeta.UnlinkRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "old"}, testMount, WithQuotaMode(QuotaModeEscrow))
				return program.Compiled, err
			},
		},
		{
			name: "open_write_session",
			lower: func() (SemanticDelta, error) {
				return lowerOpenWriteSession(fsmeta.OpenWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}, testMount)
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileOpenWriteSessionProgram(fsmeta.OpenWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "heartbeat_write_session",
			lower: func() (SemanticDelta, error) {
				return lowerHeartbeatWriteSession(fsmeta.HeartbeatWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}, testMount)
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileHeartbeatWriteSessionProgram(fsmeta.HeartbeatWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "close_write_session",
			lower: func() (SemanticDelta, error) {
				return lowerCloseWriteSession(fsmeta.CloseWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1"}, testMount)
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileCloseWriteSessionProgram(fsmeta.CloseWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1"}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "expire_write_sessions",
			lower: func() (SemanticDelta, error) {
				return lowerExpireWriteSessions(fsmeta.ExpireWriteSessionsRequest{Mount: "vol", Limit: 16}, testMount)
			},
			compile: func() (CompiledOp, error) {
				program, err := CompileExpireWriteSessionsProgram(fsmeta.ExpireWriteSessionsRequest{Mount: "vol", Limit: 16}, testMount)
				return program.Compiled, err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delta, err := tc.lower()
			require.NoError(t, err)
			compiled, err := tc.compile()
			require.NoError(t, err)
			require.Equal(t, testCompileAOT(t, delta), compiled)
		})
	}
}

func TestSessionProgramsRejectInvalidTTL(t *testing.T) {
	_, err := CompileOpenWriteSessionProgram(fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
	}, testMount)
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)

	_, err = CompileHeartbeatWriteSessionProgram(fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
	}, testMount)
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
}

func TestOpenWriteSessionMaterializerMatchesGenericMaterialization(t *testing.T) {
	req := fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	program, err := CompileOpenWriteSessionProgram(req, testMount)
	require.NoError(t, err)
	sessionValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: 200})
	require.NoError(t, err)
	inodeValue, err := fsmeta.EncodeInodeValue(fsmeta.InodeRecord{Inode: req.Inode, Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)
	inodeKey := program.Compiled.Delta.ReadPredicates[0].Key
	sessionKey := program.Compiled.Delta.ReadPredicates[1].Key
	ownerKey := program.Compiled.Delta.ReadPredicates[2].Key
	proofs := []PredicateProof{
		testPredicateProof(inodeKey, inodeValue, true, 9, ReadSourceBase),
		testPredicateProof(sessionKey, nil, false, 9, ReadSourceBase),
	}

	materialized, err := MaterializeOpenWriteSession(program, OpenWriteSessionValues{
		SessionValue:    sessionValue,
		PredicateProofs: proofs,
		KnownAbsent:     [][]byte{ownerKey},
	})
	require.NoError(t, err)

	expectedDelta := cloneDelta(program.Compiled.Delta)
	expectedDelta.ReadPredicates = []Predicate{
		{Kind: PredicateObservedValue, Key: inodeKey, ExpectedValue: inodeValue, HasExpectedValue: true, RuntimeChecked: true},
		{Kind: PredicateNotExists, Key: sessionKey, RuntimeChecked: true},
		{Kind: PredicateNotExists, Key: ownerKey, RuntimeChecked: true},
	}
	expectedDelta.WriteEffects = []WriteEffect{
		{Kind: EffectPut, Key: sessionKey, Value: sessionValue},
		{Kind: EffectPut, Key: ownerKey, Value: sessionValue},
	}
	expected := testMaterializeAOT(t, expectedDelta, proofs)
	expected.IntentDigest = program.Compiled.IntentDigest
	require.Equal(t, expected, materialized)
}

func TestHeartbeatWriteSessionMaterializerMatchesGenericMaterialization(t *testing.T) {
	req := fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	program, err := CompileHeartbeatWriteSessionProgram(req, testMount)
	require.NoError(t, err)
	oldValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: 100})
	require.NoError(t, err)
	newValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: 200})
	require.NoError(t, err)
	sessionKey := program.Compiled.Delta.ReadPredicates[0].Key
	ownerKey := program.Compiled.Delta.ReadPredicates[1].Key
	proofs := []PredicateProof{
		testPredicateProof(sessionKey, oldValue, true, 11, ReadSourceBase),
		testPredicateProof(ownerKey, oldValue, true, 11, ReadSourceBase),
	}

	materialized, err := MaterializeHeartbeatWriteSession(program, HeartbeatWriteSessionValues{
		SessionValue:    newValue,
		PredicateProofs: proofs,
	})
	require.NoError(t, err)

	expectedDelta := cloneDelta(program.Compiled.Delta)
	expectedDelta.ReadPredicates = []Predicate{
		{Kind: PredicateObservedValue, Key: sessionKey, ExpectedValue: oldValue, HasExpectedValue: true, RuntimeChecked: true},
		{Kind: PredicateObservedValue, Key: ownerKey, ExpectedValue: oldValue, HasExpectedValue: true, RuntimeChecked: true},
	}
	expectedDelta.WriteEffects = []WriteEffect{
		{Kind: EffectPut, Key: sessionKey, Value: newValue},
		{Kind: EffectPut, Key: ownerKey, Value: newValue},
	}
	expected := testMaterializeAOT(t, expectedDelta, proofs)
	expected.IntentDigest = program.Compiled.IntentDigest
	require.Equal(t, expected, materialized)
}

func TestCloseWriteSessionMaterializerMatchesGenericMaterialization(t *testing.T) {
	req := fsmeta.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
	}
	program, err := CompileCloseWriteSessionProgram(req, testMount)
	require.NoError(t, err)
	sessionValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: 100})
	require.NoError(t, err)
	ownerValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: "other", Inode: req.Inode, ExpiresUnixNs: 100})
	require.NoError(t, err)
	sessionKey := program.Compiled.Delta.ReadPredicates[0].Key
	ownerKey := program.Compiled.Delta.ReadPredicates[1].Key
	proofs := []PredicateProof{
		testPredicateProof(sessionKey, sessionValue, true, 12, ReadSourceBase),
		testPredicateProof(ownerKey, ownerValue, true, 12, ReadSourceBase),
	}

	materialized, err := MaterializeCloseWriteSession(program, CloseWriteSessionValues{
		PredicateProofs: proofs,
	})
	require.NoError(t, err)

	expectedDelta := cloneDelta(program.Compiled.Delta)
	expectedDelta.ReadPredicates = []Predicate{
		{Kind: PredicateObservedValue, Key: sessionKey, ExpectedValue: sessionValue, HasExpectedValue: true, RuntimeChecked: true},
		{Kind: PredicateObservedValue, Key: ownerKey, ExpectedValue: ownerValue, HasExpectedValue: true, RuntimeChecked: true},
	}
	expectedDelta.WriteEffects = []WriteEffect{{Kind: EffectDelete, Key: sessionKey}}
	expected := testMaterializeAOT(t, expectedDelta, proofs)
	expected.IntentDigest = program.Compiled.IntentDigest
	require.Equal(t, expected, materialized)

	withOwnerDelete, err := MaterializeCloseWriteSession(program, CloseWriteSessionValues{
		DeleteOwner:     true,
		PredicateProofs: proofs,
	})
	require.NoError(t, err)
	expectedDelta.WriteEffects = []WriteEffect{
		{Kind: EffectDelete, Key: sessionKey},
		{Kind: EffectDelete, Key: ownerKey},
	}
	expected = testMaterializeAOT(t, expectedDelta, proofs)
	expected.IntentDigest = program.Compiled.IntentDigest
	require.Equal(t, expected, withOwnerDelete)
}

func TestSessionMaterializersRejectMalformedInput(t *testing.T) {
	openReq := fsmeta.OpenWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}
	openProgram, err := CompileOpenWriteSessionProgram(openReq, testMount)
	require.NoError(t, err)
	_, err = MaterializeOpenWriteSession(openProgram, OpenWriteSessionValues{})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)

	sessionValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: openReq.Session, Inode: openReq.Inode, ExpiresUnixNs: 100})
	require.NoError(t, err)
	badProof := PredicateProof{Key: openProgram.Compiled.Delta.ReadPredicates[0].Key, Present: true, Value: []byte("bad"), Version: 1, Source: ReadSourceBase}
	_, err = MaterializeOpenWriteSession(openProgram, OpenWriteSessionValues{
		SessionValue:    sessionValue,
		PredicateProofs: []PredicateProof{badProof},
	})
	require.Error(t, err)
}

func TestCreateRespectsQuotaMode(t *testing.T) {
	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 3,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
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

func BenchmarkCompileCreateProgram(b *testing.B) {
	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 128, Mode: 0o644},
	}
	inodeID := fsmeta.InodeID(22)
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
	req := fsmeta.OpenWriteSessionRequest{
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

func TestDerivedOperationRequiresRuntimeMaterialization(t *testing.T) {
	delta, err := testUpdateInodeDelta(t, fsmeta.UpdateInodeRequest{
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
	require.False(t, op.Placement.CanSegment)
	require.True(t, op.Placement.RequiresMaterialize)
	require.Equal(t, DurabilityVisibleOnly, op.Durability)
	require.Len(t, op.Predicates, 2)
	require.True(t, op.Predicates[0].NeedValue)
	require.True(t, op.Predicates[1].NeedValue)
	require.Len(t, op.Effects, 1)
	require.Equal(t, DerivationRuntimeValue, op.Effects[0].Derivation)
	require.False(t, op.Effects[0].Concrete)
}

func TestMaterializedOpRecompilesConcreteEffectsAndCarriesProofs(t *testing.T) {
	delta, err := testUpdateInodeDelta(t, fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  3,
		Inode:   44,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	}, testMount)
	require.NoError(t, err)
	compiled := testCompileAOT(t, delta)
	require.True(t, compiled.Placement.RequiresMaterialize)

	key := mustInodeKey(t, 44)
	value := []byte("new-inode")
	proof := PredicateProof{
		Key:     key,
		Present: true,
		Value:   []byte("old-inode"),
		Version: 9,
		Source:  ReadSourceBase,
	}
	proof.Digest = PredicateProofDigest(proof.Key, proof.Value, proof.Present, proof.Version, proof.Source)
	materialized := testMaterializeAOTWithEffects(t, compiled, []WriteEffect{{Kind: EffectPut, Key: key, Value: value}}, []PredicateProof{proof})

	require.True(t, materialized.Placement.CanSegment)
	require.False(t, materialized.Placement.RequiresMaterialize)
	require.Equal(t, compiled.IntentDigest, materialized.IntentDigest)
	require.NotEqual(t, compiled.DescriptorDigest, materialized.ReplayDigest)
	require.Equal(t, materialized.DescriptorDigest, materialized.ReplayDigest)
	require.Len(t, materialized.Effects, 1)
	require.True(t, materialized.Effects[0].Concrete)
	require.Equal(t, DerivationNone, materialized.Effects[0].Derivation)
	require.Equal(t, sha256.Sum256(value), materialized.Effects[0].ValueHash)
	require.Len(t, materialized.PredicateProofs, 1)
	require.Equal(t, proof.Digest, materialized.PredicateProofs[0].Digest)

	proof.Value[0] ^= 0xff
	value[0] ^= 0xff
	require.NotEqual(t, proof.Value, materialized.PredicateProofs[0].Value)
	require.NotEqual(t, value, materialized.Effects[0].Value)
}

func TestCompiledDigestSemanticsAreStableAcrossMaterialization(t *testing.T) {
	createDelta, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, 44)
	require.NoError(t, err)
	create := testCompileAOT(t, createDelta)
	require.Equal(t, create.DescriptorDigest, create.IntentDigest)
	require.Equal(t, create.DescriptorDigest, create.ReplayDigest)

	updateDelta, err := testUpdateInodeDelta(t, fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  3,
		Inode:   44,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	}, testMount)
	require.NoError(t, err)
	compiled := testCompileAOT(t, updateDelta)
	key := mustInodeKey(t, 44)
	proof := testPredicateProof(key, []byte("old-inode"), true, 9, ReadSourceBase)
	materialized := testMaterializeAOTWithEffects(t, compiled, []WriteEffect{{Kind: EffectPut, Key: key, Value: []byte("new-inode")}}, []PredicateProof{proof})
	require.Equal(t, compiled.IntentDigest, materialized.IntentDigest)
	require.NotEqual(t, compiled.DescriptorDigest, materialized.DescriptorDigest)
	require.Equal(t, materialized.DescriptorDigest, materialized.ReplayDigest)
}

func TestMaterializedOpValidationRejectsReplayDigestDrift(t *testing.T) {
	delta, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, 44)
	require.NoError(t, err)
	op := testMaterializeAOT(t, delta, nil)
	op.ReplayDigest[0] ^= 0xff

	var validationErr ValidationError
	err = op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationCanonicalMismatch, validationErr.Kind)
}

func TestObservedValuePredicateCompilesExactProofObligation(t *testing.T) {
	expected := []byte("old-inode")
	delta, _ := testConcreteUpdateInodeDelta(t, expected)

	op := testCompileAOT(t, delta)
	require.Len(t, op.Predicates, 2)
	require.True(t, op.Predicates[1].NeedValue)
	require.True(t, op.Predicates[1].HasExpectedValue)
	require.Equal(t, sha256.Sum256(expected), op.Predicates[1].ExpectHash)
}

func TestMaterializedOpValidationRejectsUncoveredWrite(t *testing.T) {
	delta, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, 44)
	require.NoError(t, err)
	delta.Authority = AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{fsmeta.BucketForInodeID(55)},
		Inodes:     []fsmeta.InodeID{55},
	}

	var validationErr ValidationError
	op := testMaterializeAOT(t, delta, nil)
	err = op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationAuthorityMismatch, validationErr.Kind)
}

func TestMaterializedOpValidationRejectsNonCanonicalDescriptor(t *testing.T) {
	delta, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, 44)
	require.NoError(t, err)
	op := testMaterializeAOT(t, delta, nil)
	op.Placement.CanSegment = false

	var validationErr ValidationError
	err = op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationCanonicalMismatch, validationErr.Kind)
}

func TestMaterializedOpValidationRequiresObservedValueProof(t *testing.T) {
	expected := []byte("old-inode")
	delta, proofs := testConcreteUpdateInodeDelta(t, expected)

	var validationErr ValidationError
	err := testMaterializeAOT(t, delta, nil).ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMissing, validationErr.Kind)

	op := WithGuardProofs(testMaterializeAOT(t, delta, proofs), GuardProofsFor(delta.RuntimeGuards))
	require.NoError(t, op.ValidateForAdmission())
}

func TestMaterializedOpValidationRejectsBadPredicateProofContract(t *testing.T) {
	delta, proofs := testConcreteUpdateInodeDelta(t, []byte("old-inode"))
	badProof := PredicateProof{
		Key:     proofs[1].Key,
		Present: proofs[1].Present,
		Value:   proofs[1].Value,
		Source:  ReadSourceUnknown,
	}
	badProof.Digest = PredicateProofDigest(badProof.Key, badProof.Value, badProof.Present, badProof.Version, badProof.Source)

	var validationErr ValidationError
	err := testMaterializeAOT(t, delta, []PredicateProof{proofs[0], badProof}).ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMismatch, validationErr.Kind)

	duplicateProof := proofs[1]
	err = testMaterializeAOT(t, delta, []PredicateProof{proofs[0], duplicateProof, duplicateProof}).ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMismatch, validationErr.Kind)
}

func TestMaterializedOpValidationRequiresGuardProof(t *testing.T) {
	delta, proofs := testConcreteUpdateInodeDelta(t, []byte("old-inode"))

	var validationErr ValidationError
	err := testMaterializeAOT(t, delta, proofs).ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationGuardProofMissing, validationErr.Kind)

	op := WithGuardProofs(testMaterializeAOT(t, delta, proofs), GuardProofsFor(delta.RuntimeGuards))
	require.NoError(t, op.ValidateForAdmission())
}

func TestSegmentMergeDecisionUsesCompilerPlans(t *testing.T) {
	left, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 8,
		Name:   "a",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, testParentInSameBucket(t, 8))
	require.NoError(t, err)
	right, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 8,
		Name:   "b",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, testParentInDifferentBucketAfter(t, 8, 128))
	require.NoError(t, err)

	decision := CanAppendSegment(testCompileAOT(t, left), testCompileAOT(t, right), SegmentBudget{
		MaxOperations:   2,
		MaxMutations:    4,
		MaxPayloadBytes: 1 << 20,
	})
	require.Equal(t, SegmentDecisionAppend, decision.Kind)

	decision = CanAppendSegment(testCompileAOT(t, left), testCompileAOT(t, right), SegmentBudget{MaxMutations: 3})
	require.Equal(t, SegmentDecisionCut, decision.Kind)

	snapshot, err := testSnapshotSubtreeDelta(t, fsmeta.SnapshotSubtreeRequest{Mount: "vol", RootInode: 8}, testMount)
	require.NoError(t, err)
	decision = CanAppendSegment(testCompileAOT(t, left), testCompileAOT(t, snapshot), SegmentBudget{})
	require.Equal(t, SegmentDecisionFlushBeforeAndAfter, decision.Kind)
	require.Equal(t, SlowReasonDurabilityBarrier, decision.Reason)
}

func TestSegmentPlanAPIPreservesCompilerBoundary(t *testing.T) {
	left, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 8,
		Name:   "a",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, testParentInSameBucket(t, 8))
	require.NoError(t, err)
	right, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 8,
		Name:   "b",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, testParentInDifferentBucketAfter(t, 8, 128))
	require.NoError(t, err)

	leftPlan := testCompileAOT(t, left).Segment
	rightPlan := testCompileAOT(t, right).Segment
	decision := CanAppendSegmentPlans(leftPlan, rightPlan, DurabilityVisibleOnly, SegmentBudget{MaxMutations: 4})
	require.Equal(t, SegmentDecisionAppend, decision.Kind)

	merged := MergeSegmentPlans(leftPlan, rightPlan)
	require.Equal(t, uint32(2), merged.OperationCount)
	require.Equal(t, uint32(4), merged.MutationCount)
	require.Greater(t, merged.EstimatedPayloadBytes, leftPlan.EstimatedPayloadBytes)

	installPlan, ok := SegmentPlanForInstall(leftPlan, false)
	require.True(t, ok)
	require.Equal(t, SegmentInstallCatalog, installPlan.Install)

	materializePlan, ok := SegmentPlanForInstall(leftPlan, true)
	require.True(t, ok)
	require.Equal(t, SegmentInstallSingleBucket, materializePlan.Install)
	require.NotZero(t, materializePlan.MergeKey.PrimaryBucket)
}

func TestRenameBucketLocalVisibleCrossBucketSlow(t *testing.T) {
	sameParent, err := testRenameDelta(t, fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, sameParent.Eligibility)
	require.Equal(t, []fsmeta.InodeID{8}, sameParent.Authority.Parents)
	require.Equal(t, []WriteEffect{
		{Kind: EffectDelete, Key: sameParent.Plan.MutateKeys[0]},
		{Kind: EffectDerivedPut, Key: sameParent.Plan.MutateKeys[1]},
	}, sameParent.WriteEffects)

	sameBucketParent := testParentInSameBucket(t, 8)
	crossParentSameBucket, err := testRenameDelta(t, fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   "old",
		ToParent:   sameBucketParent,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, crossParentSameBucket.Eligibility)
	require.Equal(t, SlowReasonNone, crossParentSameBucket.SlowReason)
	require.Equal(t, []fsmeta.InodeID{8, sameBucketParent}, crossParentSameBucket.Authority.Parents)
	require.Len(t, crossParentSameBucket.Authority.Buckets, 1)

	differentBucketParent := testParentInDifferentBucket(t, 8)
	crossBucket, err := testRenameDelta(t, fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   "old",
		ToParent:   differentBucketParent,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, crossBucket.Eligibility)
	require.Equal(t, SlowReasonCrossBucket, crossBucket.SlowReason)
	require.Equal(t, []fsmeta.InodeID{8, differentBucketParent}, crossBucket.Authority.Parents)
	require.Len(t, crossBucket.Authority.Buckets, 2)
}

func TestSlowPathBoundariesStayExplicit(t *testing.T) {
	snapshot, err := testSnapshotSubtreeDelta(t, fsmeta.SnapshotSubtreeRequest{
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
	require.False(t, compiledSnapshot.Placement.CanSegment)

	expire, err := testExpireWriteSessionsDelta(t, fsmeta.ExpireWriteSessionsRequest{
		Mount: "vol",
		Limit: 8,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, expire.Eligibility)
	require.Equal(t, SlowReasonMaintenanceScan, expire.SlowReason)
	require.Len(t, expire.ReadPredicates, fsmeta.DefaultAffinityBucketCount)
}

func TestLinkAndUnlinkCompileRuntimeConcreteVisibleCommitDeltas(t *testing.T) {
	link, err := testLinkDelta(t, fsmeta.LinkRequest{
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
	require.Len(t, link.WriteEffects, 2)
	require.Equal(t, EffectDerivedPut, link.WriteEffects[0].Kind)
	require.Equal(t, EffectDerivedPut, link.WriteEffects[1].Kind)

	delta, err := testUnlinkDelta(t, fsmeta.UnlinkRequest{
		Mount:  "vol",
		Parent: 5,
		Name:   "file",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, delta.Eligibility)
	require.Empty(t, delta.SlowReason)
	require.Contains(t, delta.RuntimeGuards, GuardNonDirectoryInode)
	require.Equal(t, EffectDelete, delta.WriteEffects[0].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[1].Kind)
}

func TestSessionOperationsCompileVisibleCommitDeltas(t *testing.T) {
	open, err := testOpenWriteSessionDelta(t, fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   33,
		Session: "writer-1",
		TTL:     time.Minute,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, open.Eligibility)
	require.Contains(t, open.RuntimeGuards, GuardExpiredSessionOwner)

	heartbeat, err := testHeartbeatWriteSessionDelta(t, fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   33,
		Session: "writer-1",
		TTL:     time.Minute,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, heartbeat.Eligibility)
	require.Contains(t, heartbeat.RuntimeGuards, GuardLiveSession)

	closeDelta, err := testCloseWriteSessionDelta(t, fsmeta.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   33,
		Session: "writer-1",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, closeDelta.Eligibility)
	require.Equal(t, DurabilityNeedsCloseSession, testCompileAOT(t, closeDelta).Durability)
	require.Contains(t, closeDelta.RuntimeGuards, GuardLiveSession)
	require.Len(t, closeDelta.ReadPredicates, 2)
	require.Len(t, closeDelta.WriteEffects, 2)
}

func TestDeltasCloneReturnedBytes(t *testing.T) {
	delta, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, 22)
	require.NoError(t, err)
	primary := append([]byte(nil), delta.Plan.PrimaryKey...)
	predicate := append([]byte(nil), delta.ReadPredicates[0].Key...)
	value := append([]byte(nil), delta.WriteEffects[0].Value...)

	delta.Plan.PrimaryKey[0] ^= 0xff
	delta.ReadPredicates[0].Key[0] ^= 0xff
	delta.WriteEffects[0].Value[0] ^= 0xff

	require.Equal(t, primary, delta.Plan.MutateKeys[0])
	require.NotEqual(t, delta.Plan.PrimaryKey, delta.Plan.MutateKeys[0])
	require.Equal(t, predicate, delta.Plan.MutateKeys[0])
	require.NotEqual(t, predicate, delta.ReadPredicates[0].Key)
	require.NotEqual(t, value, delta.WriteEffects[0].Value)
}
