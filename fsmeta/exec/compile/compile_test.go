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

func TestCreateCompilesVisibleCommitDelta(t *testing.T) {
	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 128, Mode: 0o644},
	}
	delta, err := Create(req, testMount, 22)
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
	delta, err := Create(fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, inodeID)
	require.NoError(t, err)
	dentryKey, err := fsmeta.EncodeDentryKey(testMount, fsmeta.RootInode, "file")
	require.NoError(t, err)

	op := CompileDelta(delta)
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

func TestCreateRespectsQuotaMode(t *testing.T) {
	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 3,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}

	shared, err := Create(req, testMount, 44, WithQuotaMode(QuotaModeShared))
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, shared.Eligibility)
	require.Equal(t, SlowReasonSharedQuota, shared.SlowReason)

	escrow, err := Create(req, testMount, 44, WithQuotaMode(QuotaModeEscrow))
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, escrow.Eligibility)
	require.Contains(t, escrow.RuntimeGuards, GuardQuotaCredit)
}

func TestDerivedOperationRequiresRuntimeMaterialization(t *testing.T) {
	delta, err := UpdateInode(fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  3,
		Inode:   44,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	}, testMount)
	require.NoError(t, err)

	op := CompileDelta(delta)
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
	delta, err := UpdateInode(fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  3,
		Inode:   44,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	}, testMount)
	require.NoError(t, err)
	compiled := CompileDelta(delta)
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
	materialized := MaterializeCompiledOp(compiled, []WriteEffect{{Kind: EffectPut, Key: key, Value: value}}, []PredicateProof{proof})

	require.True(t, materialized.Placement.CanSegment)
	require.False(t, materialized.Placement.RequiresMaterialize)
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

func TestObservedValuePredicateCompilesExactProofObligation(t *testing.T) {
	expected := []byte("old-inode")
	delta := SemanticDelta{
		Kind:        fsmeta.OperationUpdateInode,
		Eligibility: EligibilityVisibleCommit,
		Authority: AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{fsmeta.BucketForInodeID(44)},
		},
		ReadPredicates: []Predicate{{
			Kind:             PredicateObservedValue,
			Key:              mustInodeKey(t, 44),
			ExpectedValue:    expected,
			HasExpectedValue: true,
		}},
		WriteEffects: []WriteEffect{{
			Kind:  EffectPut,
			Key:   mustInodeKey(t, 44),
			Value: []byte("new-inode"),
		}},
	}

	op := CompileDelta(delta)
	require.Len(t, op.Predicates, 1)
	require.True(t, op.Predicates[0].NeedValue)
	require.True(t, op.Predicates[0].HasExpectedValue)
	require.Equal(t, sha256.Sum256(expected), op.Predicates[0].ExpectHash)
}

func TestSegmentMergeDecisionUsesCompilerPlans(t *testing.T) {
	left, err := Create(fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 8,
		Name:   "a",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, testParentInDifferentBucket(t, 8))
	require.NoError(t, err)
	right, err := Create(fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 8,
		Name:   "b",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, testParentInDifferentBucketAfter(t, 8, 128))
	require.NoError(t, err)

	decision := CanAppendSegment(CompileDelta(left), CompileDelta(right), SegmentBudget{
		MaxOperations:   2,
		MaxMutations:    4,
		MaxPayloadBytes: 1 << 20,
	})
	require.Equal(t, SegmentDecisionAppend, decision.Kind)

	decision = CanAppendSegment(CompileDelta(left), CompileDelta(right), SegmentBudget{MaxMutations: 3})
	require.Equal(t, SegmentDecisionCut, decision.Kind)

	snapshot, err := SnapshotSubtree(fsmeta.SnapshotSubtreeRequest{Mount: "vol", RootInode: 8}, testMount)
	require.NoError(t, err)
	decision = CanAppendSegment(CompileDelta(left), CompileDelta(snapshot), SegmentBudget{})
	require.Equal(t, SegmentDecisionFlushBeforeAndAfter, decision.Kind)
	require.Equal(t, SlowReasonDurabilityBarrier, decision.Reason)
}

func TestRenameBucketLocalVisibleCrossBucketSlow(t *testing.T) {
	sameParent, err := Rename(fsmeta.RenameRequest{
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
	crossParentSameBucket, err := Rename(fsmeta.RenameRequest{
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
	crossBucket, err := Rename(fsmeta.RenameRequest{
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
	snapshot, err := SnapshotSubtree(fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 11,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, snapshot.Eligibility)
	require.Equal(t, SlowReasonDurabilityBarrier, snapshot.SlowReason)
	require.True(t, snapshot.DurabilityBarrier)
	compiledSnapshot := CompileDelta(snapshot)
	require.False(t, compiledSnapshot.Authority.Required)
	require.Equal(t, FenceNone, compiledSnapshot.Authority.Fence)
	require.Equal(t, DurabilityNeedsPublishCheckpoint, compiledSnapshot.Durability)
	require.False(t, compiledSnapshot.Placement.CanSegment)

	expire, err := ExpireWriteSessions(fsmeta.ExpireWriteSessionsRequest{
		Mount: "vol",
		Limit: 8,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, expire.Eligibility)
	require.Equal(t, SlowReasonMaintenanceScan, expire.SlowReason)
	require.Len(t, expire.ReadPredicates, fsmeta.DefaultAffinityBucketCount)
}

func TestLinkAndUnlinkCompileRuntimeConcreteVisibleCommitDeltas(t *testing.T) {
	link, err := Link(fsmeta.LinkRequest{
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

	delta, err := Unlink(fsmeta.UnlinkRequest{
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
	open, err := OpenWriteSession(fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   33,
		Session: "writer-1",
		TTL:     time.Minute,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, open.Eligibility)
	require.Contains(t, open.RuntimeGuards, GuardExpiredSessionOwner)

	heartbeat, err := HeartbeatWriteSession(fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   33,
		Session: "writer-1",
		TTL:     time.Minute,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, heartbeat.Eligibility)
	require.Contains(t, heartbeat.RuntimeGuards, GuardLiveSession)

	closeDelta, err := CloseWriteSession(fsmeta.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   33,
		Session: "writer-1",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, closeDelta.Eligibility)
	require.Contains(t, closeDelta.RuntimeGuards, GuardLiveSession)
	require.Len(t, closeDelta.ReadPredicates, 2)
	require.Len(t, closeDelta.WriteEffects, 2)
}

func TestDeltasCloneReturnedBytes(t *testing.T) {
	delta, err := Create(fsmeta.CreateRequest{
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
