package compile

import (
	"slices"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

var testMount = fsmeta.MountIdentity{MountID: "vol", MountKeyID: 7}

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

func TestRenameSameParentFastCrossParentSlow(t *testing.T) {
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

	crossParent, err := Rename(fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   "old",
		ToParent:   9,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, crossParent.Eligibility)
	require.Empty(t, crossParent.SlowReason)
	require.Equal(t, []fsmeta.InodeID{8, 9}, crossParent.Authority.Parents)
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
	require.Contains(t, delta.RuntimeGuards, GuardNotLastReference)
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
