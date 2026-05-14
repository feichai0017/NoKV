package compile

import (
	"crypto/sha256"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestMaterializedOpValidationRequiresAbsentProof(t *testing.T) {
	program, err := CompileCreateProgram(fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, 44)
	require.NoError(t, err)
	op, err := MaterializeCreate(program, CreateValues{})
	require.NoError(t, err)
	require.NoError(t, op.ValidateForAdmissionIntent())

	var validationErr ValidationError
	err = op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMissing, validationErr.Kind)

	proofs := []PredicateProof{
		PredicateProofFor(op.Delta.ReadPredicates[0].Key, nil, false, 0, ReadSourceOverlay, ProofFrontier{EpochID: 1, Sequence: 1}),
		PredicateProofFor(op.Delta.ReadPredicates[1].Key, nil, false, 0, ReadSourceOverlay, ProofFrontier{EpochID: 1, Sequence: 1}),
	}
	op = WithPredicateProofs(op, proofs)
	require.NoError(t, op.ValidateForAdmission())
}

func TestMaterializedOpValidationBindsGuardProofEvidence(t *testing.T) {
	delta, proofs := testConcreteUpdateInodeDelta(t, nil)
	op := testMaterializeAOT(t, delta, proofs)
	wrongEvidence := GuardEvidenceFor(op.CompiledOp, nil)
	op = WithGuardProofs(op, []GuardProof{GuardProofFor(op.Delta.RuntimeGuards[0], true, wrongEvidence)})

	var validationErr ValidationError
	err := op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationGuardProofMismatch, validationErr.Kind)

	op = testMaterializeAOT(t, delta, proofs)
	op = WithGuardProofs(op, testGuardProofsFor(op))
	require.NoError(t, op.ValidateForAdmission())
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
	expected, err := fsmeta.EncodeInodeValue(fsmeta.InodeRecord{Inode: 44, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	require.NoError(t, err)
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
	delta, proofs := testConcreteUpdateInodeDelta(t, nil)

	var validationErr ValidationError
	err := testMaterializeAOT(t, delta, nil).ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMissing, validationErr.Kind)

	op := testMaterializeAOT(t, delta, proofs)
	op = WithGuardProofs(op, testGuardProofsFor(op))
	require.NoError(t, op.ValidateForAdmission())
}

func TestMaterializedOpValidationRejectsBadPredicateProofContract(t *testing.T) {
	delta, proofs := testConcreteUpdateInodeDelta(t, nil)
	badProof := PredicateProof{
		Key:     proofs[1].Key,
		Present: proofs[1].Present,
		Value:   proofs[1].Value,
		Source:  ReadSourceUnknown,
	}
	badProof.Digest = PredicateProofDigest(badProof.Key, badProof.Value, badProof.Present, badProof.Version, badProof.Source, badProof.ProofFrontier)

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
	delta, proofs := testConcreteUpdateInodeDelta(t, nil)

	var validationErr ValidationError
	err := testMaterializeAOT(t, delta, proofs).ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationGuardProofMissing, validationErr.Kind)

	op := testMaterializeAOT(t, delta, proofs)
	op = WithGuardProofs(op, testGuardProofsFor(op))
	require.NoError(t, op.ValidateForAdmission())
}
