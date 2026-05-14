package compile

import (
	"bytes"
	"crypto/sha256"
	"slices"

	"github.com/feichai0017/NoKV/fsmeta"
)

type ValidationErrorKind uint8

const (
	ValidationOK ValidationErrorKind = iota
	ValidationMissingConcreteEffects
	ValidationAuthorityMismatch
	ValidationPredicateProofMissing
	ValidationPredicateProofMismatch
	ValidationGuardProofMissing
	ValidationGuardProofMismatch
	ValidationPlacementMismatch
	ValidationCanonicalMismatch
)

func (k ValidationErrorKind) String() string {
	switch k {
	case ValidationMissingConcreteEffects:
		return "missing concrete effects"
	case ValidationAuthorityMismatch:
		return "authority mismatch"
	case ValidationPredicateProofMissing:
		return "predicate proof missing"
	case ValidationPredicateProofMismatch:
		return "predicate proof mismatch"
	case ValidationGuardProofMissing:
		return "guard proof missing"
	case ValidationGuardProofMismatch:
		return "guard proof mismatch"
	case ValidationPlacementMismatch:
		return "placement mismatch"
	case ValidationCanonicalMismatch:
		return "canonical mismatch"
	default:
		return "ok"
	}
}

type ValidationError struct {
	Kind ValidationErrorKind
	Key  []byte
}

func (e ValidationError) Error() string {
	return "invalid materialized metadata operation: " + e.Kind.String()
}

func (op MaterializedOp) ValidateForAdmissionIntent() error {
	return op.validateForAdmission(false)
}

func (op MaterializedOp) ValidateForAdmission() error {
	return op.validateForAdmission(true)
}

func (op MaterializedOp) validateForAdmission(requireGuardProofs bool) error {
	if !materializedOpIsCanonical(op) {
		return ValidationError{Kind: ValidationCanonicalMismatch}
	}
	if op.Delta.Eligibility != EligibilityVisibleCommit ||
		!op.Placement.CanSegment ||
		op.Placement.RequiresMaterialize ||
		op.Footprint.HasPrefixRead {
		return ValidationError{Kind: ValidationPlacementMismatch}
	}
	if len(op.Effects) == 0 {
		return ValidationError{Kind: ValidationMissingConcreteEffects}
	}
	for _, effect := range op.Effects {
		if !effect.Concrete || len(effect.Key) == 0 {
			return ValidationError{Kind: ValidationMissingConcreteEffects, Key: effect.Key}
		}
		switch effect.Kind {
		case EffectPut:
			if effect.Value == nil {
				return ValidationError{Kind: ValidationMissingConcreteEffects, Key: effect.Key}
			}
		case EffectDelete:
		default:
			return ValidationError{Kind: ValidationMissingConcreteEffects, Key: effect.Key}
		}
		if !authorityScopeCoversKey(op.Authority.Scope, keyRef(KeyAccessWrite, effect.Key)) {
			return ValidationError{Kind: ValidationAuthorityMismatch, Key: effect.Key}
		}
	}
	for _, ref := range op.Footprint.Reads {
		if !authorityScopeCoversKey(op.Authority.Scope, ref) {
			return ValidationError{Kind: ValidationAuthorityMismatch, Key: ref.Key}
		}
	}
	if !authorityScopeCoversBuckets(op.Authority.Scope, op.Placement.Buckets) {
		return ValidationError{Kind: ValidationPlacementMismatch}
	}
	proofs, err := predicateProofMap(op.PredicateProofs)
	if err != nil {
		return err
	}
	for _, obligation := range op.Predicates {
		requireProof := obligation.Kind == PredicateObservedValue
		if requireGuardProofs && (obligation.NeedValue || obligation.NeedAbsent) {
			requireProof = true
		}
		if !requireProof {
			continue
		}
		proof, ok := proofs[string(obligation.Key)]
		if !ok {
			return ValidationError{Kind: ValidationPredicateProofMissing, Key: obligation.Key}
		}
		if !predicateProofMatches(obligation, proof) {
			return ValidationError{Kind: ValidationPredicateProofMismatch, Key: obligation.Key}
		}
		if requireGuardProofs && obligation.NeedAbsent && !durableAbsenceProof(proof) {
			return ValidationError{Kind: ValidationPredicateProofMismatch, Key: obligation.Key}
		}
	}
	guardProofs, err := guardProofMap(op.GuardProofs)
	if err != nil {
		return err
	}
	for _, obligation := range op.Guards {
		proof, ok := guardProofs[obligation.Guard]
		if !ok {
			if !requireGuardProofs {
				continue
			}
			return ValidationError{Kind: ValidationGuardProofMissing}
		}
		guardEvidence, err := GuardEvidenceForGuard(op.CompiledOp, op.PredicateProofs, obligation.Guard)
		if err != nil {
			return ValidationError{Kind: ValidationGuardProofMismatch}
		}
		if proof.Evidence != guardEvidence || proof.Digest != GuardProofDigest(proof.Guard, proof.Passed, proof.Evidence) {
			return ValidationError{Kind: ValidationGuardProofMismatch}
		}
	}
	return nil
}

func materializedOpIsCanonical(op MaterializedOp) bool {
	canonical, err := compileAOTDelta(op.Delta)
	if err != nil {
		return false
	}
	if op.DescriptorDigest != canonical.DescriptorDigest ||
		op.ReplayDigest != canonical.DescriptorDigest {
		return false
	}
	return authorityPlanEqual(op.Authority, canonical.Authority) &&
		placementPlanEqual(op.Placement, canonical.Placement) &&
		keyFootprintEqual(op.Footprint, canonical.Footprint) &&
		predicateObligationsEqual(op.Predicates, canonical.Predicates) &&
		guardObligationsEqual(op.Guards, canonical.Guards) &&
		effectPlansEqual(op.Effects, canonical.Effects) &&
		atomicityGroupEqual(op.Atomicity, canonical.Atomicity) &&
		op.Durability == canonical.Durability &&
		watchProjectionsEqual(op.Watch, canonical.Watch) &&
		op.Completion == canonical.Completion &&
		op.Segment == canonical.Segment
}

func authorityPlanEqual(left, right AuthorityPlan) bool {
	return left.Required == right.Required &&
		left.Fence == right.Fence &&
		authorityScopeEqual(left.Scope, right.Scope)
}

func authorityScopeEqual(left, right AuthorityScope) bool {
	return left.Mount == right.Mount &&
		left.MountKeyID == right.MountKeyID &&
		left.Broad == right.Broad &&
		left.AllowOpaqueKeys == right.AllowOpaqueKeys &&
		slices.Equal(left.Buckets, right.Buckets) &&
		slices.Equal(left.Parents, right.Parents) &&
		slices.Equal(left.Inodes, right.Inodes)
}

func placementPlanEqual(left, right PlacementPlan) bool {
	return left.MountKeyID == right.MountKeyID &&
		left.SingleBucket == right.SingleBucket &&
		left.Install == right.Install &&
		left.CanSegment == right.CanSegment &&
		left.RequiresMaterialize == right.RequiresMaterialize &&
		left.SlowReason == right.SlowReason &&
		left.MergeKey == right.MergeKey &&
		slices.Equal(left.Buckets, right.Buckets)
}

func keyFootprintEqual(left, right KeyFootprint) bool {
	return left.HasPrefixRead == right.HasPrefixRead &&
		left.HasOpaqueKeys == right.HasOpaqueKeys &&
		left.EstimatedBytes == right.EstimatedBytes &&
		keyRefsEqual(left.Reads, right.Reads) &&
		keyRefsEqual(left.Writes, right.Writes) &&
		keyRefsEqual(left.ConflictKeys, right.ConflictKeys)
}

func keyRefsEqual(left, right []KeyRef) bool {
	return slices.EqualFunc(left, right, func(a, b KeyRef) bool {
		return a.Mode == b.Mode &&
			a.Opaque == b.Opaque &&
			a.MountKeyID == b.MountKeyID &&
			a.Bucket == b.Bucket &&
			a.Kind == b.Kind &&
			a.Parent == b.Parent &&
			a.Inode == b.Inode &&
			bytes.Equal(a.Key, b.Key)
	})
}

func predicateObligationsEqual(left, right []PredicateObligation) bool {
	return slices.EqualFunc(left, right, func(a, b PredicateObligation) bool {
		return a.Kind == b.Kind &&
			a.NeedValue == b.NeedValue &&
			a.NeedAbsent == b.NeedAbsent &&
			a.Guard == b.Guard &&
			a.HasExpectedValue == b.HasExpectedValue &&
			a.ExpectHash == b.ExpectHash &&
			bytes.Equal(a.Key, b.Key)
	})
}

func guardObligationsEqual(left, right []GuardObligation) bool {
	return slices.Equal(left, right)
}

func effectPlansEqual(left, right []EffectPlan) bool {
	return slices.EqualFunc(left, right, func(a, b EffectPlan) bool {
		return a.ID == b.ID &&
			a.Kind == b.Kind &&
			a.Concrete == b.Concrete &&
			a.Opaque == b.Opaque &&
			a.MountKeyID == b.MountKeyID &&
			a.Bucket == b.Bucket &&
			a.RecordKind == b.RecordKind &&
			a.ValueHash == b.ValueHash &&
			a.Derivation == b.Derivation &&
			bytes.Equal(a.Key, b.Key) &&
			bytes.Equal(a.Value, b.Value)
	})
}

func atomicityGroupEqual(left, right AtomicityGroup) bool {
	return left.Splittable == right.Splittable &&
		left.Recovery == right.Recovery &&
		left.Digest == right.Digest &&
		slices.Equal(left.Members, right.Members)
}

func watchProjectionsEqual(left, right []WatchProjection) bool {
	return slices.EqualFunc(left, right, func(a, b WatchProjection) bool {
		return a.EventKind == b.EventKind &&
			a.Parent == b.Parent &&
			a.Name == b.Name &&
			a.Inode == b.Inode &&
			a.EmitAt == b.EmitAt &&
			bytes.Equal(a.Key, b.Key)
	})
}

func predicateProofMap(proofs []PredicateProof) (map[string]PredicateProof, error) {
	out := make(map[string]PredicateProof, len(proofs))
	for _, proof := range proofs {
		if len(proof.Key) == 0 {
			return nil, ValidationError{Kind: ValidationPredicateProofMismatch}
		}
		if !proof.Present && len(proof.Value) != 0 {
			return nil, ValidationError{Kind: ValidationPredicateProofMismatch, Key: proof.Key}
		}
		if !predicateProofSourceValid(proof) {
			return nil, ValidationError{Kind: ValidationPredicateProofMismatch, Key: proof.Key}
		}
		digest := PredicateProofDigest(proof.Key, proof.Value, proof.Present, proof.Version, proof.Source, proof.ProofFrontier)
		if digest != proof.Digest {
			return nil, ValidationError{Kind: ValidationPredicateProofMismatch, Key: proof.Key}
		}
		if _, ok := out[string(proof.Key)]; ok {
			return nil, ValidationError{Kind: ValidationPredicateProofMismatch, Key: proof.Key}
		}
		out[string(proof.Key)] = proof
	}
	return out, nil
}

func durableAbsenceProof(proof PredicateProof) bool {
	if proof.Present {
		return true
	}
	switch proof.Source {
	case ReadSourceBase, ReadSourceSegment:
		return proof.Version != 0
	case ReadSourceOverlay:
		return proof.ProofFrontier.Valid()
	default:
		return false
	}
}

func predicateProofSourceValid(proof PredicateProof) bool {
	switch proof.Source {
	case ReadSourceUnknown:
		return !proof.Present && len(proof.Value) == 0 && proof.Version == 0 && proof.ProofFrontier == (ProofFrontier{})
	case ReadSourceOverlay:
		return proof.Version == 0
	case ReadSourceBase, ReadSourceSegment:
		return proof.Version != 0
	default:
		return false
	}
}

func predicateProofMatches(obligation PredicateObligation, proof PredicateProof) bool {
	if !bytes.Equal(obligation.Key, proof.Key) {
		return false
	}
	if obligation.NeedValue && !proof.Present {
		return false
	}
	if obligation.NeedAbsent && proof.Present {
		return false
	}
	if obligation.HasExpectedValue {
		if !proof.Present {
			return false
		}
		if sha256.Sum256(proof.Value) != obligation.ExpectHash {
			return false
		}
	}
	return true
}

func authorityScopeCoversKey(scope AuthorityScope, ref KeyRef) bool {
	if len(ref.Key) == 0 || ref.Mode == KeyAccessReadPrefix {
		return false
	}
	if ref.Opaque {
		return scope.AllowOpaqueKeys
	}
	if scope.MountKeyID == 0 || ref.MountKeyID != scope.MountKeyID {
		return false
	}
	if !slices.Contains(scope.Buckets, ref.Bucket) {
		return false
	}
	switch ref.Kind {
	case fsmeta.KeyKindMount:
		return true
	case fsmeta.KeyKindDentry:
		return scope.Broad || slices.Contains(scope.Parents, ref.Parent)
	case fsmeta.KeyKindInode, fsmeta.KeyKindChunk, fsmeta.KeyKindSession:
		return scope.Broad || slices.Contains(scope.Inodes, ref.Inode)
	case fsmeta.KeyKindUsage:
		return true
	default:
		return false
	}
}

func authorityScopeCoversBuckets(scope AuthorityScope, buckets []fsmeta.AffinityBucket) bool {
	if len(buckets) == 0 {
		return true
	}
	if scope.MountKeyID == 0 || len(scope.Buckets) == 0 {
		return scope.Broad
	}
	for _, bucket := range buckets {
		if !slices.Contains(scope.Buckets, bucket) {
			return false
		}
	}
	return true
}

func guardProofMap(proofs []GuardProof) (map[RuntimeGuard]GuardProof, error) {
	out := make(map[RuntimeGuard]GuardProof, len(proofs))
	for _, proof := range proofs {
		if proof.Guard == "" || !proof.Passed {
			return nil, ValidationError{Kind: ValidationGuardProofMismatch}
		}
		if proof.Evidence.Guard != proof.Guard {
			return nil, ValidationError{Kind: ValidationGuardProofMismatch}
		}
		if proof.Digest != GuardProofDigest(proof.Guard, proof.Passed, proof.Evidence) {
			return nil, ValidationError{Kind: ValidationGuardProofMismatch}
		}
		if _, ok := out[proof.Guard]; ok {
			return nil, ValidationError{Kind: ValidationGuardProofMismatch}
		}
		out[proof.Guard] = proof
	}
	return out, nil
}
