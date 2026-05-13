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
	ValidationPlacementMismatch
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
	case ValidationPlacementMismatch:
		return "placement mismatch"
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

func (op MaterializedOp) ValidateForAdmission() error {
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
		if obligation.Kind != PredicateObservedValue {
			continue
		}
		proof, ok := proofs[string(obligation.Key)]
		if !ok {
			return ValidationError{Kind: ValidationPredicateProofMissing, Key: obligation.Key}
		}
		if !predicateProofMatches(obligation, proof) {
			return ValidationError{Kind: ValidationPredicateProofMismatch, Key: obligation.Key}
		}
	}
	return nil
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
		digest := PredicateProofDigest(proof.Key, proof.Value, proof.Present, proof.Version, proof.Source)
		if digest != proof.Digest {
			return nil, ValidationError{Kind: ValidationPredicateProofMismatch, Key: proof.Key}
		}
		out[string(proof.Key)] = proof
	}
	return out, nil
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
		return true
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
		return len(scope.Parents) == 0 || slices.Contains(scope.Parents, ref.Parent)
	case fsmeta.KeyKindInode, fsmeta.KeyKindChunk, fsmeta.KeyKindSession:
		return len(scope.Inodes) == 0 || slices.Contains(scope.Inodes, ref.Inode)
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
		return false
	}
	for _, bucket := range buckets {
		if !slices.Contains(scope.Buckets, bucket) {
			return false
		}
	}
	return true
}
