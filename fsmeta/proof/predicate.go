// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package proof

import "crypto/sha256"

type ReadSource uint8

const (
	ReadSourceUnknown ReadSource = iota
	ReadSourceOverlay
	ReadSourceSegment
	ReadSourceBase
)

type PredicateProofKind uint8

const (
	PredicateProofUnknown PredicateProofKind = iota
	PredicateProofPointValue
	PredicateProofPointAbsence
	PredicateProofOverlayFrontierAbsence
)

type PredicateProof struct {
	SchemaVersion Version
	Rule          RuleID
	Key           []byte
	Present       bool
	Value         []byte
	Version       uint64
	Source        ReadSource
	ProofFrontier ProofFrontier
	ProofKind     PredicateProofKind
	ScopeDigest   [32]byte
	Digest        [32]byte
}

func PredicateRuleFor(kind PredicateProofKind) RuleID {
	switch kind {
	case PredicateProofPointValue:
		return RulePredicatePointValue
	case PredicateProofPointAbsence:
		return RulePredicatePointAbsence
	case PredicateProofOverlayFrontierAbsence:
		return RulePredicateOverlayFrontierAbsence
	default:
		return ""
	}
}

func PredicateProofKindFor(present bool, source ReadSource) PredicateProofKind {
	if present {
		return PredicateProofPointValue
	}
	if source == ReadSourceOverlay {
		return PredicateProofOverlayFrontierAbsence
	}
	if source == ReadSourceBase || source == ReadSourceSegment {
		return PredicateProofPointAbsence
	}
	return PredicateProofUnknown
}

func PredicateProofScopeDigest(key, value []byte, present bool, version uint64, source ReadSource, frontier ProofFrontier) [32]byte {
	kind := PredicateProofKindFor(present, source)
	h := newDigestBuilder()
	h.writeString("fsmeta-proof/predicate-scope")
	h.writeUint64(uint64(Version1))
	h.writeString(string(PredicateRuleFor(kind)))
	h.writeUint64(uint64(kind))
	h.writeBytes(key)
	h.writeBool(present)
	h.writeUint64(uint64(source))
	switch {
	case present:
		valueHash := sha256.Sum256(value)
		h.writeBytes(valueHash[:])
		h.writeUint64(version)
		h.writeUint64(frontier.EpochID)
		h.writeUint64(frontier.Sequence)
	case source == ReadSourceOverlay:
		h.writeUint64(frontier.EpochID)
		h.writeUint64(frontier.Sequence)
	default:
		h.writeUint64(version)
	}
	return h.sum()
}

func PredicateProofDigest(key, value []byte, present bool, version uint64, source ReadSource, frontier ProofFrontier) [32]byte {
	kind := PredicateProofKindFor(present, source)
	scopeDigest := PredicateProofScopeDigest(key, value, present, version, source, frontier)
	h := newDigestBuilder()
	h.writeString("fsmeta-proof/predicate")
	h.writeUint64(uint64(Version1))
	h.writeString(string(PredicateRuleFor(kind)))
	h.writeBytes(key)
	h.writeBool(present)
	h.writeBytes(value)
	h.writeUint64(version)
	h.writeUint64(uint64(source))
	h.writeUint64(frontier.EpochID)
	h.writeUint64(frontier.Sequence)
	h.writeUint64(uint64(kind))
	h.writeBytes(scopeDigest[:])
	return h.sum()
}

func NewPredicateProof(key, value []byte, present bool, version uint64, source ReadSource, frontier ProofFrontier) PredicateProof {
	kind := PredicateProofKindFor(present, source)
	proof := PredicateProof{
		SchemaVersion: Version1,
		Rule:          PredicateRuleFor(kind),
		Key:           cloneBytes(key),
		Present:       present,
		Value:         cloneBytes(value),
		Version:       version,
		Source:        source,
		ProofFrontier: frontier,
		ProofKind:     kind,
	}
	proof.ScopeDigest = PredicateProofScopeDigest(proof.Key, proof.Value, proof.Present, proof.Version, proof.Source, proof.ProofFrontier)
	proof.Digest = PredicateProofDigest(proof.Key, proof.Value, proof.Present, proof.Version, proof.Source, proof.ProofFrontier)
	return proof
}

func VerifyPredicateProof(proof PredicateProof) error {
	if proof.SchemaVersion != Version1 {
		return ErrInvalidProof
	}
	if len(proof.Key) == 0 || (!proof.Present && len(proof.Value) != 0) {
		return ErrInvalidProof
	}
	if !predicateProofSourceValid(proof) {
		return ErrInvalidProof
	}
	if proof.ProofKind != PredicateProofKindFor(proof.Present, proof.Source) {
		return ErrInvalidProof
	}
	if proof.Rule != PredicateRuleFor(proof.ProofKind) {
		return ErrInvalidProof
	}
	if proof.ScopeDigest != PredicateProofScopeDigest(proof.Key, proof.Value, proof.Present, proof.Version, proof.Source, proof.ProofFrontier) {
		return ErrInvalidProof
	}
	if proof.Digest != PredicateProofDigest(proof.Key, proof.Value, proof.Present, proof.Version, proof.Source, proof.ProofFrontier) {
		return ErrInvalidProof
	}
	return nil
}

func predicateProofSourceValid(proof PredicateProof) bool {
	switch proof.Source {
	case ReadSourceUnknown:
		return !proof.Present && len(proof.Value) == 0 && proof.Version == 0 && proof.ProofFrontier == (ProofFrontier{})
	case ReadSourceOverlay:
		return proof.Version == 0
	case ReadSourceBase, ReadSourceSegment:
		return proof.Version != 0 && proof.ProofFrontier == (ProofFrontier{})
	default:
		return false
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}
