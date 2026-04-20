package state

import (
	"fmt"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"strings"
)

func CoordinatorDutyName(dutyMask uint32) string {
	return rootproto.CoordinatorDutyName(dutyMask)
}

func CoordinatorSealDigest(seal CoordinatorSeal) string {
	if !seal.Present() {
		return ""
	}
	holderID := strings.TrimSpace(seal.HolderID)
	digest := fmt.Sprintf("%s/%d/%d", holderID, seal.CertGeneration, seal.DutyMask)
	for _, dutyMask := range OrderedCoordinatorDutyMasks(ResolvedCoordinatorDutyMask(seal.DutyMask), CoordinatorDutyFrontiers{}) {
		digest = fmt.Sprintf("%s/%d/%d", digest, dutyMask, seal.Frontiers.Frontier(dutyMask))
	}
	return fmt.Sprintf("%s/%d/%d", digest, seal.SealedAtCursor.Term, seal.SealedAtCursor.Index)
}

func ResolveCoordinatorLeasePredecessorDigest(current CoordinatorLease, seal CoordinatorSeal, holderID string, nowUnixNano int64) string {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return ""
	}
	if current.ActiveAt(nowUnixNano) && current.HolderID == holderID && !CoordinatorGenerationSealed(current, seal) {
		return strings.TrimSpace(current.PredecessorDigest)
	}
	if CoordinatorGenerationSealed(current, seal) {
		return CoordinatorSealDigest(seal)
	}
	return ""
}

func CoordinatorGenerationSealed(current CoordinatorLease, seal CoordinatorSeal) bool {
	if current.CertGeneration == 0 || seal.CertGeneration == 0 {
		return false
	}
	if current.CertGeneration != seal.CertGeneration {
		return false
	}
	if strings.TrimSpace(current.HolderID) == "" || !seal.Present() {
		return false
	}
	return current.HolderID == seal.HolderID
}

func ResolvedCoordinatorDutyMask(mask uint32) uint32 {
	if mask == 0 {
		return CoordinatorDutyMaskDefault
	}
	return mask
}

func CoordinatorLeaseAllowsDuty(current CoordinatorLease, seal CoordinatorSeal, holderID string, nowUnixNano int64, dutyMask uint32) bool {
	if !CoordinatorLeaseContinuable(current, seal, holderID, nowUnixNano) {
		return false
	}
	dutyMask = ResolvedCoordinatorDutyMask(dutyMask)
	if dutyMask == 0 {
		return true
	}
	currentMask := ResolvedCoordinatorDutyMask(current.DutyMask)
	return currentMask&dutyMask == dutyMask
}

// ValidateCoordinatorLeaseCampaign verifies whether holder can install a new
// coordinator lease over current at nowUnixNano.
func ValidateCoordinatorLeaseCampaign(current CoordinatorLease, seal CoordinatorSeal, holderID, predecessorDigest string, expiresUnixNano, nowUnixNano int64) error {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return fmt.Errorf("%w: holder id is required", ErrInvalidCoordinatorLease)
	}
	if expiresUnixNano <= nowUnixNano {
		return fmt.Errorf("%w: expiry must be in the future", ErrInvalidCoordinatorLease)
	}
	predecessorDigest = strings.TrimSpace(predecessorDigest)
	if CoordinatorGenerationSealed(current, seal) {
		expected := CoordinatorSealDigest(seal)
		if predecessorDigest != expected {
			return fmt.Errorf("%w: predecessor_digest=%q expected=%q", ErrCoordinatorLeaseLineage, predecessorDigest, expected)
		}
		return nil
	}
	if current.ActiveAt(nowUnixNano) && current.HolderID == holderID {
		expected := strings.TrimSpace(current.PredecessorDigest)
		if predecessorDigest != expected {
			return fmt.Errorf("%w: predecessor_digest=%q expected=%q", ErrCoordinatorLeaseLineage, predecessorDigest, expected)
		}
	}
	if current.ActiveAt(nowUnixNano) && current.HolderID != holderID {
		return fmt.Errorf("%w: holder=%s expires_unix_nano=%d", ErrCoordinatorLeaseHeld, current.HolderID, current.ExpiresUnixNano)
	}
	return nil
}

func EvaluateCoordinatorLeaseSuccessorCoverage(current CoordinatorLease, seal CoordinatorSeal, frontiers CoordinatorDutyFrontiers) CoordinatorSuccessorCoverageStatus {
	if !seal.Present() {
		return CoordinatorSuccessorCoverageStatus{}
	}
	dutyMask := ResolvedCoordinatorDutyMask(seal.DutyMask)
	requiredFrontiers := CoordinatorSealRequiredFrontiers(seal)
	activeDutyMasks := OrderedCoordinatorDutyMasks(dutyMask, CoordinatorDutyFrontiers{})
	status := CoordinatorSuccessorCoverageStatus{
		Checks: make([]CoordinatorFrontierCoverage, 0, len(activeDutyMasks)),
	}
	for _, mask := range activeDutyMasks {
		required := requiredFrontiers.Frontier(mask)
		actual := frontiers.Frontier(mask)
		status.Checks = append(status.Checks, CoordinatorFrontierCoverage{
			DutyMask:         mask,
			DutyName:         CoordinatorDutyName(mask),
			RequiredFrontier: required,
			ActualFrontier:   actual,
			Covered:          actual >= required,
		})
	}
	return status
}

func ValidateCoordinatorLeaseSuccessorCoverageFrontiers(current CoordinatorLease, seal CoordinatorSeal, frontiers CoordinatorDutyFrontiers) error {
	if !CoordinatorGenerationSealed(current, seal) {
		return nil
	}
	coverage := EvaluateCoordinatorLeaseSuccessorCoverage(current, seal, frontiers)
	if gap, ok := coverage.FirstGap(); ok {
		return fmt.Errorf(
			"%w: duty=%s actual=%d required=%d",
			ErrCoordinatorLeaseCoverage,
			gap.DutyName,
			gap.ActualFrontier,
			gap.RequiredFrontier,
		)
	}
	return nil
}

// ValidateCoordinatorLeaseRelease verifies whether holder can explicitly release
// the current coordinator lease at nowUnixNano.
func ValidateCoordinatorLeaseRelease(current CoordinatorLease, holderID string, nowUnixNano int64) error {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return fmt.Errorf("%w: holder id is required", ErrInvalidCoordinatorLease)
	}
	if strings.TrimSpace(current.HolderID) == "" {
		return fmt.Errorf("%w: no current holder", ErrCoordinatorLeaseOwner)
	}
	if current.HolderID != holderID {
		return fmt.Errorf("%w: current=%s requested=%s", ErrCoordinatorLeaseOwner, current.HolderID, holderID)
	}
	if current.ExpiresUnixNano <= nowUnixNano {
		return nil
	}
	return nil
}

// NextCoordinatorLeaseGeneration returns the authority generation that should be
// attached to the next rooted lease record for holderID at nowUnixNano.
//
// Same-holder renewals of a still-live lease preserve the current generation.
// Any new authority instance, including takeover after expiry or reacquire
// after explicit release, advances the generation.
func NextCoordinatorLeaseGeneration(current CoordinatorLease, seal CoordinatorSeal, holderID string, nowUnixNano int64) uint64 {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return current.CertGeneration
	}
	if current.ActiveAt(nowUnixNano) && current.HolderID == holderID && !CoordinatorGenerationSealed(current, seal) {
		if current.CertGeneration != 0 {
			return current.CertGeneration
		}
		return 1
	}
	if current.CertGeneration == 0 {
		return 1
	}
	return current.CertGeneration + 1
}

// ValidateCoordinatorLeaseSeal verifies whether holder can seal the current
// rooted authority generation.
func ValidateCoordinatorLeaseSeal(current CoordinatorLease, holderID string) error {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return fmt.Errorf("%w: holder id is required", ErrInvalidCoordinatorLease)
	}
	if strings.TrimSpace(current.HolderID) == "" || current.CertGeneration == 0 {
		return fmt.Errorf("%w: no current holder", ErrCoordinatorLeaseOwner)
	}
	if current.HolderID != holderID {
		return fmt.Errorf("%w: current=%s requested=%s", ErrCoordinatorLeaseOwner, current.HolderID, holderID)
	}
	return nil
}

func CoordinatorLeaseContinuable(current CoordinatorLease, seal CoordinatorSeal, holderID string, nowUnixNano int64) bool {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return false
	}
	if current.HolderID != holderID || !current.ActiveAt(nowUnixNano) {
		return false
	}
	return !CoordinatorGenerationSealed(current, seal)
}
