package state

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"strings"
)

type CoordinatorProtocolState struct {
	Lease   CoordinatorLease
	Seal    CoordinatorSeal
	Closure CoordinatorClosure
}

func (s State) CoordinatorProtocol() CoordinatorProtocolState {
	return CoordinatorProtocolState{
		Lease:   s.CoordinatorLease,
		Seal:    s.CoordinatorSeal,
		Closure: s.CoordinatorClosure,
	}
}

func (l CoordinatorLease) Present() bool {
	return strings.TrimSpace(l.HolderID) != "" && l.CertGeneration != 0
}

func (s CoordinatorSeal) Present() bool {
	return s.CertGeneration != 0 && strings.TrimSpace(s.HolderID) != ""
}

func (c CoordinatorClosure) Present() bool {
	return strings.TrimSpace(c.HolderID) != "" &&
		c.SealGeneration != 0 &&
		c.SuccessorGeneration != 0 &&
		strings.TrimSpace(c.SealDigest) != "" &&
		c.Stage != rootproto.CoordinatorClosureStageUnspecified
}

func CoordinatorSealDigest(seal CoordinatorSeal) string {
	if !seal.Present() {
		return ""
	}
	hasher := sha256.New()
	writeUint32 := func(value uint32) {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], value)
		_, _ = hasher.Write(buf[:])
	}
	writeUint64 := func(value uint64) {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], value)
		_, _ = hasher.Write(buf[:])
	}
	writeString := func(value string) {
		writeUint32(uint32(len(value)))
		_, _ = hasher.Write([]byte(value))
	}

	holderID := strings.TrimSpace(seal.HolderID)
	dutyMask := ResolvedCoordinatorDutyMask(seal.DutyMask)
	writeString(holderID)
	writeUint64(seal.CertGeneration)
	writeUint32(dutyMask)
	for _, mask := range rootproto.OrderedCoordinatorDutyMasks(dutyMask, seal.Frontiers) {
		frontier := seal.Frontiers.Frontier(mask)
		if frontier == 0 && dutyMask&mask == 0 {
			continue
		}
		writeUint32(mask)
		writeUint64(frontier)
	}
	writeUint64(seal.SealedAtCursor.Term)
	writeUint64(seal.SealedAtCursor.Index)
	return hex.EncodeToString(hasher.Sum(nil))
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
		return rootproto.CoordinatorDutyMaskDefault
	}
	return mask
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

func EvaluateCoordinatorLeaseSuccessorCoverage(current CoordinatorLease, seal CoordinatorSeal, frontiers rootproto.CoordinatorDutyFrontiers) rootproto.CoordinatorSuccessorCoverageStatus {
	if !seal.Present() {
		return rootproto.CoordinatorSuccessorCoverageStatus{}
	}
	dutyMask := ResolvedCoordinatorDutyMask(seal.DutyMask)
	requiredFrontiers := seal.Frontiers
	activeDutyMasks := rootproto.OrderedCoordinatorDutyMasks(dutyMask, rootproto.CoordinatorDutyFrontiers{})
	status := rootproto.CoordinatorSuccessorCoverageStatus{
		Checks: make([]rootproto.CoordinatorFrontierCoverage, 0, len(activeDutyMasks)),
	}
	for _, mask := range activeDutyMasks {
		required := requiredFrontiers.Frontier(mask)
		actual := frontiers.Frontier(mask)
		status.Checks = append(status.Checks, rootproto.CoordinatorFrontierCoverage{
			DutyMask:         mask,
			RequiredFrontier: required,
			ActualFrontier:   actual,
			Covered:          actual >= required,
		})
	}
	return status
}

func ValidateCoordinatorLeaseSuccessorCoverageFrontiers(current CoordinatorLease, seal CoordinatorSeal, frontiers rootproto.CoordinatorDutyFrontiers) error {
	if !CoordinatorGenerationSealed(current, seal) {
		return nil
	}
	coverage := EvaluateCoordinatorLeaseSuccessorCoverage(current, seal, frontiers)
	if gap, ok := coverage.FirstGap(); ok {
		return fmt.Errorf(
			"%w: duty=%s actual=%d required=%d",
			ErrCoordinatorLeaseCoverage,
			rootproto.CoordinatorDutyName(gap.DutyMask),
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

// ValidateCoordinatorLeaseStartCoverage is the rooted gate for any ordered
// served-summary frontier carried under CoordinatorDutyLeaseStart. It rejects
// a successor candidate whose lease_start does not strictly exceed the sealed
// predecessor's persisted served frontier.
func ValidateCoordinatorLeaseStartCoverage(seal CoordinatorSeal, successorLeaseStart uint64) error {
	if !seal.Present() {
		return nil
	}
	frontier := seal.Frontiers.Frontier(rootproto.CoordinatorDutyLeaseStart)
	if frontier == 0 {
		return nil
	}
	if successorLeaseStart > frontier {
		return nil
	}
	return fmt.Errorf(
		"%w: duty=%s successor_lease_start=%d served_frontier=%d",
		ErrCoordinatorLeaseCoverage,
		rootproto.CoordinatorDutyName(rootproto.CoordinatorDutyLeaseStart),
		successorLeaseStart,
		frontier,
	)
}

// CoordinatorSealWithServedFrontier returns seal with the lease-start frontier
// bumped to at least servedTimestamp. Existing frontier values are never
// lowered.
func CoordinatorSealWithServedFrontier(seal CoordinatorSeal, servedTimestamp uint64) CoordinatorSeal {
	existing := seal.Frontiers.Frontier(rootproto.CoordinatorDutyLeaseStart)
	if servedTimestamp <= existing {
		return seal
	}
	seal.Frontiers = seal.Frontiers.WithFrontier(rootproto.CoordinatorDutyLeaseStart, servedTimestamp)
	return seal
}
