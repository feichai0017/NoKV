package state

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"strings"
)

type SuccessionState struct {
	Tenure   Tenure
	Legacy   Legacy
	Handover Handover
}

func (s State) Succession() SuccessionState {
	return SuccessionState{
		Tenure:   s.Tenure,
		Legacy:   s.Legacy,
		Handover: s.Handover,
	}
}

func (l Tenure) Present() bool {
	return strings.TrimSpace(l.HolderID) != "" && l.Epoch != 0
}

func (s Legacy) Present() bool {
	return s.Epoch != 0 && strings.TrimSpace(s.HolderID) != ""
}

func (c Handover) Present() bool {
	return strings.TrimSpace(c.HolderID) != "" &&
		c.LegacyEpoch != 0 &&
		c.SuccessorEpoch != 0 &&
		strings.TrimSpace(c.LegacyDigest) != "" &&
		c.Stage != rootproto.HandoverStageUnspecified
}

func DigestOfLegacy(seal Legacy) string {
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
	mandate := seal.Mandate
	writeString(holderID)
	writeUint64(seal.Epoch)
	writeUint32(mandate)
	for _, mask := range rootproto.OrderedMandateMasks(mandate, seal.Frontiers) {
		frontier := seal.Frontiers.Frontier(mask)
		if frontier == 0 && mandate&mask == 0 {
			continue
		}
		writeUint32(mask)
		writeUint64(frontier)
	}
	writeUint64(seal.SealedAt.Term)
	writeUint64(seal.SealedAt.Index)
	return hex.EncodeToString(hasher.Sum(nil))
}

func ResolveLineageDigest(current Tenure, seal Legacy, holderID string, nowUnixNano int64) string {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return ""
	}
	if current.ActiveAt(nowUnixNano) && current.HolderID == holderID && !TenureSealed(current, seal) {
		return strings.TrimSpace(current.LineageDigest)
	}
	if TenureSealed(current, seal) {
		return DigestOfLegacy(seal)
	}
	return ""
}

func TenureSealed(current Tenure, seal Legacy) bool {
	if current.Epoch == 0 || seal.Epoch == 0 {
		return false
	}
	if current.Epoch != seal.Epoch {
		return false
	}
	if strings.TrimSpace(current.HolderID) == "" || !seal.Present() {
		return false
	}
	return current.HolderID == seal.HolderID
}

// ValidateTenureClaim verifies whether holder can install a new
// coordinator lease over current at nowUnixNano.
func ValidateTenureClaim(current Tenure, seal Legacy, holderID, lineageDigest string, expiresUnixNano, nowUnixNano int64) error {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return fmt.Errorf("%w: holder id is required", ErrInvalidTenure)
	}
	if expiresUnixNano <= nowUnixNano {
		return fmt.Errorf("%w: expiry must be in the future", ErrInvalidTenure)
	}
	lineageDigest = strings.TrimSpace(lineageDigest)
	if TenureSealed(current, seal) {
		expected := DigestOfLegacy(seal)
		if lineageDigest != expected {
			return fmt.Errorf("%w: lineage_digest=%q expected=%q", ErrInheritance, lineageDigest, expected)
		}
		return nil
	}
	if current.ActiveAt(nowUnixNano) && current.HolderID == holderID {
		expected := strings.TrimSpace(current.LineageDigest)
		if lineageDigest != expected {
			return fmt.Errorf("%w: lineage_digest=%q expected=%q", ErrInheritance, lineageDigest, expected)
		}
	}
	if current.ActiveAt(nowUnixNano) && current.HolderID != holderID {
		return fmt.Errorf("%w: holder=%s expires_unix_nano=%d", ErrPrimacy, current.HolderID, current.ExpiresUnixNano)
	}
	return nil
}

func EvaluateInheritance(current Tenure, seal Legacy, frontiers rootproto.MandateFrontiers) rootproto.InheritanceStatus {
	if !seal.Present() {
		return rootproto.InheritanceStatus{}
	}
	mandate := seal.Mandate
	requiredFrontiers := seal.Frontiers
	activeMandates := rootproto.OrderedMandateMasks(mandate, rootproto.MandateFrontiers{})
	status := rootproto.InheritanceStatus{
		Checks: make([]rootproto.InheritanceCoverage, 0, len(activeMandates)),
	}
	for _, mask := range activeMandates {
		required := requiredFrontiers.Frontier(mask)
		actual := frontiers.Frontier(mask)
		status.Checks = append(status.Checks, rootproto.InheritanceCoverage{
			Mandate:          mask,
			RequiredFrontier: required,
			ActualFrontier:   actual,
			Covered:          actual >= required,
		})
	}
	return status
}

func ValidateInheritance(current Tenure, seal Legacy, frontiers rootproto.MandateFrontiers) error {
	if !TenureSealed(current, seal) {
		return nil
	}
	coverage := EvaluateInheritance(current, seal, frontiers)
	if gap, ok := coverage.FirstGap(); ok {
		return fmt.Errorf(
			"%w: duty=%s actual=%d required=%d",
			ErrInheritance,
			rootproto.MandateName(gap.Mandate),
			gap.ActualFrontier,
			gap.RequiredFrontier,
		)
	}
	return nil
}

// ValidateTenureYield verifies whether holder can explicitly release
// the current coordinator lease at nowUnixNano.
func ValidateTenureYield(current Tenure, holderID string, nowUnixNano int64) error {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return fmt.Errorf("%w: holder id is required", ErrInvalidTenure)
	}
	if strings.TrimSpace(current.HolderID) == "" {
		return fmt.Errorf("%w: no current holder", ErrPrimacy)
	}
	if current.HolderID != holderID {
		return fmt.Errorf("%w: current=%s requested=%s", ErrPrimacy, current.HolderID, holderID)
	}
	if current.ExpiresUnixNano <= nowUnixNano {
		return nil
	}
	return nil
}

// NextTenureEpoch returns the authority generation that should be
// attached to the next rooted lease record for holderID at nowUnixNano.
//
// Same-holder renewals of a still-live lease preserve the current generation.
// Any new authority instance, including takeover after expiry or reacquire
// after explicit release, advances the generation.
func NextTenureEpoch(current Tenure, seal Legacy, holderID string, nowUnixNano int64) uint64 {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return current.Epoch
	}
	if current.ActiveAt(nowUnixNano) && current.HolderID == holderID && !TenureSealed(current, seal) {
		if current.Epoch != 0 {
			return current.Epoch
		}
		return 1
	}
	if current.Epoch == 0 {
		return 1
	}
	return current.Epoch + 1
}

// ValidateLegacyFormation verifies whether holder can seal the current
// rooted authority generation.
func ValidateLegacyFormation(current Tenure, holderID string) error {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return fmt.Errorf("%w: holder id is required", ErrInvalidTenure)
	}
	if strings.TrimSpace(current.HolderID) == "" || current.Epoch == 0 {
		return fmt.Errorf("%w: no current holder", ErrPrimacy)
	}
	if current.HolderID != holderID {
		return fmt.Errorf("%w: current=%s requested=%s", ErrPrimacy, current.HolderID, holderID)
	}
	return nil
}

func TenureRenewable(current Tenure, seal Legacy, holderID string, nowUnixNano int64) bool {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return false
	}
	if current.HolderID != holderID || !current.ActiveAt(nowUnixNano) {
		return false
	}
	return !TenureSealed(current, seal)
}

// ValidateLeaseStartInheritance is the rooted gate for any ordered
// served-summary frontier carried under MandateLeaseStart. It rejects
// a successor candidate whose lease_start does not strictly exceed the sealed
// predecessor's persisted served frontier.
func ValidateLeaseStartInheritance(seal Legacy, successorLeaseStart uint64) error {
	if !seal.Present() {
		return nil
	}
	frontier := seal.Frontiers.Frontier(rootproto.MandateLeaseStart)
	if frontier == 0 {
		return nil
	}
	if successorLeaseStart > frontier {
		return nil
	}
	return fmt.Errorf(
		"%w: duty=%s successor_lease_start=%d served_frontier=%d",
		ErrInheritance,
		rootproto.MandateName(rootproto.MandateLeaseStart),
		successorLeaseStart,
		frontier,
	)
}

// LegacyWithServedFrontier returns seal with the lease-start frontier
// bumped to at least servedTimestamp. Existing frontier values are never
// lowered.
func LegacyWithServedFrontier(seal Legacy, servedTimestamp uint64) Legacy {
	existing := seal.Frontiers.Frontier(rootproto.MandateLeaseStart)
	if servedTimestamp <= existing {
		return seal
	}
	seal.Frontiers = seal.Frontiers.WithFrontier(rootproto.MandateLeaseStart, servedTimestamp)
	return seal
}
