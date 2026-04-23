package protocol

import (
	"fmt"
	"math/bits"
	"strings"
)

// Cursor identifies one committed position in the metadata-root log.
type Cursor struct {
	Term  uint64
	Index uint64
}

const (
	MandateAllocID uint32 = 1 << iota
	MandateTSO
	MandateGetRegionByKey
	MandateLeaseStart
)

const MandateDefault = MandateAllocID | MandateTSO | MandateGetRegionByKey

type MandateFrontier struct {
	Mandate  uint32
	Frontier uint64
}

const mandateMaskAll = MandateAllocID | MandateTSO | MandateGetRegionByKey | MandateLeaseStart

var mandateOrder = [...]uint32{
	MandateAllocID,
	MandateTSO,
	MandateGetRegionByKey,
	MandateLeaseStart,
}

// MandateFrontiers is the protocol-level duty frontier algebra carried
// across rooted handoff, verifier, and audit boundaries.
type MandateFrontiers struct {
	values  [len(mandateOrder)]uint64
	present uint32
}

type InheritanceCoverage struct {
	Mandate          uint32
	RequiredFrontier uint64
	ActualFrontier   uint64
	Covered          bool
}

type InheritanceStatus struct {
	Checks []InheritanceCoverage
}

type AuthorityHandoffRecord struct {
	HolderID        string
	ExpiresUnixNano int64
	Epoch           uint64
	IssuedAt        Cursor
	Mandate         uint32
	LineageDigest   string
	Frontiers       MandateFrontiers
}

type ContinuationWitness struct {
	Mandate          uint32
	Epoch            uint64
	ConsumedFrontier uint64
}

type TransitStage uint8

const (
	TransitStageUnspecified TransitStage = iota
	TransitStagePendingConfirm
	TransitStageConfirmed
	TransitStageClosed
	TransitStageReattached
)

const (
	ContinuationWitnessGenerationAttached   uint64 = 0
	ContinuationWitnessGenerationSuppressed uint64 = ^uint64(0)
)

func (s TransitStage) String() string {
	switch s {
	case TransitStageUnspecified:
		return "unspecified"
	case TransitStagePendingConfirm:
		return "pending_confirm"
	case TransitStageConfirmed:
		return "confirmed"
	case TransitStageClosed:
		return "closed"
	case TransitStageReattached:
		return "reattached"
	default:
		return "unknown"
	}
}

type TransitWitness struct {
	LegacyEpoch               uint64
	LegacyDigest              string
	SuccessorPresent          bool
	Inheritance               InheritanceStatus
	SuccessorLineageSatisfied bool
	SealedGenerationRetired   bool
	Stage                     TransitStage
}

type TransitStatus struct {
	Stage TransitStage
}

type TenureAct uint8

const (
	TenureActUnknown TenureAct = iota
	TenureActIssue
	TenureActRelease
)

type TenureCommand struct {
	Kind               TenureAct
	HolderID           string
	ExpiresUnixNano    int64
	NowUnixNano        int64
	LineageDigest      string
	InheritedFrontiers MandateFrontiers
}

type TransitAct uint8

const (
	TransitActUnknown TransitAct = iota
	TransitActSeal
	TransitActConfirm
	TransitActClose
	TransitActReattach
)

type TransitCommand struct {
	Kind        TransitAct
	HolderID    string
	NowUnixNano int64
	Frontiers   MandateFrontiers
}

func MandateName(mandate uint32) string {
	switch mandate {
	case MandateAllocID:
		return "alloc_id"
	case MandateTSO:
		return "tso"
	case MandateGetRegionByKey:
		return "get_region_by_key"
	case MandateLeaseStart:
		return "lease_start"
	default:
		return fmt.Sprintf("mandate_%d", mandate)
	}
}

func NewMandateFrontiers(entries ...MandateFrontier) MandateFrontiers {
	var frontiers MandateFrontiers
	for _, entry := range entries {
		frontiers = frontiers.WithFrontier(entry.Mandate, entry.Frontier)
	}
	return frontiers
}

func MandateFrontiersFromMap(values map[uint32]uint64) MandateFrontiers {
	var frontiers MandateFrontiers
	for mandate, frontier := range values {
		frontiers = frontiers.WithFrontier(mandate, frontier)
	}
	return frontiers
}

func (f MandateFrontiers) Frontier(mandate uint32) uint64 {
	idx, ok := mandateIndex(mandate)
	if !ok {
		return 0
	}
	return f.values[idx]
}

func (f MandateFrontiers) Len() int {
	return bits.OnesCount32(f.present & mandateMaskAll)
}

func (f MandateFrontiers) Entries() []MandateFrontier {
	if f.Len() == 0 {
		return nil
	}
	out := make([]MandateFrontier, 0, f.Len())
	for _, mandate := range OrderedMandateMasks(0, f) {
		out = append(out, MandateFrontier{
			Mandate:  mandate,
			Frontier: f.Frontier(mandate),
		})
	}
	return out
}

func (f MandateFrontiers) AsMap() map[uint32]uint64 {
	if f.Len() == 0 {
		return map[uint32]uint64{}
	}
	out := make(map[uint32]uint64, f.Len())
	for _, mandate := range OrderedMandateMasks(0, f) {
		out[mandate] = f.Frontier(mandate)
	}
	return out
}

func (f MandateFrontiers) WithFrontier(mandate uint32, frontier uint64) MandateFrontiers {
	idx, ok := mandateIndex(mandate)
	if !ok {
		return f
	}
	f.values[idx] = frontier
	f.present |= mandate
	return f
}

func OrderedMandateMasks(mandate uint32, frontiers MandateFrontiers) []uint32 {
	seen := (frontiers.present | mandate) & mandateMaskAll
	out := make([]uint32, 0, bits.OnesCount32(seen))
	for _, mask := range mandateOrder {
		if seen&mask == 0 {
			continue
		}
		out = append(out, mask)
	}
	return out
}

func NewContinuationWitness(mandate uint32, epoch, consumedFrontier uint64) ContinuationWitness {
	return ContinuationWitness{
		Mandate:          mandate,
		Epoch:            epoch,
		ConsumedFrontier: consumedFrontier,
	}
}

func NewSuppressedContinuationWitness(mandate uint32) ContinuationWitness {
	return ContinuationWitness{
		Mandate:          mandate,
		Epoch:            ContinuationWitnessGenerationSuppressed,
		ConsumedFrontier: 0,
	}
}

func NewAuthorityHandoffRecord(holderID string, expiresUnixNano int64, epoch uint64, issuedAt Cursor, mandate uint32, lineageDigest string, frontiers MandateFrontiers) (AuthorityHandoffRecord, error) {
	holderID = strings.TrimSpace(holderID)
	lineageDigest = strings.TrimSpace(lineageDigest)
	if holderID == "" {
		if expiresUnixNano == 0 && epoch == 0 && issuedAt == (Cursor{}) && mandate == 0 && lineageDigest == "" && frontiers.Len() == 0 {
			return AuthorityHandoffRecord{}, nil
		}
		return AuthorityHandoffRecord{}, fmt.Errorf("authority handoff record: holder id is required")
	}
	if epoch == 0 {
		return AuthorityHandoffRecord{}, fmt.Errorf("authority handoff record: cert generation is required")
	}
	resolvedMandate := mandate & mandateMaskAll
	if resolvedMandate == 0 {
		return AuthorityHandoffRecord{}, fmt.Errorf("authority handoff record: duty mask is required")
	}
	if frontiers.present&resolvedMandate != resolvedMandate {
		return AuthorityHandoffRecord{}, fmt.Errorf("authority handoff record: frontiers must cover all duty mask bits")
	}
	return AuthorityHandoffRecord{
		HolderID:        holderID,
		ExpiresUnixNano: expiresUnixNano,
		Epoch:           epoch,
		IssuedAt:        issuedAt,
		Mandate:         resolvedMandate,
		LineageDigest:   lineageDigest,
		Frontiers:       frontiers,
	}, nil
}

func MustNewAuthorityHandoffRecord(holderID string, expiresUnixNano int64, epoch uint64, issuedAt Cursor, mandate uint32, lineageDigest string, frontiers MandateFrontiers) AuthorityHandoffRecord {
	record, err := NewAuthorityHandoffRecord(holderID, expiresUnixNano, epoch, issuedAt, mandate, lineageDigest, frontiers)
	if err != nil {
		panic(err)
	}
	return record
}

func (s InheritanceStatus) Covered() bool {
	for _, check := range s.Checks {
		if !check.Covered {
			return false
		}
	}
	return true
}

func (s InheritanceStatus) CoveredMandate(mandate uint32) bool {
	for _, check := range s.Checks {
		if mandate&check.Mandate == 0 {
			continue
		}
		if !check.Covered {
			return false
		}
	}
	return true
}

func (s InheritanceStatus) FirstGap() (InheritanceCoverage, bool) {
	for _, check := range s.Checks {
		if !check.Covered {
			return check, true
		}
	}
	return InheritanceCoverage{}, false
}

func (w TransitWitness) ClosureSatisfied() bool {
	return w.LegacyEpoch != 0 &&
		w.SuccessorPresent &&
		w.SuccessorLineageSatisfied &&
		w.Inheritance.Covered() &&
		w.SealedGenerationRetired
}

func (w TransitWitness) SuccessorMonotoneCovered() bool {
	return w.SuccessorPresent &&
		w.Inheritance.CoveredMandate(MandateAllocID|MandateTSO)
}

func (w TransitWitness) SuccessorDescriptorCovered() bool {
	return w.SuccessorPresent &&
		w.Inheritance.CoveredMandate(MandateGetRegionByKey)
}

func (w TransitWitness) ReplyGenerationLegal(epoch uint64) bool {
	if epoch == ContinuationWitnessGenerationAttached {
		return true
	}
	if epoch == ContinuationWitnessGenerationSuppressed {
		return false
	}
	if w.LegacyEpoch == 0 {
		return true
	}
	if epoch == w.LegacyEpoch {
		return false
	}
	return w.ClosureSatisfied()
}

func (w TransitWitness) WithStage(stage TransitStage) TransitWitness {
	w.Stage = stage
	return w
}

func (r AuthorityHandoffRecord) Present() bool {
	return r.HolderID != ""
}

func TransitStageAtLeast(stage, target TransitStage) bool {
	switch target {
	case TransitStageUnspecified:
		return true
	case TransitStagePendingConfirm:
		return stage == TransitStagePendingConfirm ||
			stage == TransitStageConfirmed ||
			stage == TransitStageClosed ||
			stage == TransitStageReattached
	case TransitStageConfirmed:
		return stage == TransitStageConfirmed ||
			stage == TransitStageClosed ||
			stage == TransitStageReattached
	case TransitStageClosed:
		return stage == TransitStageClosed ||
			stage == TransitStageReattached
	case TransitStageReattached:
		return stage == TransitStageReattached
	default:
		return false
	}
}

func mandateIndex(mandate uint32) (int, bool) {
	switch mandate {
	case MandateAllocID:
		return 0, true
	case MandateTSO:
		return 1, true
	case MandateGetRegionByKey:
		return 2, true
	case MandateLeaseStart:
		return 3, true
	default:
		return 0, false
	}
}
