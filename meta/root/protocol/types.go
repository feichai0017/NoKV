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
	Era             uint64
	IssuedAt        Cursor
	Mandate         uint32
	LineageDigest   string
	Frontiers       MandateFrontiers
}

type AuthorityRootToken struct {
	Term     uint64
	Index    uint64
	Revision uint64
}

type AuthorityCertificate struct {
	HolderID           string
	Era                uint64
	Mandate            uint32
	ExpiresUnixNano    int64
	IssuedRootToken    AuthorityRootToken
	LineageDigest      string
	ObservedLegacyEra  uint64
	LegacyDigest       string
	DescriptorRevision uint64
	InheritedFrontiers MandateFrontiers
}

type MandateWitness struct {
	Mandate          uint32
	Era              uint64
	ConsumedFrontier uint64
}

type HandoverStage uint8

const (
	HandoverStageUnspecified HandoverStage = iota
	HandoverStageConfirmed
	HandoverStageClosed
	HandoverStageReattached
)

const (
	MandateWitnessEraAttached   uint64 = 0
	MandateWitnessEraSuppressed uint64 = ^uint64(0)
)

func (s HandoverStage) String() string {
	switch s {
	case HandoverStageUnspecified:
		return "unspecified"
	case HandoverStageConfirmed:
		return "confirmed"
	case HandoverStageClosed:
		return "closed"
	case HandoverStageReattached:
		return "reattached"
	default:
		return "unknown"
	}
}

type HandoverWitness struct {
	LegacyEra                 uint64
	LegacyDigest              string
	SuccessorPresent          bool
	Inheritance               InheritanceStatus
	SuccessorLineageSatisfied bool
	SealedEraRetired          bool
	Stage                     HandoverStage
}

type HandoverStatus struct {
	Stage HandoverStage
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

type HandoverAct uint8

const (
	HandoverActUnknown HandoverAct = iota
	HandoverActSeal
	HandoverActConfirm
	HandoverActClose
	HandoverActReattach
)

type HandoverCommand struct {
	Kind        HandoverAct
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

func NewMandateWitness(mandate uint32, era, consumedFrontier uint64) MandateWitness {
	return MandateWitness{
		Mandate:          mandate,
		Era:              era,
		ConsumedFrontier: consumedFrontier,
	}
}

func NewAuthorityCertificate(holderID string, era uint64, mandate uint32, expiresUnixNano int64, issuedRootToken AuthorityRootToken, lineageDigest string, observedLegacyEra uint64, legacyDigest string, descriptorRevision uint64, inheritedFrontiers MandateFrontiers) (AuthorityCertificate, error) {
	holderID = strings.TrimSpace(holderID)
	lineageDigest = strings.TrimSpace(lineageDigest)
	legacyDigest = strings.TrimSpace(legacyDigest)
	if holderID == "" {
		return AuthorityCertificate{}, fmt.Errorf("authority certificate: holder id is required")
	}
	if era == 0 || era == MandateWitnessEraSuppressed {
		return AuthorityCertificate{}, fmt.Errorf("authority certificate: detached era is required")
	}
	resolvedMandate := mandate & mandateMaskAll
	if resolvedMandate == 0 {
		return AuthorityCertificate{}, fmt.Errorf("authority certificate: duty mask is required")
	}
	if expiresUnixNano <= 0 {
		return AuthorityCertificate{}, fmt.Errorf("authority certificate: expiry is required")
	}
	if observedLegacyEra != 0 && legacyDigest == "" {
		return AuthorityCertificate{}, fmt.Errorf("authority certificate: legacy digest is required")
	}
	return AuthorityCertificate{
		HolderID:           holderID,
		Era:                era,
		Mandate:            resolvedMandate,
		ExpiresUnixNano:    expiresUnixNano,
		IssuedRootToken:    issuedRootToken,
		LineageDigest:      lineageDigest,
		ObservedLegacyEra:  observedLegacyEra,
		LegacyDigest:       legacyDigest,
		DescriptorRevision: descriptorRevision,
		InheritedFrontiers: inheritedFrontiers,
	}, nil
}

func (c AuthorityCertificate) Present() bool {
	return strings.TrimSpace(c.HolderID) != "" || c.Era != 0 || c.Mandate != 0 || c.ExpiresUnixNano != 0
}

func (c AuthorityCertificate) Validate(mandate uint32, era uint64, nowUnixNano int64, observedLegacyEra uint64, legacyDigest string) error {
	if !c.Present() {
		return fmt.Errorf("authority certificate: missing")
	}
	holderID := strings.TrimSpace(c.HolderID)
	if holderID == "" {
		return fmt.Errorf("authority certificate: holder id is required")
	}
	if era == 0 || era == MandateWitnessEraSuppressed {
		return fmt.Errorf("authority certificate: invalid reply era=%d", era)
	}
	if c.Era != era {
		return fmt.Errorf("authority certificate: era=%d reply_era=%d", c.Era, era)
	}
	if c.Mandate&mandate != mandate {
		return fmt.Errorf("authority certificate: required_mandate=%d certificate_mandate=%d", mandate, c.Mandate)
	}
	if c.ExpiresUnixNano <= 0 {
		return fmt.Errorf("authority certificate: expiry is required")
	}
	if nowUnixNano > 0 && c.ExpiresUnixNano <= nowUnixNano {
		return fmt.Errorf("authority certificate: expired")
	}
	if c.ObservedLegacyEra < observedLegacyEra {
		return fmt.Errorf("authority certificate: observed_legacy_era=%d reply_observed_legacy_era=%d", c.ObservedLegacyEra, observedLegacyEra)
	}
	if observedLegacyEra != 0 && c.ObservedLegacyEra != observedLegacyEra {
		return fmt.Errorf("authority certificate: observed_legacy_era=%d reply_observed_legacy_era=%d", c.ObservedLegacyEra, observedLegacyEra)
	}
	legacyDigest = strings.TrimSpace(legacyDigest)
	if observedLegacyEra != 0 && legacyDigest != "" && strings.TrimSpace(c.LegacyDigest) != legacyDigest {
		return fmt.Errorf("authority certificate: legacy digest mismatch")
	}
	return nil
}

func NewSuppressedMandateWitness(mandate uint32) MandateWitness {
	return MandateWitness{
		Mandate:          mandate,
		Era:              MandateWitnessEraSuppressed,
		ConsumedFrontier: 0,
	}
}

func NewAuthorityHandoffRecord(holderID string, expiresUnixNano int64, era uint64, issuedAt Cursor, mandate uint32, lineageDigest string, frontiers MandateFrontiers) (AuthorityHandoffRecord, error) {
	holderID = strings.TrimSpace(holderID)
	lineageDigest = strings.TrimSpace(lineageDigest)
	if holderID == "" {
		if expiresUnixNano == 0 && era == 0 && issuedAt == (Cursor{}) && mandate == 0 && lineageDigest == "" && frontiers.Len() == 0 {
			return AuthorityHandoffRecord{}, nil
		}
		return AuthorityHandoffRecord{}, fmt.Errorf("authority handoff record: holder id is required")
	}
	if era == 0 {
		return AuthorityHandoffRecord{}, fmt.Errorf("authority handoff record: era is required")
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
		Era:             era,
		IssuedAt:        issuedAt,
		Mandate:         resolvedMandate,
		LineageDigest:   lineageDigest,
		Frontiers:       frontiers,
	}, nil
}

func MustNewAuthorityHandoffRecord(holderID string, expiresUnixNano int64, era uint64, issuedAt Cursor, mandate uint32, lineageDigest string, frontiers MandateFrontiers) AuthorityHandoffRecord {
	record, err := NewAuthorityHandoffRecord(holderID, expiresUnixNano, era, issuedAt, mandate, lineageDigest, frontiers)
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

func (w HandoverWitness) FinalitySatisfied() bool {
	return w.LegacyEra != 0 &&
		w.SuccessorPresent &&
		w.SuccessorLineageSatisfied &&
		w.Inheritance.Covered() &&
		w.SealedEraRetired
}

func (w HandoverWitness) SuccessorMonotoneCovered() bool {
	return w.SuccessorPresent &&
		w.Inheritance.CoveredMandate(MandateAllocID|MandateTSO)
}

func (w HandoverWitness) SuccessorDescriptorCovered() bool {
	return w.SuccessorPresent &&
		w.Inheritance.CoveredMandate(MandateGetRegionByKey)
}

func (w HandoverWitness) ReplyEraLegal(era uint64) bool {
	if era == MandateWitnessEraAttached {
		return true
	}
	if era == MandateWitnessEraSuppressed {
		return false
	}
	if w.LegacyEra == 0 {
		return true
	}
	if era == w.LegacyEra {
		return false
	}
	return w.FinalitySatisfied()
}

func (w HandoverWitness) WithStage(stage HandoverStage) HandoverWitness {
	w.Stage = stage
	return w
}

func (r AuthorityHandoffRecord) Present() bool {
	return r.HolderID != ""
}

func HandoverStageAtLeast(stage, target HandoverStage) bool {
	switch target {
	case HandoverStageUnspecified:
		return true
	case HandoverStageConfirmed:
		return stage == HandoverStageConfirmed ||
			stage == HandoverStageClosed ||
			stage == HandoverStageReattached
	case HandoverStageClosed:
		return stage == HandoverStageClosed ||
			stage == HandoverStageReattached
	case HandoverStageReattached:
		return stage == HandoverStageReattached
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
