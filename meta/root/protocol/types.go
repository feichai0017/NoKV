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
	CoordinatorDutyAllocID uint32 = 1 << iota
	CoordinatorDutyTSO
	CoordinatorDutyGetRegionByKey
	CoordinatorDutyLeaseStart
)

const CoordinatorDutyMaskDefault = CoordinatorDutyAllocID | CoordinatorDutyTSO | CoordinatorDutyGetRegionByKey

type CoordinatorDutyFrontier struct {
	DutyMask uint32
	DutyName string
	Frontier uint64
}

const coordinatorDutyFrontierMaskAll = CoordinatorDutyAllocID | CoordinatorDutyTSO | CoordinatorDutyGetRegionByKey | CoordinatorDutyLeaseStart

var coordinatorDutyOrder = [...]uint32{
	CoordinatorDutyAllocID,
	CoordinatorDutyTSO,
	CoordinatorDutyGetRegionByKey,
	CoordinatorDutyLeaseStart,
}

// CoordinatorDutyFrontiers is the protocol-level duty frontier algebra carried
// across rooted handoff, verifier, and audit boundaries.
type CoordinatorDutyFrontiers struct {
	values  [len(coordinatorDutyOrder)]uint64
	present uint32
}

type CoordinatorFrontierCoverage struct {
	DutyMask         uint32
	DutyName         string
	RequiredFrontier uint64
	ActualFrontier   uint64
	Covered          bool
}

type CoordinatorSuccessorCoverageStatus struct {
	Checks []CoordinatorFrontierCoverage
}

type AuthorityHandoffRecord struct {
	holderID          string
	expiresUnixNano   int64
	certGeneration    uint64
	issuedCursor      Cursor
	dutyMask          uint32
	predecessorDigest string
	frontiers         CoordinatorDutyFrontiers
}

type ContinuationWitness struct {
	DutyMask         uint32
	DutyName         string
	CertGeneration   uint64
	ConsumedFrontier uint64
}

type CoordinatorClosureStage uint8

const (
	CoordinatorClosureStageUnspecified CoordinatorClosureStage = iota
	CoordinatorClosureStagePendingConfirm
	CoordinatorClosureStageConfirmed
	CoordinatorClosureStageClosed
	CoordinatorClosureStageReattached
)

const (
	ContinuationWitnessGenerationAttached   uint64 = 0
	ContinuationWitnessGenerationSuppressed uint64 = ^uint64(0)
)

func (s CoordinatorClosureStage) String() string {
	switch s {
	case CoordinatorClosureStageUnspecified:
		return "unspecified"
	case CoordinatorClosureStagePendingConfirm:
		return "pending_confirm"
	case CoordinatorClosureStageConfirmed:
		return "confirmed"
	case CoordinatorClosureStageClosed:
		return "closed"
	case CoordinatorClosureStageReattached:
		return "reattached"
	default:
		return "unknown"
	}
}

type ClosureWitness struct {
	SealGeneration            uint64
	SealDigest                string
	SuccessorPresent          bool
	SuccessorCoverage         CoordinatorSuccessorCoverageStatus
	SuccessorLineageSatisfied bool
	SealedGenerationRetired   bool
	Stage                     CoordinatorClosureStage
}

type CoordinatorClosureStatus struct {
	Stage CoordinatorClosureStage
}

type CoordinatorLeaseCommandKind uint8

const (
	CoordinatorLeaseCommandUnknown CoordinatorLeaseCommandKind = iota
	CoordinatorLeaseCommandIssue
	CoordinatorLeaseCommandRelease
)

type CoordinatorLeaseCommand struct {
	Kind              CoordinatorLeaseCommandKind
	HolderID          string
	ExpiresUnixNano   int64
	NowUnixNano       int64
	PredecessorDigest string
	HandoffFrontiers  CoordinatorDutyFrontiers
}

type CoordinatorClosureCommandKind uint8

const (
	CoordinatorClosureCommandUnknown CoordinatorClosureCommandKind = iota
	CoordinatorClosureCommandSeal
	CoordinatorClosureCommandConfirm
	CoordinatorClosureCommandClose
	CoordinatorClosureCommandReattach
)

type CoordinatorClosureCommand struct {
	Kind        CoordinatorClosureCommandKind
	HolderID    string
	NowUnixNano int64
	Frontiers   CoordinatorDutyFrontiers
}

func CoordinatorDutyName(dutyMask uint32) string {
	switch dutyMask {
	case CoordinatorDutyAllocID:
		return "alloc_id"
	case CoordinatorDutyTSO:
		return "tso"
	case CoordinatorDutyGetRegionByKey:
		return "get_region_by_key"
	case CoordinatorDutyLeaseStart:
		return "lease_start"
	default:
		return fmt.Sprintf("duty_%d", dutyMask)
	}
}

func NewCoordinatorDutyFrontiers(entries ...CoordinatorDutyFrontier) CoordinatorDutyFrontiers {
	var frontiers CoordinatorDutyFrontiers
	for _, entry := range entries {
		frontiers = frontiers.WithFrontier(entry.DutyMask, entry.Frontier)
	}
	return frontiers
}

func CoordinatorDutyFrontiersFromMap(values map[uint32]uint64) CoordinatorDutyFrontiers {
	var frontiers CoordinatorDutyFrontiers
	for dutyMask, frontier := range values {
		frontiers = frontiers.WithFrontier(dutyMask, frontier)
	}
	return frontiers
}

func (f CoordinatorDutyFrontiers) Frontier(dutyMask uint32) uint64 {
	idx, ok := coordinatorDutyFrontierIndex(dutyMask)
	if !ok {
		return 0
	}
	return f.values[idx]
}

func (f CoordinatorDutyFrontiers) Len() int {
	return bits.OnesCount32(f.present & coordinatorDutyFrontierMaskAll)
}

func (f CoordinatorDutyFrontiers) Entries() []CoordinatorDutyFrontier {
	if f.Len() == 0 {
		return nil
	}
	out := make([]CoordinatorDutyFrontier, 0, f.Len())
	for _, dutyMask := range OrderedCoordinatorDutyMasks(0, f) {
		out = append(out, CoordinatorDutyFrontier{
			DutyMask: dutyMask,
			DutyName: CoordinatorDutyName(dutyMask),
			Frontier: f.Frontier(dutyMask),
		})
	}
	return out
}

func (f CoordinatorDutyFrontiers) AsMap() map[uint32]uint64 {
	if f.Len() == 0 {
		return map[uint32]uint64{}
	}
	out := make(map[uint32]uint64, f.Len())
	for _, dutyMask := range OrderedCoordinatorDutyMasks(0, f) {
		out[dutyMask] = f.Frontier(dutyMask)
	}
	return out
}

func (f CoordinatorDutyFrontiers) WithFrontier(dutyMask uint32, frontier uint64) CoordinatorDutyFrontiers {
	idx, ok := coordinatorDutyFrontierIndex(dutyMask)
	if !ok {
		return f
	}
	f.values[idx] = frontier
	f.present |= dutyMask
	return f
}

func CloneDutyFrontiers(frontiers CoordinatorDutyFrontiers) CoordinatorDutyFrontiers {
	return frontiers
}

func OrderedCoordinatorDutyMasks(dutyMask uint32, frontiers CoordinatorDutyFrontiers) []uint32 {
	seen := (frontiers.present | dutyMask) & coordinatorDutyFrontierMaskAll
	out := make([]uint32, 0, bits.OnesCount32(seen))
	for _, mask := range coordinatorDutyOrder {
		if seen&mask == 0 {
			continue
		}
		out = append(out, mask)
	}
	return out
}

func NewContinuationWitness(dutyMask uint32, certGeneration, consumedFrontier uint64) ContinuationWitness {
	return ContinuationWitness{
		DutyMask:         dutyMask,
		DutyName:         CoordinatorDutyName(dutyMask),
		CertGeneration:   certGeneration,
		ConsumedFrontier: consumedFrontier,
	}
}

func NewSuppressedContinuationWitness(dutyMask uint32) ContinuationWitness {
	return ContinuationWitness{
		DutyMask:         dutyMask,
		DutyName:         CoordinatorDutyName(dutyMask),
		CertGeneration:   ContinuationWitnessGenerationSuppressed,
		ConsumedFrontier: 0,
	}
}

func NewAuthorityHandoffRecord(holderID string, expiresUnixNano int64, certGeneration uint64, issuedCursor Cursor, dutyMask uint32, predecessorDigest string, frontiers CoordinatorDutyFrontiers) (AuthorityHandoffRecord, error) {
	holderID = strings.TrimSpace(holderID)
	predecessorDigest = strings.TrimSpace(predecessorDigest)
	if holderID == "" {
		if expiresUnixNano == 0 && certGeneration == 0 && issuedCursor == (Cursor{}) && dutyMask == 0 && predecessorDigest == "" && frontiers.Len() == 0 {
			return AuthorityHandoffRecord{}, nil
		}
		return AuthorityHandoffRecord{}, fmt.Errorf("authority handoff record: holder id is required")
	}
	if certGeneration == 0 {
		return AuthorityHandoffRecord{}, fmt.Errorf("authority handoff record: cert generation is required")
	}
	resolvedDutyMask := dutyMask & coordinatorDutyFrontierMaskAll
	if resolvedDutyMask == 0 {
		return AuthorityHandoffRecord{}, fmt.Errorf("authority handoff record: duty mask is required")
	}
	if frontiers.present&resolvedDutyMask != resolvedDutyMask {
		return AuthorityHandoffRecord{}, fmt.Errorf("authority handoff record: frontiers must cover all duty mask bits")
	}
	return AuthorityHandoffRecord{
		holderID:          holderID,
		expiresUnixNano:   expiresUnixNano,
		certGeneration:    certGeneration,
		issuedCursor:      issuedCursor,
		dutyMask:          resolvedDutyMask,
		predecessorDigest: predecessorDigest,
		frontiers:         CloneDutyFrontiers(frontiers),
	}, nil
}

func MustNewAuthorityHandoffRecord(holderID string, expiresUnixNano int64, certGeneration uint64, issuedCursor Cursor, dutyMask uint32, predecessorDigest string, frontiers CoordinatorDutyFrontiers) AuthorityHandoffRecord {
	record, err := NewAuthorityHandoffRecord(holderID, expiresUnixNano, certGeneration, issuedCursor, dutyMask, predecessorDigest, frontiers)
	if err != nil {
		panic(err)
	}
	return record
}

func (s CoordinatorSuccessorCoverageStatus) Covered() bool {
	for _, check := range s.Checks {
		if !check.Covered {
			return false
		}
	}
	return true
}

func (s CoordinatorSuccessorCoverageStatus) CoveredDuty(dutyMask uint32) bool {
	for _, check := range s.Checks {
		if check.DutyMask == dutyMask {
			return check.Covered
		}
	}
	return true
}

func (s CoordinatorSuccessorCoverageStatus) CoveredDutyMask(dutyMask uint32) bool {
	for _, check := range s.Checks {
		if dutyMask&check.DutyMask == 0 {
			continue
		}
		if !check.Covered {
			return false
		}
	}
	return true
}

func (s CoordinatorSuccessorCoverageStatus) FirstGap() (CoordinatorFrontierCoverage, bool) {
	for _, check := range s.Checks {
		if !check.Covered {
			return check, true
		}
	}
	return CoordinatorFrontierCoverage{}, false
}

func (w ClosureWitness) ClosureSatisfied() bool {
	return w.SealGeneration != 0 &&
		w.SuccessorPresent &&
		w.SuccessorLineageSatisfied &&
		w.SuccessorCoverage.Covered() &&
		w.SealedGenerationRetired
}

func (w ClosureWitness) SuccessorMonotoneCovered() bool {
	return w.SuccessorPresent &&
		w.SuccessorCoverage.CoveredDutyMask(CoordinatorDutyAllocID|CoordinatorDutyTSO)
}

func (w ClosureWitness) SuccessorDescriptorCovered() bool {
	return w.SuccessorPresent &&
		w.SuccessorCoverage.CoveredDutyMask(CoordinatorDutyGetRegionByKey)
}

func (w ClosureWitness) ReplyGenerationLegal(certGeneration uint64) bool {
	if certGeneration == ContinuationWitnessGenerationAttached {
		return true
	}
	if certGeneration == ContinuationWitnessGenerationSuppressed {
		return false
	}
	if w.SealGeneration == 0 {
		return true
	}
	if certGeneration == w.SealGeneration {
		return false
	}
	return w.ClosureSatisfied()
}

func (w ClosureWitness) WithStage(stage CoordinatorClosureStage) ClosureWitness {
	w.Stage = stage
	return w
}

func (r AuthorityHandoffRecord) Present() bool {
	return r.holderID != ""
}

func (r AuthorityHandoffRecord) HolderID() string {
	return r.holderID
}

func (r AuthorityHandoffRecord) ExpiresUnixNano() int64 {
	return r.expiresUnixNano
}

func (r AuthorityHandoffRecord) CertGeneration() uint64 {
	return r.certGeneration
}

func (r AuthorityHandoffRecord) IssuedCursor() Cursor {
	return r.issuedCursor
}

func (r AuthorityHandoffRecord) DutyMask() uint32 {
	return r.dutyMask
}

func (r AuthorityHandoffRecord) PredecessorDigest() string {
	return r.predecessorDigest
}

func (r AuthorityHandoffRecord) Frontiers() CoordinatorDutyFrontiers {
	return CloneDutyFrontiers(r.frontiers)
}

func (w ContinuationWitness) Attached() bool {
	return w.CertGeneration == ContinuationWitnessGenerationAttached
}

func (w ContinuationWitness) Suppressed() bool {
	return w.CertGeneration == ContinuationWitnessGenerationSuppressed
}

func ClosureStageAtLeast(stage, target CoordinatorClosureStage) bool {
	switch target {
	case CoordinatorClosureStageUnspecified:
		return true
	case CoordinatorClosureStagePendingConfirm:
		return stage == CoordinatorClosureStagePendingConfirm ||
			stage == CoordinatorClosureStageConfirmed ||
			stage == CoordinatorClosureStageClosed ||
			stage == CoordinatorClosureStageReattached
	case CoordinatorClosureStageConfirmed:
		return stage == CoordinatorClosureStageConfirmed ||
			stage == CoordinatorClosureStageClosed ||
			stage == CoordinatorClosureStageReattached
	case CoordinatorClosureStageClosed:
		return stage == CoordinatorClosureStageClosed ||
			stage == CoordinatorClosureStageReattached
	case CoordinatorClosureStageReattached:
		return stage == CoordinatorClosureStageReattached
	default:
		return false
	}
}

func coordinatorDutyFrontierIndex(dutyMask uint32) (int, bool) {
	switch dutyMask {
	case CoordinatorDutyAllocID:
		return 0, true
	case CoordinatorDutyTSO:
		return 1, true
	case CoordinatorDutyGetRegionByKey:
		return 2, true
	case CoordinatorDutyLeaseStart:
		return 3, true
	default:
		return 0, false
	}
}
