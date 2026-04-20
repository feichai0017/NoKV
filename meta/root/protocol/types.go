package protocol

import (
	"fmt"
	"sort"
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

// CoordinatorDutyFrontiers is the protocol-level duty frontier algebra carried
// across rooted handoff, verifier, and audit boundaries.
type CoordinatorDutyFrontiers struct {
	values map[uint32]uint64
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
	HolderID          string
	ExpiresUnixNano   int64
	CertGeneration    uint64
	IssuedCursor      Cursor
	DutyMask          uint32
	PredecessorDigest string
	Frontiers         CoordinatorDutyFrontiers
}

type ContinuationWitness struct {
	DutyMask         uint32
	DutyName         string
	CertGeneration   uint64
	ConsumedFrontier uint64
}

type CoordinatorClosureStage uint8

const (
	CoordinatorClosureStagePendingConfirm CoordinatorClosureStage = iota
	CoordinatorClosureStageConfirmed
	CoordinatorClosureStageClosed
	CoordinatorClosureStageReattached
)

func (s CoordinatorClosureStage) String() string {
	switch s {
	case CoordinatorClosureStageConfirmed:
		return "confirmed"
	case CoordinatorClosureStageClosed:
		return "closed"
	case CoordinatorClosureStageReattached:
		return "reattached"
	default:
		return "pending_confirm"
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
	if len(entries) == 0 {
		return CoordinatorDutyFrontiers{values: map[uint32]uint64{}}
	}
	values := make(map[uint32]uint64, len(entries))
	for _, entry := range entries {
		if !validCoordinatorDutyFrontierMask(entry.DutyMask) {
			continue
		}
		values[entry.DutyMask] = entry.Frontier
	}
	return CoordinatorDutyFrontiers{values: values}
}

func CoordinatorDutyFrontiersFromMap(values map[uint32]uint64) CoordinatorDutyFrontiers {
	if len(values) == 0 {
		return CoordinatorDutyFrontiers{values: map[uint32]uint64{}}
	}
	entries := make([]CoordinatorDutyFrontier, 0, len(values))
	for dutyMask, frontier := range values {
		entries = append(entries, CoordinatorDutyFrontier{
			DutyMask: dutyMask,
			DutyName: CoordinatorDutyName(dutyMask),
			Frontier: frontier,
		})
	}
	return NewCoordinatorDutyFrontiers(entries...)
}

func (f CoordinatorDutyFrontiers) Frontier(dutyMask uint32) uint64 {
	if len(f.values) == 0 || !validCoordinatorDutyFrontierMask(dutyMask) {
		return 0
	}
	return f.values[dutyMask]
}

func (f CoordinatorDutyFrontiers) Len() int {
	return len(f.values)
}

func (f CoordinatorDutyFrontiers) Entries() []CoordinatorDutyFrontier {
	if len(f.values) == 0 {
		return nil
	}
	out := make([]CoordinatorDutyFrontier, 0, len(f.values))
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
	if len(f.values) == 0 {
		return map[uint32]uint64{}
	}
	out := make(map[uint32]uint64, len(f.values))
	for dutyMask, frontier := range f.values {
		out[dutyMask] = frontier
	}
	return out
}

func (f CoordinatorDutyFrontiers) WithFrontier(dutyMask uint32, frontier uint64) CoordinatorDutyFrontiers {
	values := make(map[uint32]uint64, len(f.values)+1)
	for mask, current := range f.values {
		values[mask] = current
	}
	if validCoordinatorDutyFrontierMask(dutyMask) {
		values[dutyMask] = frontier
	}
	return CoordinatorDutyFrontiers{values: values}
}

func CloneDutyFrontiers(frontiers CoordinatorDutyFrontiers) CoordinatorDutyFrontiers {
	if len(frontiers.values) == 0 {
		return CoordinatorDutyFrontiers{values: map[uint32]uint64{}}
	}
	values := make(map[uint32]uint64, len(frontiers.values))
	for dutyMask, frontier := range frontiers.values {
		values[dutyMask] = frontier
	}
	return CoordinatorDutyFrontiers{values: values}
}

func OrderedCoordinatorDutyMasks(dutyMask uint32, frontiers CoordinatorDutyFrontiers) []uint32 {
	seen := make(map[uint32]struct{}, len(frontiers.values)+4)
	for mask := range frontiers.values {
		if !validCoordinatorDutyFrontierMask(mask) {
			continue
		}
		seen[mask] = struct{}{}
	}
	for bit := uint32(1); bit != 0; bit <<= 1 {
		if dutyMask&bit != 0 {
			seen[bit] = struct{}{}
		}
	}
	out := make([]uint32, 0, len(seen))
	for mask := range seen {
		out = append(out, mask)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
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
	if certGeneration == 0 {
		return true
	}
	if w.SealGeneration == 0 {
		return true
	}
	if certGeneration == w.SealGeneration {
		return false
	}
	return w.ClosureSatisfied()
}

func validCoordinatorDutyFrontierMask(dutyMask uint32) bool {
	return dutyMask != 0 && dutyMask&(dutyMask-1) == 0
}
