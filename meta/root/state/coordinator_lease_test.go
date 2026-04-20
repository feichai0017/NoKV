package state_test

import (
	"errors"
	"testing"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

func TestValidateCoordinatorLeaseStartCoverageRejectsBelowFrontier(t *testing.T) {
	seal := rootstate.CoordinatorSeal{
		HolderID:       "n1",
		CertGeneration: 1,
		DutyMask:       rootstate.CoordinatorDutyMaskDefault,
		Frontiers: rootstate.NewCoordinatorDutyFrontiers(
			rootstate.CoordinatorDutyFrontier{DutyMask: rootstate.CoordinatorDutyLeaseStart, Frontier: 15},
		),
	}

	if err := rootstate.ValidateCoordinatorLeaseStartCoverage(seal, 15); !errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage) {
		t.Fatalf("lease_start=15 must be rejected (not strictly greater than frontier), got err=%v", err)
	}
	if err := rootstate.ValidateCoordinatorLeaseStartCoverage(seal, 10); !errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage) {
		t.Fatalf("lease_start=10 must be rejected, got err=%v", err)
	}
	if err := rootstate.ValidateCoordinatorLeaseStartCoverage(seal, 16); err != nil {
		t.Fatalf("lease_start=16 must be accepted, got err=%v", err)
	}
}

func TestValidateCoordinatorLeaseStartCoverageNoOpWhenSealAbsent(t *testing.T) {
	if err := rootstate.ValidateCoordinatorLeaseStartCoverage(rootstate.CoordinatorSeal{}, 0); err != nil {
		t.Fatalf("empty seal must be a no-op, got err=%v", err)
	}
}

func TestValidateCoordinatorLeaseStartCoverageNoOpWhenFrontierZero(t *testing.T) {
	seal := rootstate.CoordinatorSeal{HolderID: "n1", CertGeneration: 1}
	if err := rootstate.ValidateCoordinatorLeaseStartCoverage(seal, 0); err != nil {
		t.Fatalf("zero frontier must be a no-op, got err=%v", err)
	}
}

func TestCoordinatorSealWithServedFrontierMonotonic(t *testing.T) {
	seal := rootstate.CoordinatorSeal{HolderID: "n1", CertGeneration: 1}

	updated := rootstate.CoordinatorSealWithServedFrontier(seal, 10)
	if got := updated.Frontiers.Frontier(rootstate.CoordinatorDutyLeaseStart); got != 10 {
		t.Fatalf("frontier must be 10, got %d", got)
	}

	bumped := rootstate.CoordinatorSealWithServedFrontier(updated, 15)
	if got := bumped.Frontiers.Frontier(rootstate.CoordinatorDutyLeaseStart); got != 15 {
		t.Fatalf("frontier must be 15, got %d", got)
	}

	lowered := rootstate.CoordinatorSealWithServedFrontier(bumped, 5)
	if got := lowered.Frontiers.Frontier(rootstate.CoordinatorDutyLeaseStart); got != 15 {
		t.Fatalf("frontier must stay at 15 after attempting to lower, got %d", got)
	}
}

func TestNextCoordinatorLeaseGeneration(t *testing.T) {
	current := rootstate.CoordinatorLease{
		HolderID:        "c1",
		ExpiresUnixNano: 1_000,
		CertGeneration:  7,
	}
	seal := rootstate.CoordinatorSeal{HolderID: "c1", CertGeneration: 7}

	if got := rootstate.NextCoordinatorLeaseGeneration(rootstate.CoordinatorLease{}, rootstate.CoordinatorSeal{}, "c1", 100); got != 1 {
		t.Fatalf("empty lease should start at generation 1, got %d", got)
	}
	if got := rootstate.NextCoordinatorLeaseGeneration(current, rootstate.CoordinatorSeal{}, "c1", 500); got != 7 {
		t.Fatalf("active same-holder lease should keep generation 7, got %d", got)
	}
	if got := rootstate.NextCoordinatorLeaseGeneration(current, rootstate.CoordinatorSeal{}, "c2", 500); got != 8 {
		t.Fatalf("new holder should bump generation to 8, got %d", got)
	}
	if got := rootstate.NextCoordinatorLeaseGeneration(current, rootstate.CoordinatorSeal{}, "c1", 1_000); got != 8 {
		t.Fatalf("expired lease should bump generation to 8, got %d", got)
	}
	if got := rootstate.NextCoordinatorLeaseGeneration(current, seal, "c1", 500); got != 8 {
		t.Fatalf("sealed lease should bump generation to 8, got %d", got)
	}
}

func TestCoordinatorLeaseContinuableAndSealedGeneration(t *testing.T) {
	current := rootstate.CoordinatorLease{
		HolderID:        "c1",
		ExpiresUnixNano: 1_000,
		CertGeneration:  7,
	}
	seal := rootstate.CoordinatorSeal{HolderID: "c1", CertGeneration: 7}

	if rootstate.CoordinatorGenerationSealed(current, rootstate.CoordinatorSeal{}) {
		t.Fatalf("unsealed generation must not be marked sealed")
	}
	if !rootstate.CoordinatorGenerationSealed(current, seal) {
		t.Fatalf("matching seal must mark generation sealed")
	}
	if !rootstate.CoordinatorLeaseContinuable(current, rootstate.CoordinatorSeal{}, "c1", 500) {
		t.Fatalf("active same-holder lease should be continuable")
	}
	if rootstate.CoordinatorLeaseContinuable(current, seal, "c1", 500) {
		t.Fatalf("sealed lease must not be continuable")
	}
}

func TestValidateCoordinatorLeaseSuccessorCoverageFrontiers(t *testing.T) {
	current := rootstate.CoordinatorLease{
		HolderID:        "c1",
		ExpiresUnixNano: 1_000,
		CertGeneration:  7,
	}
	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 7,
		DutyMask:       rootstate.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(20, 40, 60),
	}

	if err := rootstate.ValidateCoordinatorLeaseSuccessorCoverageFrontiers(current, rootstate.CoordinatorSeal{}, rootstate.CoordinatorDutyFrontiers{}); err != nil {
		t.Fatalf("empty seal must not require coverage, got err=%v", err)
	}
	if err := rootstate.ValidateCoordinatorLeaseSuccessorCoverageFrontiers(current, seal, controlplane.Frontiers(19, 40, 60)); !errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage) {
		t.Fatalf("alloc_id gap must be rejected, got err=%v", err)
	}
	if err := rootstate.ValidateCoordinatorLeaseSuccessorCoverageFrontiers(current, seal, controlplane.Frontiers(20, 39, 60)); !errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage) {
		t.Fatalf("tso gap must be rejected, got err=%v", err)
	}
	if err := rootstate.ValidateCoordinatorLeaseSuccessorCoverageFrontiers(current, seal, controlplane.Frontiers(20, 40, 59)); !errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage) {
		t.Fatalf("descriptor gap must be rejected, got err=%v", err)
	}
	if err := rootstate.ValidateCoordinatorLeaseSuccessorCoverageFrontiers(current, seal, controlplane.Frontiers(20, 40, 60)); err != nil {
		t.Fatalf("exact coverage must be accepted, got err=%v", err)
	}
	if err := rootstate.ValidateCoordinatorLeaseSuccessorCoverageFrontiers(current, seal, controlplane.Frontiers(25, 45, 65)); err != nil {
		t.Fatalf("higher coverage must be accepted, got err=%v", err)
	}

	coverage := rootstate.EvaluateCoordinatorLeaseSuccessorCoverage(current, seal, controlplane.Frontiers(25, 45, 65))
	if len(coverage.Checks) != 3 {
		t.Fatalf("expected 3 coverage checks, got %d", len(coverage.Checks))
	}
	if !coverage.Covered() {
		t.Fatalf("coverage should be satisfied")
	}
	if coverage.Checks[0].DutyName != "alloc_id" || coverage.Checks[0].RequiredFrontier != 20 || coverage.Checks[0].ActualFrontier != 25 {
		t.Fatalf("unexpected alloc_id coverage check: %+v", coverage.Checks[0])
	}

	maskedSeal := seal
	maskedSeal.DutyMask = rootstate.CoordinatorDutyAllocID | rootstate.CoordinatorDutyTSO
	maskedCoverage := rootstate.EvaluateCoordinatorLeaseSuccessorCoverage(current, maskedSeal, controlplane.Frontiers(20, 40, 1))
	if len(maskedCoverage.Checks) != 2 {
		t.Fatalf("expected 2 masked checks, got %d", len(maskedCoverage.Checks))
	}
	if !maskedCoverage.CoveredDuty(rootstate.CoordinatorDutyGetRegionByKey) {
		t.Fatalf("masked-out duty should be treated as covered")
	}
}

func TestValidateCoordinatorLeaseCampaignLineage(t *testing.T) {
	current := rootstate.CoordinatorLease{
		HolderID:          "c1",
		ExpiresUnixNano:   1_000,
		CertGeneration:    7,
		PredecessorDigest: "prev",
	}
	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 7,
		DutyMask:       rootstate.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(20, 40, 60),
		SealedAtCursor: rootstate.Cursor{Term: 1, Index: 9},
	}

	if err := rootstate.ValidateCoordinatorLeaseCampaign(current, rootstate.CoordinatorSeal{}, "c1", "prev", 1_100, 500); err != nil {
		t.Fatalf("continuation without seal should be accepted, got err=%v", err)
	}
	if err := rootstate.ValidateCoordinatorLeaseCampaign(current, rootstate.CoordinatorSeal{}, "c1", "", 1_100, 500); !errors.Is(err, rootstate.ErrCoordinatorLeaseLineage) {
		t.Fatalf("missing predecessor digest must be rejected, got err=%v", err)
	}

	expected := rootstate.CoordinatorSealDigest(seal)
	if err := rootstate.ValidateCoordinatorLeaseCampaign(current, seal, "c1", expected, 1_100, 500); err != nil {
		t.Fatalf("matching seal digest should be accepted, got err=%v", err)
	}
	if err := rootstate.ValidateCoordinatorLeaseCampaign(current, seal, "c1", "", 1_100, 500); !errors.Is(err, rootstate.ErrCoordinatorLeaseLineage) {
		t.Fatalf("missing seal digest must be rejected, got err=%v", err)
	}
}

func TestCoordinatorSealDigestIncludesNonMaskedLeaseStartFrontier(t *testing.T) {
	base := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 7,
		DutyMask:       rootstate.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(20, 40, 60),
		SealedAtCursor: rootstate.Cursor{Term: 1, Index: 9},
	}
	withLeaseStart := base
	withLeaseStart.Frontiers = withLeaseStart.Frontiers.WithFrontier(rootstate.CoordinatorDutyLeaseStart, 105)

	baseDigest := rootstate.CoordinatorSealDigest(base)
	leaseStartDigest := rootstate.CoordinatorSealDigest(withLeaseStart)
	if baseDigest == leaseStartDigest {
		t.Fatalf("lease_start frontier must affect seal digest")
	}
}
