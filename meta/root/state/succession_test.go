package state_test

import (
	"errors"
	"testing"

	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

func TestValidateLeaseStartInheritanceRejectsBelowFrontier(t *testing.T) {
	seal := rootstate.Legacy{
		HolderID:       "n1",
		Epoch: 1,
		Mandate:       rootproto.MandateDefault,
		Frontiers: rootproto.NewMandateFrontiers(
			rootproto.MandateFrontier{Mandate: rootproto.MandateLeaseStart, Frontier: 15},
		),
	}

	if err := rootstate.ValidateLeaseStartInheritance(seal, 15); !errors.Is(err, rootstate.ErrInheritance) {
		t.Fatalf("lease_start=15 must be rejected (not strictly greater than frontier), got err=%v", err)
	}
	if err := rootstate.ValidateLeaseStartInheritance(seal, 10); !errors.Is(err, rootstate.ErrInheritance) {
		t.Fatalf("lease_start=10 must be rejected, got err=%v", err)
	}
	if err := rootstate.ValidateLeaseStartInheritance(seal, 16); err != nil {
		t.Fatalf("lease_start=16 must be accepted, got err=%v", err)
	}
}

func TestValidateLeaseStartInheritanceNoOpWhenSealAbsent(t *testing.T) {
	if err := rootstate.ValidateLeaseStartInheritance(rootstate.Legacy{}, 0); err != nil {
		t.Fatalf("empty seal must be a no-op, got err=%v", err)
	}
}

func TestValidateLeaseStartInheritanceNoOpWhenFrontierZero(t *testing.T) {
	seal := rootstate.Legacy{HolderID: "n1", Epoch: 1}
	if err := rootstate.ValidateLeaseStartInheritance(seal, 0); err != nil {
		t.Fatalf("zero frontier must be a no-op, got err=%v", err)
	}
}

func TestLegacyWithServedFrontierMonotonic(t *testing.T) {
	seal := rootstate.Legacy{HolderID: "n1", Epoch: 1}

	updated := rootstate.LegacyWithServedFrontier(seal, 10)
	if got := updated.Frontiers.Frontier(rootproto.MandateLeaseStart); got != 10 {
		t.Fatalf("frontier must be 10, got %d", got)
	}

	bumped := rootstate.LegacyWithServedFrontier(updated, 15)
	if got := bumped.Frontiers.Frontier(rootproto.MandateLeaseStart); got != 15 {
		t.Fatalf("frontier must be 15, got %d", got)
	}

	lowered := rootstate.LegacyWithServedFrontier(bumped, 5)
	if got := lowered.Frontiers.Frontier(rootproto.MandateLeaseStart); got != 15 {
		t.Fatalf("frontier must stay at 15 after attempting to lower, got %d", got)
	}
}

func TestNextTenureEpoch(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 1_000,
		Epoch:  7,
	}
	seal := rootstate.Legacy{HolderID: "c1", Epoch: 7}

	if got := rootstate.NextTenureEpoch(rootstate.Tenure{}, rootstate.Legacy{}, "c1", 100); got != 1 {
		t.Fatalf("empty lease should start at generation 1, got %d", got)
	}
	if got := rootstate.NextTenureEpoch(current, rootstate.Legacy{}, "c1", 500); got != 7 {
		t.Fatalf("active same-holder lease should keep generation 7, got %d", got)
	}
	if got := rootstate.NextTenureEpoch(current, rootstate.Legacy{}, "c2", 500); got != 8 {
		t.Fatalf("new holder should bump generation to 8, got %d", got)
	}
	if got := rootstate.NextTenureEpoch(current, rootstate.Legacy{}, "c1", 1_000); got != 8 {
		t.Fatalf("expired lease should bump generation to 8, got %d", got)
	}
	if got := rootstate.NextTenureEpoch(current, seal, "c1", 500); got != 8 {
		t.Fatalf("sealed lease should bump generation to 8, got %d", got)
	}
}

func TestTenureRenewableAndSealedGeneration(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 1_000,
		Epoch:  7,
	}
	seal := rootstate.Legacy{HolderID: "c1", Epoch: 7}

	if rootstate.TenureSealed(current, rootstate.Legacy{}) {
		t.Fatalf("unsealed generation must not be marked sealed")
	}
	if !rootstate.TenureSealed(current, seal) {
		t.Fatalf("matching seal must mark generation sealed")
	}
	if !rootstate.TenureRenewable(current, rootstate.Legacy{}, "c1", 500) {
		t.Fatalf("active same-holder lease should be continuable")
	}
	if rootstate.TenureRenewable(current, seal, "c1", 500) {
		t.Fatalf("sealed lease must not be continuable")
	}
}

func TestValidateInheritance(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 1_000,
		Epoch:  7,
	}
	seal := rootstate.Legacy{
		HolderID:       "c1",
		Epoch: 7,
		Mandate:       rootproto.MandateDefault,
		Frontiers:      succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
	}

	if err := rootstate.ValidateInheritance(current, rootstate.Legacy{}, rootproto.MandateFrontiers{}); err != nil {
		t.Fatalf("empty seal must not require coverage, got err=%v", err)
	}
	if err := rootstate.ValidateInheritance(current, seal, succession.Frontiers(rootstate.State{IDFence: 19, TSOFence: 40}, 60)); !errors.Is(err, rootstate.ErrInheritance) {
		t.Fatalf("alloc_id gap must be rejected, got err=%v", err)
	}
	if err := rootstate.ValidateInheritance(current, seal, succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 39}, 60)); !errors.Is(err, rootstate.ErrInheritance) {
		t.Fatalf("tso gap must be rejected, got err=%v", err)
	}
	if err := rootstate.ValidateInheritance(current, seal, succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 59)); !errors.Is(err, rootstate.ErrInheritance) {
		t.Fatalf("descriptor gap must be rejected, got err=%v", err)
	}
	if err := rootstate.ValidateInheritance(current, seal, succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60)); err != nil {
		t.Fatalf("exact coverage must be accepted, got err=%v", err)
	}
	if err := rootstate.ValidateInheritance(current, seal, succession.Frontiers(rootstate.State{IDFence: 25, TSOFence: 45}, 65)); err != nil {
		t.Fatalf("higher coverage must be accepted, got err=%v", err)
	}

	coverage := rootstate.EvaluateInheritance(current, seal, succession.Frontiers(rootstate.State{IDFence: 25, TSOFence: 45}, 65))
	if len(coverage.Checks) != 3 {
		t.Fatalf("expected 3 coverage checks, got %d", len(coverage.Checks))
	}
	if !coverage.Covered() {
		t.Fatalf("coverage should be satisfied")
	}
	if coverage.Checks[0].Mandate != rootproto.MandateAllocID || coverage.Checks[0].RequiredFrontier != 20 || coverage.Checks[0].ActualFrontier != 25 {
		t.Fatalf("unexpected alloc_id coverage check: %+v", coverage.Checks[0])
	}

	maskedSeal := seal
	maskedSeal.Mandate = rootproto.MandateAllocID | rootproto.MandateTSO
	maskedCoverage := rootstate.EvaluateInheritance(current, maskedSeal, succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 1))
	if len(maskedCoverage.Checks) != 2 {
		t.Fatalf("expected 2 masked checks, got %d", len(maskedCoverage.Checks))
	}
	if !maskedCoverage.CoveredMandate(rootproto.MandateGetRegionByKey) {
		t.Fatalf("masked-out duty should be treated as covered")
	}
}

func TestValidateTenureClaimLineage(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:          "c1",
		ExpiresUnixNano:   1_000,
		Epoch:    7,
		LineageDigest: "prev",
	}
	seal := rootstate.Legacy{
		HolderID:       "c1",
		Epoch: 7,
		Mandate:       rootproto.MandateDefault,
		Frontiers:      succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
		SealedAt: rootstate.Cursor{Term: 1, Index: 9},
	}

	if err := rootstate.ValidateTenureClaim(current, rootstate.Legacy{}, "c1", "prev", 1_100, 500); err != nil {
		t.Fatalf("continuation without seal should be accepted, got err=%v", err)
	}
	if err := rootstate.ValidateTenureClaim(current, rootstate.Legacy{}, "c1", "", 1_100, 500); !errors.Is(err, rootstate.ErrInheritance) {
		t.Fatalf("missing predecessor digest must be rejected, got err=%v", err)
	}

	expected := rootstate.DigestOfLegacy(seal)
	if err := rootstate.ValidateTenureClaim(current, seal, "c1", expected, 1_100, 500); err != nil {
		t.Fatalf("matching seal digest should be accepted, got err=%v", err)
	}
	if err := rootstate.ValidateTenureClaim(current, seal, "c1", "", 1_100, 500); !errors.Is(err, rootstate.ErrInheritance) {
		t.Fatalf("missing seal digest must be rejected, got err=%v", err)
	}
}

func TestDigestOfLegacyIncludesNonMaskedLeaseStartFrontier(t *testing.T) {
	base := rootstate.Legacy{
		HolderID:       "c1",
		Epoch: 7,
		Mandate:       rootproto.MandateDefault,
		Frontiers:      succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
		SealedAt: rootstate.Cursor{Term: 1, Index: 9},
	}
	withLeaseStart := base
	withLeaseStart.Frontiers = withLeaseStart.Frontiers.WithFrontier(rootproto.MandateLeaseStart, 105)

	baseDigest := rootstate.DigestOfLegacy(base)
	leaseStartDigest := rootstate.DigestOfLegacy(withLeaseStart)
	if baseDigest == leaseStartDigest {
		t.Fatalf("lease_start frontier must affect seal digest")
	}
}
