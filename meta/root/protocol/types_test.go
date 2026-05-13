package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthorityGrantPresenceAndDutyLookup(t *testing.T) {
	grant := AuthorityGrant{}
	require.False(t, grant.Present())

	grant = AuthorityGrant{
		GrantID:         "g1",
		HolderID:        "c1",
		Era:             1,
		ExpiresUnixNano: 2_000,
		Duties: []DutyGrant{
			NewGlobalMonotoneDuty(DutyAllocID, 10),
			NewGlobalVersionDuty(DutyRegionLookup, AuthorityRootToken{Term: 1, Index: 2}, 7, 0),
		},
	}
	require.True(t, grant.Present())
	require.True(t, grant.ActiveAt(1_999))
	require.False(t, grant.ActiveAt(2_000))

	alloc, ok := grant.Duty(DutyAllocID)
	require.True(t, ok)
	require.Equal(t, DutyBoundMonotone, alloc.Bound.Kind)
	require.Equal(t, uint64(10), alloc.Bound.MonotoneUpper)

	_, ok = grant.Duty(DutyTSO)
	require.False(t, ok)
}

func TestGrantRetirementPresence(t *testing.T) {
	require.False(t, GrantRetirement{}.Present())
	require.False(t, GrantRetirement{GrantID: "g1", Era: 1}.Present())
	require.True(t, GrantRetirement{GrantID: "g1", Era: 1, Mode: GrantRetirementExpiredBound}.Present())
}

func TestDutyNameAndAuthorityEraConstants(t *testing.T) {
	require.Equal(t, "alloc_id", DutyName(DutyAllocID))
	require.Equal(t, "custom", DutyName(DutyID("custom")))
	require.Equal(t, "unspecified", DutyName(""))
	require.Equal(t, uint64(0), AuthorityEraAttached)
	require.Equal(t, ^uint64(0), AuthorityEraSuppressed)
}

// TestLegacyRetiredEraFloorExpandsAcrossRegisteredGlobalDuties locks the upgrade
// contract for old checkpoints: their aggregate floor remains conservative for
// every currently registered global duty.
func TestLegacyRetiredEraFloorExpandsAcrossRegisteredGlobalDuties(t *testing.T) {
	global := DutyScope{Kind: DutyScopeGlobal}
	floors := AuthorityRetiredEraFloorsFromLegacyFloor(9)

	for _, spec := range RegisteredDutySpecs() {
		if spec.ScopeKind != DutyScopeGlobal {
			continue
		}
		require.Equal(t, uint64(9), AuthorityRetiredEraFloorFor(floors, spec.ID, global), spec.ID)
	}
	require.Zero(t, AuthorityRetiredEraFloorFor(floors, DutyID("unknown"), global))
}

// TestNormalizeAuthorityRetiredEraFloorsDoesNotOverlayLegacyWhenScopedFloorsExist
// ensures new scoped states are not polluted by the legacy aggregate floor.
func TestNormalizeAuthorityRetiredEraFloorsDoesNotOverlayLegacyWhenScopedFloorsExist(t *testing.T) {
	global := DutyScope{Kind: DutyScopeGlobal}
	original := []AuthorityRetiredEraFloor{{
		DutyID:          DutyAllocID,
		Scope:           global,
		RetiredEraFloor: 7,
	}}

	normalized := NormalizeAuthorityRetiredEraFloors(original, 22)

	require.Equal(t, uint64(7), AuthorityRetiredEraFloorFor(normalized, DutyAllocID, global))
	require.Zero(t, AuthorityRetiredEraFloorFor(normalized, DutyTSO, global))
	normalized[0].RetiredEraFloor = 100
	require.Equal(t, uint64(7), original[0].RetiredEraFloor)
}
