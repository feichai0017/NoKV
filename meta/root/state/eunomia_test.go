// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestEunomiaProjectionCarriesGrantLifecycle(t *testing.T) {
	st := rootstate.State{
		ActiveGrants: []rootproto.AuthorityGrant{{
			GrantID:  "c2/2",
			HolderID: "c2",
			Era:      2,
			Duties: []rootproto.DutyGrant{
				rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20),
			},
		}},
		RetiredGrants: []rootproto.GrantRetirement{
			{
				GrantID: "c1/1",
				Era:     1,
				Mode:    rootproto.GrantRetirementExpiredBound,
				Bounds: []rootproto.DutyGrant{
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10),
				},
			},
		},
		GrantInheritances: []rootproto.GrantInheritance{
			{PredecessorGrantID: "c1/1", SuccessorGrantID: "c2/2"},
		},
	}

	projected := st.Eunomia()

	require.Len(t, projected.ActiveGrants, 1)
	require.True(t, projected.ActiveGrants[0].Present())
	require.Equal(t, "c2/2", projected.ActiveGrants[0].GrantID)
	require.Len(t, projected.RetiredGrants, 1)
	require.Equal(t, rootproto.GrantRetirementExpiredBound, projected.RetiredGrants[0].Mode)
	require.Len(t, projected.GrantInheritances, 1)
	require.Equal(t, "c2/2", projected.GrantInheritances[0].SuccessorGrantID)
}

// TestGrantInheritanceAdvancesRetiredEraFloorByDuty proves that inheriting an
// alloc_id grant advances alloc_id finality without also retiring TSO.
func TestGrantInheritanceAdvancesRetiredEraFloorByDuty(t *testing.T) {
	state := rootstate.State{}
	rootstate.ApplyEventToState(&state, rootproto.Cursor{Term: 1, Index: 1}, rootevent.GrantRetired(rootproto.GrantRetirement{
		GrantID:  "c0/22",
		HolderID: "c0",
		Era:      22,
		Bounds:   []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 100)},
	}))
	rootstate.ApplyEventToState(&state, rootproto.Cursor{Term: 1, Index: 2}, rootevent.GrantInherited(rootproto.GrantInheritance{
		PredecessorGrantID: "c0/22",
		SuccessorGrantID:   "c1/23",
	}))

	global := rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}
	require.Equal(t, uint64(22), rootproto.AuthorityRetiredEraFloorFor(state.RetiredEraFloors, rootproto.DutyAllocID, global))
	require.Zero(t, rootproto.AuthorityRetiredEraFloorFor(state.RetiredEraFloors, rootproto.DutyTSO, global))
	require.Equal(t, state.RetiredEraFloors, state.Eunomia().RetiredEraFloors)
}

// TestGrantInheritanceAdvancesEachCoveredDutyFloor covers multi-duty grants: an
// inherited grant advances a separate compact floor for every duty it covered.
func TestGrantInheritanceAdvancesEachCoveredDutyFloor(t *testing.T) {
	state := rootstate.State{}
	rootstate.ApplyEventToState(&state, rootproto.Cursor{Term: 1, Index: 1}, rootevent.GrantRetired(rootproto.GrantRetirement{
		GrantID:  "c0/7",
		HolderID: "c0",
		Era:      7,
		Bounds: []rootproto.DutyGrant{
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 100),
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 200),
			rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 9, 0),
		},
	}))
	rootstate.ApplyEventToState(&state, rootproto.Cursor{Term: 1, Index: 2}, rootevent.GrantInherited(rootproto.GrantInheritance{
		PredecessorGrantID: "c0/7",
		SuccessorGrantID:   "c1/8",
	}))

	global := rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}
	require.Equal(t, uint64(7), rootproto.AuthorityRetiredEraFloorFor(state.RetiredEraFloors, rootproto.DutyAllocID, global))
	require.Equal(t, uint64(7), rootproto.AuthorityRetiredEraFloorFor(state.RetiredEraFloors, rootproto.DutyTSO, global))
	require.Equal(t, uint64(7), rootproto.AuthorityRetiredEraFloorFor(state.RetiredEraFloors, rootproto.DutyRegionLookup, global))
}

// TestCompactEunomiaStateUsesScopedFinalityFloors protects Finality during GC:
// compaction may drop alloc_id history after its floor advances, but must retain
// TSO history whose own floor has not advanced.
func TestCompactEunomiaStateUsesScopedFinalityFloors(t *testing.T) {
	global := rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}
	allocRetirement := rootproto.GrantRetirement{
		GrantID:            "alloc/22",
		HolderID:           "coord-a",
		Era:                22,
		Mode:               rootproto.GrantRetirementExpiredBound,
		Bounds:             []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 100)},
		InheritedByGrantID: "alloc/23",
	}
	tsoRetirement := rootproto.GrantRetirement{
		GrantID:            "tso/21",
		HolderID:           "coord-b",
		Era:                21,
		Mode:               rootproto.GrantRetirementExpiredBound,
		Bounds:             []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 200)},
		InheritedByGrantID: "tso/22",
	}
	state := rootstate.State{
		RetiredEraFloors: []rootproto.AuthorityRetiredEraFloor{{
			DutyID:          rootproto.DutyAllocID,
			Scope:           global,
			RetiredEraFloor: 22,
		}},
		RetiredGrants: []rootproto.GrantRetirement{
			allocRetirement,
			tsoRetirement,
		},
		GrantInheritances: []rootproto.GrantInheritance{
			{PredecessorGrantID: allocRetirement.GrantID, SuccessorGrantID: allocRetirement.InheritedByGrantID},
			{PredecessorGrantID: tsoRetirement.GrantID, SuccessorGrantID: tsoRetirement.InheritedByGrantID},
		},
	}

	compacted := rootstate.CompactEunomiaState(state)

	require.Equal(t, []rootproto.GrantRetirement{tsoRetirement}, compacted.RetiredGrants)
	require.Equal(t, []rootproto.GrantInheritance{{
		PredecessorGrantID: tsoRetirement.GrantID,
		SuccessorGrantID:   tsoRetirement.InheritedByGrantID,
	}}, compacted.GrantInheritances)
}

// TestCompactEunomiaStateKeepsMultiDutyRetirementUntilAllScopedFloorsCoverIt
// prevents partial GC of a multi-duty retirement; all covered duty/scope floors
// must reach the retired era first.
func TestCompactEunomiaStateKeepsMultiDutyRetirementUntilAllScopedFloorsCoverIt(t *testing.T) {
	global := rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}
	retirement := rootproto.GrantRetirement{
		GrantID:  "multi/9",
		HolderID: "coord-a",
		Era:      9,
		Mode:     rootproto.GrantRetirementExpiredBound,
		Bounds: []rootproto.DutyGrant{
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 100),
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 200),
		},
		InheritedByGrantID: "multi/10",
	}
	state := rootstate.State{
		RetiredEraFloors: []rootproto.AuthorityRetiredEraFloor{{
			DutyID:          rootproto.DutyAllocID,
			Scope:           global,
			RetiredEraFloor: 9,
		}},
		RetiredGrants: []rootproto.GrantRetirement{retirement},
		GrantInheritances: []rootproto.GrantInheritance{{
			PredecessorGrantID: retirement.GrantID,
			SuccessorGrantID:   retirement.InheritedByGrantID,
		}},
	}

	compacted := rootstate.CompactEunomiaState(state)
	require.Equal(t, []rootproto.GrantRetirement{retirement}, compacted.RetiredGrants)
	require.Len(t, compacted.GrantInheritances, 1)

	state.RetiredEraFloors = rootproto.AdvanceAuthorityRetiredEraFloor(
		state.RetiredEraFloors,
		rootproto.DutyTSO,
		global,
		9,
	)

	compacted = rootstate.CompactEunomiaState(state)
	require.Empty(t, compacted.RetiredGrants)
	require.Empty(t, compacted.GrantInheritances)
}

// TestCompactEunomiaStateUsesScopedFloors ensures scoped floors alone compact
// final history.
func TestCompactEunomiaStateUsesScopedFloors(t *testing.T) {
	global := rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}
	retirement := rootproto.GrantRetirement{
		GrantID:            "alloc/7",
		HolderID:           "coord-a",
		Era:                7,
		Mode:               rootproto.GrantRetirementExpiredBound,
		Bounds:             []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 100)},
		InheritedByGrantID: "alloc/8",
	}
	state := rootstate.State{
		RetiredEraFloors: []rootproto.AuthorityRetiredEraFloor{{
			DutyID:          rootproto.DutyAllocID,
			Scope:           global,
			RetiredEraFloor: 7,
		}},
		RetiredGrants: []rootproto.GrantRetirement{retirement},
		GrantInheritances: []rootproto.GrantInheritance{{
			PredecessorGrantID: retirement.GrantID,
			SuccessorGrantID:   retirement.InheritedByGrantID,
		}},
	}

	compacted := rootstate.CompactEunomiaState(state)

	require.Empty(t, compacted.RetiredGrants)
	require.Empty(t, compacted.GrantInheritances)
}
