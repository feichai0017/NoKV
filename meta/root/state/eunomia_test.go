package state_test

import (
	"testing"

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
