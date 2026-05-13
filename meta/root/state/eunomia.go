package state

import rootproto "github.com/feichai0017/NoKV/meta/root/protocol"

// EunomiaState is the grant-lifecycle slice of root truth returned by ApplyGrant.
// Coordinators merge it into their local serving cache immediately so detached
// replies observe the same active grants and retired floors that root just
// committed.
type EunomiaState struct {
	ActiveGrants      []rootproto.AuthorityGrant
	RetiredGrants     []rootproto.GrantRetirement
	GrantInheritances []rootproto.GrantInheritance
	RetiredEraFloors  []rootproto.AuthorityRetiredEraFloor
}

// Eunomia projects full root state down to the grant lifecycle data needed by
// coordinator serving. The projection preserves scoped floors because they are
// the compact finality state consumed by clients and audit tools.
func (s State) Eunomia() EunomiaState {
	return EunomiaState{
		ActiveGrants:      cloneAuthorityGrants(s.ActiveGrants),
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), s.RetiredGrants...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), s.GrantInheritances...),
		RetiredEraFloors:  rootproto.CloneAuthorityRetiredEraFloors(s.RetiredEraFloors),
	}
}
