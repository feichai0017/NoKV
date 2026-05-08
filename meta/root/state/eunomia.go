package state

import rootproto "github.com/feichai0017/NoKV/meta/root/protocol"

type EunomiaState struct {
	ActiveGrants      []rootproto.AuthorityGrant
	RetiredGrants     []rootproto.GrantRetirement
	GrantInheritances []rootproto.GrantInheritance
	RetiredEraFloor   uint64
}

func (s State) Eunomia() EunomiaState {
	return EunomiaState{
		ActiveGrants:      cloneAuthorityGrants(s.ActiveGrants),
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), s.RetiredGrants...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), s.GrantInheritances...),
		RetiredEraFloor:   s.RetiredEraFloor,
	}
}
