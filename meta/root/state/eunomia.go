package state

import rootproto "github.com/feichai0017/NoKV/meta/root/protocol"

type EunomiaState struct {
	ActiveGrant       rootproto.AuthorityGrant
	RetiredGrants     []rootproto.GrantRetirement
	GrantInheritances []rootproto.GrantInheritance
}

func (s State) Eunomia() EunomiaState {
	return EunomiaState{
		ActiveGrant:       s.ActiveGrant,
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), s.RetiredGrants...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), s.GrantInheritances...),
	}
}
