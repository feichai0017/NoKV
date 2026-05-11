package state

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

func (s State) ActiveCapsuleGrantByID(grantID string) (rootproto.CapsuleAuthorityGrant, bool) {
	for _, grant := range s.ActiveCapsuleGrants {
		if grant.GrantID == grantID {
			return rootproto.CloneCapsuleAuthorityGrant(grant), true
		}
	}
	return rootproto.CapsuleAuthorityGrant{}, false
}

func (s State) ActiveCapsuleGrantFor(scope rootproto.CapsuleAuthorityScope, nowUnixNano int64) (rootproto.CapsuleAuthorityGrant, bool) {
	for _, grant := range s.ActiveCapsuleGrants {
		if grant.Covers(scope, nowUnixNano) {
			return rootproto.CloneCapsuleAuthorityGrant(grant), true
		}
	}
	return rootproto.CapsuleAuthorityGrant{}, false
}

func applyCapsuleAuthorityGrantedToState(state *State, event rootevent.Event) {
	if state == nil || event.CapsuleGrant == nil {
		return
	}
	grant := rootproto.CloneCapsuleAuthorityGrant(*event.CapsuleGrant)
	if !grant.Valid() {
		return
	}
	for i, current := range state.ActiveCapsuleGrants {
		if current.GrantID == grant.GrantID {
			state.ActiveCapsuleGrants[i] = grant
			return
		}
		if current.Overlaps(grant) {
			return
		}
	}
	state.ActiveCapsuleGrants = append(state.ActiveCapsuleGrants, grant)
}

func applyCapsuleAuthorityRetiredToState(state *State, event rootevent.Event) {
	if state == nil || event.CapsuleGrant == nil || event.CapsuleGrant.GrantID == "" {
		return
	}
	grantID := event.CapsuleGrant.GrantID
	for i := 0; i < len(state.ActiveCapsuleGrants); i++ {
		if state.ActiveCapsuleGrants[i].GrantID == grantID {
			state.ActiveCapsuleGrants = append(state.ActiveCapsuleGrants[:i], state.ActiveCapsuleGrants[i+1:]...)
			i--
		}
	}
}

func cloneCapsuleAuthorityGrants(grants []rootproto.CapsuleAuthorityGrant) []rootproto.CapsuleAuthorityGrant {
	if len(grants) == 0 {
		return nil
	}
	out := make([]rootproto.CapsuleAuthorityGrant, len(grants))
	for i, grant := range grants {
		out[i] = rootproto.CloneCapsuleAuthorityGrant(grant)
	}
	return out
}
