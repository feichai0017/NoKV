// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package state

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

func (s State) ActiveVisibleGrantByID(grantID string) (rootproto.VisibleAuthorityGrant, bool) {
	for _, grant := range s.ActiveVisibleGrants {
		if grant.GrantID == grantID {
			return rootproto.CloneVisibleAuthorityGrant(grant), true
		}
	}
	return rootproto.VisibleAuthorityGrant{}, false
}

func (s State) ActiveVisibleGrantFor(scope rootproto.VisibleAuthorityScope, nowUnixNano int64) (rootproto.VisibleAuthorityGrant, bool) {
	for _, grant := range s.ActiveVisibleGrants {
		if grant.Covers(scope, nowUnixNano) {
			return rootproto.CloneVisibleAuthorityGrant(grant), true
		}
	}
	return rootproto.VisibleAuthorityGrant{}, false
}

func (s State) LatestVisibleAuthoritySealFor(scope rootproto.VisibleAuthorityScope) (rootproto.VisibleAuthoritySeal, bool) {
	for i := len(s.VisibleAuthoritySeals) - 1; i >= 0; i-- {
		seal := s.VisibleAuthoritySeals[i]
		grant := rootproto.VisibleAuthorityGrant{
			GrantID:         seal.GrantID,
			EpochID:         seal.EpochID,
			HolderID:        seal.HolderID,
			Scope:           rootproto.CloneVisibleAuthorityScope(seal.Scope),
			ExpiresUnixNano: 1,
		}
		if grant.Covers(scope, 0) || grant.Scope.MountKeyID == scope.MountKeyID {
			return rootproto.CloneVisibleAuthoritySeal(seal), true
		}
	}
	return rootproto.VisibleAuthoritySeal{}, false
}

func NormalizeVisibleAuthorityEvent(state State, cursor Cursor, event rootevent.Event) rootevent.Event {
	if event.Kind != rootevent.KindVisibleAuthorityGranted || event.VisibleGrant == nil {
		return rootevent.CloneEvent(event)
	}
	grant := normalizeVisibleAuthorityGrant(state, cursor, *event.VisibleGrant)
	return rootevent.VisibleAuthorityGranted(grant)
}

func normalizeVisibleAuthorityGrant(state State, cursor Cursor, grant rootproto.VisibleAuthorityGrant) rootproto.VisibleAuthorityGrant {
	grant = rootproto.CloneVisibleAuthorityGrant(grant)
	if grant.RootClusterEpoch == 0 {
		grant.RootClusterEpoch = state.ClusterEpoch
		if grant.RootClusterEpoch == 0 {
			grant.RootClusterEpoch = 1
		}
	}
	// VisibleAuthority visible WAL records persist this token and may only replay against
	// an active grant from the same rooted lineage.
	if grant.IssuedRootToken.Term == 0 && grant.IssuedRootToken.Index == 0 && grant.IssuedRootToken.Revision == 0 {
		grant.IssuedRootToken = rootproto.AuthorityRootToken{
			Term:     cursor.Term,
			Index:    cursor.Index,
			Revision: cursor.Index,
		}
	}
	return grant
}

func applyVisibleAuthorityGrantedToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.VisibleGrant == nil {
		return
	}
	grant := normalizeVisibleAuthorityGrant(*state, cursor, *event.VisibleGrant)
	if !grant.Valid() {
		return
	}
	for i, current := range state.ActiveVisibleGrants {
		if current.GrantID == grant.GrantID {
			state.ActiveVisibleGrants[i] = grant
			if grant.EpochID > state.VisibleAuthorityEpoch {
				state.VisibleAuthorityEpoch = grant.EpochID
			}
			return
		}
		if current.Overlaps(grant) {
			return
		}
	}
	state.ActiveVisibleGrants = append(state.ActiveVisibleGrants, grant)
	if grant.EpochID > state.VisibleAuthorityEpoch {
		state.VisibleAuthorityEpoch = grant.EpochID
	}
}

func applyVisibleAuthoritySealedToState(state *State, event rootevent.Event) {
	if state == nil || event.VisibleSeal == nil {
		return
	}
	seal := rootproto.CloneVisibleAuthoritySeal(*event.VisibleSeal)
	if !seal.Valid() {
		return
	}
	for i, current := range state.VisibleAuthoritySeals {
		if current.GrantID == seal.GrantID {
			state.VisibleAuthoritySeals[i] = seal
			return
		}
	}
	state.VisibleAuthoritySeals = append(state.VisibleAuthoritySeals, seal)
}

func applyVisibleAuthorityRetiredToState(state *State, event rootevent.Event) {
	if state == nil || event.VisibleGrant == nil || event.VisibleGrant.GrantID == "" {
		return
	}
	grantID := event.VisibleGrant.GrantID
	for i := 0; i < len(state.ActiveVisibleGrants); i++ {
		if state.ActiveVisibleGrants[i].GrantID == grantID {
			state.ActiveVisibleGrants = append(state.ActiveVisibleGrants[:i], state.ActiveVisibleGrants[i+1:]...)
			i--
		}
	}
}

func cloneVisibleAuthorityGrants(grants []rootproto.VisibleAuthorityGrant) []rootproto.VisibleAuthorityGrant {
	if len(grants) == 0 {
		return nil
	}
	out := make([]rootproto.VisibleAuthorityGrant, len(grants))
	for i, grant := range grants {
		out[i] = rootproto.CloneVisibleAuthorityGrant(grant)
	}
	return out
}

func cloneVisibleAuthoritySeals(seals []rootproto.VisibleAuthoritySeal) []rootproto.VisibleAuthoritySeal {
	if len(seals) == 0 {
		return nil
	}
	out := make([]rootproto.VisibleAuthoritySeal, len(seals))
	for i, seal := range seals {
		out[i] = rootproto.CloneVisibleAuthoritySeal(seal)
	}
	return out
}
