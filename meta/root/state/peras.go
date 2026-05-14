// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package state

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

func (s State) ActivePerasGrantByID(grantID string) (rootproto.PerasAuthorityGrant, bool) {
	for _, grant := range s.ActivePerasGrants {
		if grant.GrantID == grantID {
			return rootproto.ClonePerasAuthorityGrant(grant), true
		}
	}
	return rootproto.PerasAuthorityGrant{}, false
}

func (s State) ActivePerasGrantFor(scope rootproto.PerasAuthorityScope, nowUnixNano int64) (rootproto.PerasAuthorityGrant, bool) {
	for _, grant := range s.ActivePerasGrants {
		if grant.Covers(scope, nowUnixNano) {
			return rootproto.ClonePerasAuthorityGrant(grant), true
		}
	}
	return rootproto.PerasAuthorityGrant{}, false
}

func (s State) LatestPerasAuthoritySealFor(scope rootproto.PerasAuthorityScope) (rootproto.PerasAuthoritySeal, bool) {
	for i := len(s.PerasAuthoritySeals) - 1; i >= 0; i-- {
		seal := s.PerasAuthoritySeals[i]
		grant := rootproto.PerasAuthorityGrant{
			GrantID:         seal.GrantID,
			EpochID:         seal.EpochID,
			HolderID:        seal.HolderID,
			Scope:           rootproto.ClonePerasAuthorityScope(seal.Scope),
			ExpiresUnixNano: 1,
		}
		if grant.Covers(scope, 0) || grant.Scope.MountKeyID == scope.MountKeyID {
			return rootproto.ClonePerasAuthoritySeal(seal), true
		}
	}
	return rootproto.PerasAuthoritySeal{}, false
}

func applyPerasAuthorityGrantedToState(state *State, event rootevent.Event) {
	if state == nil || event.PerasGrant == nil {
		return
	}
	grant := rootproto.ClonePerasAuthorityGrant(*event.PerasGrant)
	if !grant.Valid() {
		return
	}
	for i, current := range state.ActivePerasGrants {
		if current.GrantID == grant.GrantID {
			state.ActivePerasGrants[i] = grant
			if grant.EpochID > state.PerasAuthorityEpoch {
				state.PerasAuthorityEpoch = grant.EpochID
			}
			return
		}
		if current.Overlaps(grant) {
			return
		}
	}
	state.ActivePerasGrants = append(state.ActivePerasGrants, grant)
	if grant.EpochID > state.PerasAuthorityEpoch {
		state.PerasAuthorityEpoch = grant.EpochID
	}
}

func applyPerasAuthoritySealedToState(state *State, event rootevent.Event) {
	if state == nil || event.PerasSeal == nil {
		return
	}
	seal := rootproto.ClonePerasAuthoritySeal(*event.PerasSeal)
	if !seal.Valid() {
		return
	}
	for i, current := range state.PerasAuthoritySeals {
		if current.GrantID == seal.GrantID {
			state.PerasAuthoritySeals[i] = seal
			return
		}
	}
	state.PerasAuthoritySeals = append(state.PerasAuthoritySeals, seal)
}

func applyPerasAuthorityRetiredToState(state *State, event rootevent.Event) {
	if state == nil || event.PerasGrant == nil || event.PerasGrant.GrantID == "" {
		return
	}
	grantID := event.PerasGrant.GrantID
	for i := 0; i < len(state.ActivePerasGrants); i++ {
		if state.ActivePerasGrants[i].GrantID == grantID {
			state.ActivePerasGrants = append(state.ActivePerasGrants[:i], state.ActivePerasGrants[i+1:]...)
			i--
		}
	}
}

func clonePerasAuthorityGrants(grants []rootproto.PerasAuthorityGrant) []rootproto.PerasAuthorityGrant {
	if len(grants) == 0 {
		return nil
	}
	out := make([]rootproto.PerasAuthorityGrant, len(grants))
	for i, grant := range grants {
		out[i] = rootproto.ClonePerasAuthorityGrant(grant)
	}
	return out
}

func clonePerasAuthoritySeals(seals []rootproto.PerasAuthoritySeal) []rootproto.PerasAuthoritySeal {
	if len(seals) == 0 {
		return nil
	}
	out := make([]rootproto.PerasAuthoritySeal, len(seals))
	for i, seal := range seals {
		out[i] = rootproto.ClonePerasAuthoritySeal(seal)
	}
	return out
}
