package server

import (
	"github.com/feichai0017/NoKV/coordinator/rootview"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

// replicaStateName renders a ReplicaState enum into a short human label
// for expvar / dashboard consumers.
func replicaStateName(s metaregion.ReplicaState) string {
	switch s {
	case metaregion.ReplicaStateNew:
		return "new"
	case metaregion.ReplicaStateRunning:
		return "running"
	case metaregion.ReplicaStateRemoving:
		return "removing"
	case metaregion.ReplicaStateTombstone:
		return "tombstone"
	default:
		return "other"
	}
}

// DiagnosticsSnapshot exposes a structured control-plane runtime snapshot for expvar
// and diagnostics. It intentionally reports observable state only.
func (s *Service) DiagnosticsSnapshot() map[string]any {
	if s == nil {
		return map[string]any{}
	}

	state, err := s.currentReadState()
	loadErr := ""
	if err != nil {
		loadErr = err.Error()
	}
	rootSnapshot := rootview.Snapshot{}
	if s.storage != nil {
		if snapshot, snapErr := s.storage.Load(); snapErr == nil {
			rootSnapshot = snapshot
		}
	}
	snapshotRetention := rootSnapshot.SnapshotRetentionIndex()

	nowUnixNano, _, holderID, renewIn, clockSkew := s.grantCampaignBounds()
	grant := rootSnapshot.ActiveGrant
	latestRetirement := diagnosticsLatestRetirement(rootSnapshot.RetiredGrants)

	s.allocMu.Lock()
	idCurrent := s.ids.Current()
	idWindowHigh := s.idWindowHigh
	tsoCurrent := s.tso.Current()
	tsoWindowHigh := s.tsoWindowHigh
	s.allocMu.Unlock()

	s.statusMu.RLock()
	lastReload := s.lastRootReload
	lastReloadErr := s.lastRootError
	s.statusMu.RUnlock()
	authorityState, authorityInflight := s.authorityServingSnapshot()

	regionCount := 0
	regionDetails := []map[string]any{}
	if s.cluster != nil {
		snap := s.cluster.RegionSnapshot()
		regionCount = len(snap)
		regionDetails = make([]map[string]any, 0, len(snap))
		for _, r := range snap {
			peers := make([]map[string]any, 0, len(r.Descriptor.Peers))
			for _, p := range r.Descriptor.Peers {
				peers = append(peers, map[string]any{
					"store_id": p.StoreID,
					"peer_id":  p.PeerID,
				})
			}
			regionDetails = append(regionDetails, map[string]any{
				"region_id":            r.Descriptor.RegionID,
				"start_key":            string(r.Descriptor.StartKey),
				"end_key":              string(r.Descriptor.EndKey),
				"epoch_ver":            r.Descriptor.Epoch.Version,
				"conf_ver":             r.Descriptor.Epoch.ConfVersion,
				"root_epoch":           r.Descriptor.RootEpoch,
				"state":                replicaStateName(r.Descriptor.State),
				"peers":                peers,
				"last_hb_unix":         r.LastHeartbeat.UnixNano(),
				"leader_store_id":      r.LeaderStoreID,
				"leader_reported_unix": r.LeaderReportedAt.UnixNano(),
			})
		}
	}

	return map[string]any{
		"regions":            regionCount,
		"region_descriptors": regionDetails,
		"allocator": map[string]any{
			"id_current":      idCurrent,
			"id_window_high":  idWindowHigh,
			"tso_current":     tsoCurrent,
			"tso_window_high": tsoWindowHigh,
		},
		"root": map[string]any{
			"configured":                     s.storage != nil,
			"served_token":                   diagnosticsTailToken(state.servedToken),
			"current_token":                  diagnosticsTailToken(state.currentToken),
			"root_lag":                       state.rootLag,
			"catch_up_state":                 catchUpStateToProto(state.catchUpState).String(),
			"degraded_mode":                  state.degraded.String(),
			"served_by_leader":               state.servedByLeader,
			"storage_can_submit_root_writes": s.storage == nil || s.storage.CanSubmitRootWrites(),
			"storage_leader_id":              diagnosticsLeaderID(s.storage),
			"last_reload_unix_nano":          lastReload,
			"last_reload_error":              lastReloadErr,
			"read_state_load_error":          loadErr,
			"snapshot_epochs":                len(rootSnapshot.SnapshotEpochs),
			"snapshot_retention": map[string]any{
				"active":             snapshotRetention.Active(),
				"min_read_version":   snapshotRetention.GlobalFloor,
				"mount_floors":       snapshotRetention.MountFloors,
				"enforcement_target": "mvcc_gc",
			},
		},
		"grant": map[string]any{
			"enabled":            s.coordinatorGrantEnabled(),
			"coordinator_id":     holderID,
			"ttl_nanos":          s.currentGrantTTL(),
			"renew_before_nanos": renewIn.Nanoseconds(),
			"clock_skew_nanos":   clockSkew.Nanoseconds(),
			"holder_id":          grant.HolderID,
			"expires_unix_nano":  grant.ExpiresUnixNano,
			"active":             grant.ActiveAt(nowUnixNano),
			"held_by_self":       grant.HolderID != "" && grant.HolderID == holderID,
			"usable_by_self":     grantUsableBy(grant, holderID, nowUnixNano, clockSkew.Nanoseconds()),
			"era":                grant.Era,
			"issued_at": map[string]any{
				"term":  grant.IssuedAt.Term,
				"index": grant.IssuedAt.Index,
			},
			"duties": diagnosticsDutyGrants(grant.Duties),
		},
		"authority": map[string]any{
			"serving_state": authorityState.String(),
			"in_flight":     authorityInflight,
			"active_grant": map[string]any{
				"grant_id":           grant.GrantID,
				"holder_id":          grant.HolderID,
				"era":                grant.Era,
				"expires_unix_nano":  grant.ExpiresUnixNano,
				"active":             grant.ActiveAt(nowUnixNano),
				"held_by_self":       grant.HolderID != "" && grant.HolderID == holderID,
				"issued_root_token":  diagnosticsAuthorityRootToken(grant.IssuedRootToken),
				"duties":             diagnosticsDutyGrants(grant.Duties),
				"predecessor_grants": diagnosticsGrantRetirements(grant.PredecessorRetirements),
			},
			"retired_grants":      diagnosticsGrantRetirements(rootSnapshot.RetiredGrants),
			"grant_inheritances":  diagnosticsGrantInheritances(rootSnapshot.GrantInheritances),
			"remaining_id_bound":  diagnosticsRemainingMonotoneBound(grant, rootproto.DutyAllocID, idCurrent),
			"remaining_tso_bound": diagnosticsRemainingMonotoneBound(grant, rootproto.DutyTSO, tsoCurrent),
		},
		"retirement": map[string]any{
			"grant_id":   latestRetirement.GrantID,
			"holder_id":  latestRetirement.HolderID,
			"era":        latestRetirement.Era,
			"mode":       diagnosticsGrantRetirementMode(latestRetirement.Mode),
			"bounds":     diagnosticsDutyGrants(latestRetirement.Bounds),
			"retired_at": diagnosticsCursor(latestRetirement.RetiredAt),
			"inherited":  latestRetirement.InheritedByGrantID != "",
		},
		"audit": map[string]any{
			"sealed_exact_completed":    diagnosticsHasInheritedRetirement(rootSnapshot.RetiredGrants, rootproto.GrantRetirementSealedExact),
			"expired_bound_inherited":   diagnosticsHasInheritedRetirement(rootSnapshot.RetiredGrants, rootproto.GrantRetirementExpiredBound),
			"retired_not_inherited":     diagnosticsHasPendingRetirement(rootSnapshot.RetiredGrants),
			"invalid_successor_bound":   diagnosticsInvalidSuccessorBound(grant, rootSnapshot.RetiredGrants),
			"active_grant_id":           grant.GrantID,
			"active_era":                grant.Era,
			"latest_retirement_mode":    diagnosticsGrantRetirementMode(latestRetirement.Mode),
			"latest_retired_grant_id":   latestRetirement.GrantID,
			"latest_retired_era":        latestRetirement.Era,
			"latest_inherited_by_grant": latestRetirement.InheritedByGrantID,
		},
		"eunomia_metrics": s.eunomiaMetrics.snapshot(),
	}
}

func diagnosticsTailToken(token rootstorage.TailToken) map[string]any {
	return map[string]any{
		"term":     token.Cursor.Term,
		"index":    token.Cursor.Index,
		"revision": token.Revision,
	}
}

func diagnosticsLeaderID(storage rootview.RootStorage) uint64 {
	if storage == nil {
		return 0
	}
	return storage.LeaderID()
}

func diagnosticsCursor(cursor rootproto.Cursor) map[string]any {
	return map[string]any{
		"term":  cursor.Term,
		"index": cursor.Index,
	}
}

func diagnosticsAuthorityRootToken(token rootproto.AuthorityRootToken) map[string]any {
	return map[string]any{
		"term":     token.Term,
		"index":    token.Index,
		"revision": token.Revision,
	}
}

func diagnosticsDutyGrants(duties []rootproto.DutyGrant) []map[string]any {
	if len(duties) == 0 {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(duties))
	for _, duty := range duties {
		out = append(out, map[string]any{
			"duty_id": rootproto.DutyName(duty.DutyID),
			"scope":   diagnosticsDutyScope(duty.Scope),
			"bound":   diagnosticsDutyBound(duty.Bound),
		})
	}
	return out
}

func diagnosticsDutyScope(scope rootproto.DutyScope) map[string]any {
	return map[string]any{
		"kind":         diagnosticsDutyScopeKind(scope.Kind),
		"mount_id":     scope.MountID,
		"subtree_root": scope.SubtreeRoot,
		"start_key":    string(scope.StartKey),
		"end_key":      string(scope.EndKey),
	}
}

func diagnosticsDutyScopeKind(kind rootproto.DutyScopeKind) string {
	switch kind {
	case rootproto.DutyScopeGlobal:
		return "global"
	case rootproto.DutyScopeMount:
		return "mount"
	case rootproto.DutyScopeSubtree:
		return "subtree"
	case rootproto.DutyScopeRegionRange:
		return "region_range"
	default:
		return "unspecified"
	}
}

func diagnosticsDutyBound(bound rootproto.DutyBound) map[string]any {
	out := map[string]any{"kind": diagnosticsDutyBoundKind(bound.Kind)}
	switch bound.Kind {
	case rootproto.DutyBoundMonotone:
		out["monotone_upper"] = bound.MonotoneUpper
	case rootproto.DutyBoundVersion:
		out["root_token"] = diagnosticsAuthorityRootToken(bound.VersionRootToken)
		out["descriptor_revision_ceiling"] = bound.DescriptorRevisionCeiling
		out["max_root_lag"] = bound.MaxRootLag
	case rootproto.DutyBoundBudget:
		out["budget"] = bound.Budget
	case rootproto.DutyBoundEpoch:
		out["epoch"] = bound.Epoch
	}
	return out
}

func diagnosticsDutyBoundKind(kind rootproto.DutyBoundKind) string {
	switch kind {
	case rootproto.DutyBoundMonotone:
		return "monotone"
	case rootproto.DutyBoundVersion:
		return "version"
	case rootproto.DutyBoundBudget:
		return "budget"
	case rootproto.DutyBoundEpoch:
		return "epoch"
	default:
		return "unspecified"
	}
}

func diagnosticsGrantRetirements(retirements []rootproto.GrantRetirement) []map[string]any {
	if len(retirements) == 0 {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(retirements))
	for _, retirement := range retirements {
		out = append(out, map[string]any{
			"grant_id":              retirement.GrantID,
			"holder_id":             retirement.HolderID,
			"era":                   retirement.Era,
			"mode":                  diagnosticsGrantRetirementMode(retirement.Mode),
			"bounds":                diagnosticsDutyGrants(retirement.Bounds),
			"retired_at":            diagnosticsCursor(retirement.RetiredAt),
			"inherited_by_grant_id": retirement.InheritedByGrantID,
		})
	}
	return out
}

func diagnosticsGrantInheritances(inheritances []rootproto.GrantInheritance) []map[string]any {
	if len(inheritances) == 0 {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(inheritances))
	for _, inheritance := range inheritances {
		out = append(out, map[string]any{
			"predecessor_grant_id": inheritance.PredecessorGrantID,
			"successor_grant_id":   inheritance.SuccessorGrantID,
			"inherited_at":         diagnosticsCursor(inheritance.InheritedAt),
		})
	}
	return out
}

func diagnosticsGrantRetirementMode(mode rootproto.GrantRetirementMode) string {
	switch mode {
	case rootproto.GrantRetirementSealedExact:
		return "sealed_exact"
	case rootproto.GrantRetirementExpiredBound:
		return "expired_bound"
	default:
		return "unspecified"
	}
}

func diagnosticsLatestRetirement(retirements []rootproto.GrantRetirement) rootproto.GrantRetirement {
	var latest rootproto.GrantRetirement
	for _, retirement := range retirements {
		if retirement.Era > latest.Era {
			latest = retirement
		}
	}
	return latest
}

func diagnosticsRemainingMonotoneBound(grant rootproto.AuthorityGrant, duty rootproto.DutyID, current uint64) uint64 {
	dutyGrant, ok := grant.Duty(duty)
	if !ok || dutyGrant.Bound.Kind != rootproto.DutyBoundMonotone || dutyGrant.Bound.MonotoneUpper <= current {
		return 0
	}
	return dutyGrant.Bound.MonotoneUpper - current
}

func diagnosticsHasInheritedRetirement(retirements []rootproto.GrantRetirement, mode rootproto.GrantRetirementMode) bool {
	for _, retirement := range retirements {
		if retirement.Mode == mode && retirement.InheritedByGrantID != "" {
			return true
		}
	}
	return false
}

func diagnosticsHasPendingRetirement(retirements []rootproto.GrantRetirement) bool {
	for _, retirement := range retirements {
		if retirement.Present() && retirement.InheritedByGrantID == "" {
			return true
		}
	}
	return false
}

func diagnosticsInvalidSuccessorBound(grant rootproto.AuthorityGrant, retirements []rootproto.GrantRetirement) bool {
	if !grant.Present() {
		return false
	}
	for _, retirement := range retirements {
		if retirement.InheritedByGrantID != "" {
			continue
		}
		for _, bound := range retirement.Bounds {
			duty, ok := grant.Duty(bound.DutyID)
			if !ok || !authorityBoundCovers(duty.Bound, bound.Bound) {
				return true
			}
		}
	}
	return false
}

func grantUsableBy(grant rootproto.AuthorityGrant, holderID string, nowUnixNano int64, clockSkewNanos int64) bool {
	if grant.HolderID == "" || grant.HolderID != holderID {
		return false
	}
	return grant.ExpiresUnixNano > nowUnixNano+clockSkewNanos
}

func (s *Service) currentGrantTTL() int64 {
	if s == nil {
		return 0
	}
	s.grantMu.RLock()
	defer s.grantMu.RUnlock()
	return s.grantTTL.Nanoseconds()
}
