package server

import (
	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
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

	nowUnixNano, _, holderID, renewIn, clockSkew := s.leaseCampaignBounds()
	lease, _ := s.currentTenureView()
	report := coordaudit.BuildReport(rootSnapshot, holderID, nowUnixNano)
	leaseFrontiers := succession.Frontiers(rootstate.State{
		IDFence:  rootSnapshot.Allocator.IDCurrent,
		TSOFence: rootSnapshot.Allocator.TSCurrent,
	}, report.RootDescriptorRevision)

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
			"configured":            s.storage != nil,
			"served_token":          diagnosticsTailToken(state.servedToken),
			"current_token":         diagnosticsTailToken(state.currentToken),
			"root_lag":              state.rootLag,
			"catch_up_state":        catchUpStateToProto(state.catchUpState).String(),
			"degraded_mode":         state.degraded.String(),
			"served_by_leader":      state.servedByLeader,
			"storage_is_leader":     s.storage == nil || s.storage.IsLeader(),
			"storage_leader_id":     diagnosticsLeaderID(s.storage),
			"last_reload_unix_nano": lastReload,
			"last_reload_error":     lastReloadErr,
			"read_state_load_error": loadErr,
		},
		"lease": map[string]any{
			"enabled":            s.coordinatorLeaseEnabled(),
			"coordinator_id":     holderID,
			"ttl_nanos":          s.currentLeaseTTL(),
			"renew_before_nanos": renewIn.Nanoseconds(),
			"clock_skew_nanos":   clockSkew.Nanoseconds(),
			"holder_id":          lease.HolderID,
			"expires_unix_nano":  lease.ExpiresUnixNano,
			"active":             lease.ActiveAt(nowUnixNano),
			"held_by_self":       lease.HolderID != "" && lease.HolderID == holderID,
			"usable_by_self":     leaseUsableBy(lease, holderID, nowUnixNano, clockSkew.Nanoseconds()),
			"epoch":              lease.Epoch,
			"issued_at": map[string]any{
				"term":  lease.IssuedAt.Term,
				"index": lease.IssuedAt.Index,
			},
			"mandate":   lease.Mandate,
			"frontiers": diagnosticsCoordinatorFrontiers(leaseFrontiers),
		},
		"handoff": diagnosticsAuthorityHandoff(report.Handoff),
		"seal": map[string]any{
			"holder_id":          rootSnapshot.Legacy.HolderID,
			"epoch":              rootSnapshot.Legacy.Epoch,
			"mandate":            rootSnapshot.Legacy.Mandate,
			"consumed_frontiers": diagnosticsCoordinatorFrontiers(rootSnapshot.Legacy.Frontiers),
			"sealed_at": map[string]any{
				"term":  rootSnapshot.Legacy.SealedAt.Term,
				"index": rootSnapshot.Legacy.SealedAt.Index,
			},
		},
		"audit": map[string]any{
			"legacy_epoch":                 report.HandoverWitness.LegacyEpoch,
			"legacy_digest":                report.HandoverWitness.LegacyDigest,
			"successor_present":            report.HandoverWitness.SuccessorPresent,
			"successor_frontier_coverage":  diagnosticsCoordinatorCoverage(report.HandoverWitness.Inheritance),
			"successor_lineage_satisfied":  report.HandoverWitness.SuccessorLineageSatisfied,
			"successor_monotone_covered":   report.HandoverWitness.SuccessorMonotoneCovered(),
			"successor_descriptor_covered": report.HandoverWitness.SuccessorDescriptorCovered(),
			"sealed_generation_retired":    report.HandoverWitness.SealedGenerationRetired,
			"finality_satisfied":           report.HandoverWitness.FinalitySatisfied(),
			"handover_stage":               report.Handover.Stage.String(),
			"finality_defect":              string(report.Anomalies.FinalityDefect),
			"handover_recorded": map[string]any{
				"holder_id":       rootSnapshot.Handover.HolderID,
				"legacy_epoch":    rootSnapshot.Handover.LegacyEpoch,
				"successor_epoch": rootSnapshot.Handover.SuccessorEpoch,
				"legacy_digest":   rootSnapshot.Handover.LegacyDigest,
			},
		},
		"handover_witness":   diagnosticsHandoverWitness(report.HandoverWitness),
		"succession_metrics": s.successionMetrics.snapshot(),
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

func diagnosticsCoordinatorCoverage(status rootproto.InheritanceStatus) []map[string]any {
	if len(status.Checks) == 0 {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(status.Checks))
	for _, check := range status.Checks {
		out = append(out, map[string]any{
			"mandate":           check.Mandate,
			"duty_name":         rootproto.MandateName(check.Mandate),
			"required_frontier": check.RequiredFrontier,
			"actual_frontier":   check.ActualFrontier,
			"covered":           check.Covered,
		})
	}
	return out
}

func diagnosticsAuthorityHandoff(record rootproto.AuthorityHandoffRecord) map[string]any {
	return map[string]any{
		"holder_id":         record.HolderID,
		"expires_unix_nano": record.ExpiresUnixNano,
		"epoch":             record.Epoch,
		"mandate":           record.Mandate,
		"lineage_digest":    record.LineageDigest,
		"issued_at": map[string]any{
			"term":  record.IssuedAt.Term,
			"index": record.IssuedAt.Index,
		},
		"frontiers": diagnosticsCoordinatorFrontiers(record.Frontiers),
	}
}

func diagnosticsCoordinatorFrontiers(frontiers rootproto.MandateFrontiers) []map[string]any {
	if frontiers.Len() == 0 {
		return []map[string]any{}
	}
	entries := frontiers.Entries()
	out := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		out = append(out, map[string]any{
			"mandate":   entry.Mandate,
			"duty_name": rootproto.MandateName(entry.Mandate),
			"frontier":  entry.Frontier,
		})
	}
	return out
}

func diagnosticsHandoverWitness(witness rootproto.HandoverWitness) map[string]any {
	return map[string]any{
		"legacy_epoch":                witness.LegacyEpoch,
		"legacy_digest":               witness.LegacyDigest,
		"successor_present":           witness.SuccessorPresent,
		"successor_frontier_coverage": diagnosticsCoordinatorCoverage(witness.Inheritance),
		"successor_lineage_satisfied": witness.SuccessorLineageSatisfied,
		"sealed_generation_retired":   witness.SealedGenerationRetired,
		"handover_stage":              witness.Stage.String(),
		"finality_satisfied":          witness.FinalitySatisfied(),
	}
}

func leaseUsableBy(lease rootstate.Tenure, holderID string, nowUnixNano int64, clockSkewNanos int64) bool {
	if lease.HolderID == "" || lease.HolderID != holderID {
		return false
	}
	return lease.ExpiresUnixNano > nowUnixNano+clockSkewNanos
}

func (s *Service) currentLeaseTTL() int64 {
	if s == nil {
		return 0
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	return s.leaseTTL.Nanoseconds()
}
