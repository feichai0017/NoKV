package server

import (
	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

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
	rootSnapshot := coordstorage.Snapshot{}
	if s.storage != nil {
		if snapshot, snapErr := s.storage.Load(); snapErr == nil {
			rootSnapshot = snapshot
		}
	}

	nowUnixNano, _, holderID, renewIn, clockSkew := s.leaseCampaignBounds()
	lease, _ := s.currentCoordinatorLeaseView()
	report := coordaudit.BuildReport(rootSnapshot, holderID, nowUnixNano)
	leaseFrontiers := controlplane.FrontiersFromState(rootstate.State{
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

	regions := 0
	if s.cluster != nil {
		regions = len(s.cluster.RegionSnapshot())
	}

	return map[string]any{
		"regions": regions,
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
			"cert_generation":    lease.CertGeneration,
			"issued_cursor": map[string]any{
				"term":  lease.IssuedCursor.Term,
				"index": lease.IssuedCursor.Index,
			},
			"duty_mask": lease.DutyMask,
			"frontiers": diagnosticsCoordinatorFrontiers(leaseFrontiers),
		},
		"handoff": diagnosticsAuthorityHandoff(report.Handoff),
		"seal": map[string]any{
			"holder_id":          rootSnapshot.CoordinatorSeal.HolderID,
			"cert_generation":    rootSnapshot.CoordinatorSeal.CertGeneration,
			"duty_mask":          rootSnapshot.CoordinatorSeal.DutyMask,
			"consumed_frontiers": diagnosticsCoordinatorFrontiers(rootSnapshot.CoordinatorSeal.Frontiers),
			"sealed_at_cursor": map[string]any{
				"term":  rootSnapshot.CoordinatorSeal.SealedAtCursor.Term,
				"index": rootSnapshot.CoordinatorSeal.SealedAtCursor.Index,
			},
		},
		"audit": map[string]any{
			"seal_generation":              report.ClosureWitness.SealGeneration,
			"seal_digest":                  report.ClosureWitness.SealDigest,
			"successor_present":            report.ClosureWitness.SuccessorPresent,
			"successor_frontier_coverage":  diagnosticsCoordinatorCoverage(report.ClosureWitness.SuccessorCoverage),
			"successor_lineage_satisfied":  report.ClosureWitness.SuccessorLineageSatisfied,
			"successor_monotone_covered":   report.ClosureWitness.SuccessorMonotoneCovered(),
			"successor_descriptor_covered": report.ClosureWitness.SuccessorDescriptorCovered(),
			"sealed_generation_retired":    report.ClosureWitness.SealedGenerationRetired,
			"closure_satisfied":            report.ClosureWitness.ClosureSatisfied(),
			"closure_stage":                report.Closure.Stage.String(),
			"closure_defect":               string(report.Anomalies.ClosureDefect),
			"closure_recorded": map[string]any{
				"holder_id":            rootSnapshot.CoordinatorClosure.HolderID,
				"seal_generation":      rootSnapshot.CoordinatorClosure.SealGeneration,
				"successor_generation": rootSnapshot.CoordinatorClosure.SuccessorGeneration,
				"seal_digest":          rootSnapshot.CoordinatorClosure.SealDigest,
			},
		},
		"closure_witness": diagnosticsClosureWitness(report.ClosureWitness),
	}
}

func diagnosticsTailToken(token rootstorage.TailToken) map[string]any {
	return map[string]any{
		"term":     token.Cursor.Term,
		"index":    token.Cursor.Index,
		"revision": token.Revision,
	}
}

func diagnosticsLeaderID(storage coordstorage.RootStorage) uint64 {
	if storage == nil {
		return 0
	}
	return storage.LeaderID()
}

func diagnosticsCoordinatorCoverage(status rootstate.CoordinatorSuccessorCoverageStatus) []map[string]any {
	if len(status.Checks) == 0 {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(status.Checks))
	for _, check := range status.Checks {
		out = append(out, map[string]any{
			"duty_mask":         check.DutyMask,
			"duty_name":         check.DutyName,
			"required_frontier": check.RequiredFrontier,
			"actual_frontier":   check.ActualFrontier,
			"covered":           check.Covered,
		})
	}
	return out
}

func diagnosticsAuthorityHandoff(record rootstate.AuthorityHandoffRecord) map[string]any {
	return map[string]any{
		"holder_id":          record.HolderID(),
		"expires_unix_nano":  record.ExpiresUnixNano(),
		"cert_generation":    record.CertGeneration(),
		"duty_mask":          record.DutyMask(),
		"predecessor_digest": record.PredecessorDigest(),
		"issued_cursor": map[string]any{
			"term":  record.IssuedCursor().Term,
			"index": record.IssuedCursor().Index,
		},
		"frontiers": diagnosticsCoordinatorFrontiers(record.Frontiers()),
	}
}

func diagnosticsCoordinatorFrontiers(frontiers rootstate.CoordinatorDutyFrontiers) []map[string]any {
	if frontiers.Len() == 0 {
		return []map[string]any{}
	}
	entries := frontiers.Entries()
	out := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		out = append(out, map[string]any{
			"duty_mask": entry.DutyMask,
			"duty_name": entry.DutyName,
			"frontier":  entry.Frontier,
		})
	}
	return out
}

func diagnosticsClosureWitness(witness rootstate.ClosureWitness) map[string]any {
	return map[string]any{
		"seal_generation":             witness.SealGeneration,
		"seal_digest":                 witness.SealDigest,
		"successor_present":           witness.SuccessorPresent,
		"successor_frontier_coverage": diagnosticsCoordinatorCoverage(witness.SuccessorCoverage),
		"successor_lineage_satisfied": witness.SuccessorLineageSatisfied,
		"sealed_generation_retired":   witness.SealedGenerationRetired,
		"closure_stage":               witness.Stage.String(),
		"closure_satisfied":           witness.ClosureSatisfied(),
	}
}

func leaseUsableBy(lease rootstate.CoordinatorLease, holderID string, nowUnixNano int64, clockSkewNanos int64) bool {
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
