package server

import (
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

	nowUnixNano, _, holderID, renewIn, clockSkew := s.leaseCampaignBounds()
	lease := s.currentCoordinatorLease()

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
			"id_fence":           lease.IDFence,
			"tso_fence":          lease.TSOFence,
		},
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
