package controlplane

import rootstate "github.com/feichai0017/NoKV/meta/root/state"

// Frontiers materializes the built-in duty-frontier projection from the rooted
// allocator and descriptor state.
func Frontiers(idFence, tsoFence, descriptorRevision uint64) rootstate.CoordinatorDutyFrontiers {
	return rootstate.NewCoordinatorDutyFrontiers(
		rootstate.CoordinatorDutyFrontier{DutyMask: rootstate.CoordinatorDutyAllocID, DutyName: "alloc_id", Frontier: idFence},
		rootstate.CoordinatorDutyFrontier{DutyMask: rootstate.CoordinatorDutyTSO, DutyName: "tso", Frontier: tsoFence},
		rootstate.CoordinatorDutyFrontier{DutyMask: rootstate.CoordinatorDutyGetRegionByKey, DutyName: "get_region_by_key", Frontier: descriptorRevision},
	)
}

// FrontiersFromState projects the current rooted allocator and descriptor state
// into the generic duty-frontier protocol object.
func FrontiersFromState(state rootstate.State, descriptorRevision uint64) rootstate.CoordinatorDutyFrontiers {
	return Frontiers(state.IDFence, state.TSOFence, descriptorRevision)
}

// HandoffRecord projects the current rooted lease plus handoff frontier view into the portable
// handoff record used by checker and diagnostics code.
func HandoffRecord(current rootstate.CoordinatorLease, frontiers rootstate.CoordinatorDutyFrontiers) rootstate.AuthorityHandoffRecord {
	return rootstate.AuthorityHandoffRecord{
		HolderID:          current.HolderID,
		ExpiresUnixNano:   current.ExpiresUnixNano,
		CertGeneration:    current.CertGeneration,
		IssuedCursor:      current.IssuedCursor,
		DutyMask:          rootstate.ResolvedCoordinatorDutyMask(current.DutyMask),
		PredecessorDigest: current.PredecessorDigest,
		Frontiers:         frontiers,
	}
}
