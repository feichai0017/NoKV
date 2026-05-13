package protocol

import "bytes"

type DutySpec struct {
	ID        DutyID
	ScopeKind DutyScopeKind
	BoundKind DutyBoundKind
}

// builtinDutySpecs is the single registry for coordinator service duties that
// may receive root-issued AuthorityGrants. Any new duty must be registered here
// so validation and verifier keys stay in lockstep.
var builtinDutySpecs = map[DutyID]DutySpec{
	DutyAllocID: {
		ID:        DutyAllocID,
		ScopeKind: DutyScopeGlobal,
		BoundKind: DutyBoundMonotone,
	},
	DutyTSO: {
		ID:        DutyTSO,
		ScopeKind: DutyScopeGlobal,
		BoundKind: DutyBoundMonotone,
	},
	DutyRegionLookup: {
		ID:        DutyRegionLookup,
		ScopeKind: DutyScopeGlobal,
		BoundKind: DutyBoundVersion,
	},
}

func LookupDutySpec(duty DutyID) (DutySpec, bool) {
	spec, ok := builtinDutySpecs[duty]
	return spec, ok
}

func ValidateDutyGrant(grant DutyGrant) bool {
	spec, ok := LookupDutySpec(grant.DutyID)
	if !ok {
		return false
	}
	return validateDutyScope(spec, grant.Scope) && grant.Bound.Kind == spec.BoundKind
}

func ValidateAuthorityUsage(usage AuthorityUsage) bool {
	spec, ok := LookupDutySpec(usage.DutyID)
	if !ok {
		return false
	}
	return validateDutyScope(spec, usage.Scope) && usage.Usage.Kind == spec.BoundKind
}

func ScopeEqual(left, right DutyScope) bool {
	return left.Kind == right.Kind &&
		left.MountID == right.MountID &&
		left.SubtreeRoot == right.SubtreeRoot &&
		bytes.Equal(left.StartKey, right.StartKey) &&
		bytes.Equal(left.EndKey, right.EndKey)
}

// CloneDutyScope copies the mutable byte ranges inside a duty scope. Global,
// mount, and subtree scopes are value-only, but region-range scopes carry slices
// that must not be shared between root state, wire state, and callers.
func CloneDutyScope(scope DutyScope) DutyScope {
	scope.StartKey = append([]byte(nil), scope.StartKey...)
	scope.EndKey = append([]byte(nil), scope.EndKey...)
	return scope
}

// AuthorityRetiredEraFloorFor finds the strongest finality floor for exactly
// one duty/scope pair. A missing entry means this duty has no compact floor.
func AuthorityRetiredEraFloorFor(floors []AuthorityRetiredEraFloor, duty DutyID, scope DutyScope) uint64 {
	var floor uint64
	for _, entry := range floors {
		if entry.DutyID == duty && ScopeEqual(entry.Scope, scope) && entry.RetiredEraFloor > floor {
			floor = entry.RetiredEraFloor
		}
	}
	return floor
}

// AdvanceAuthorityRetiredEraFloor raises, but never lowers, the compact floor
// for one duty/scope pair. The helper preserves monotonic finality while keeping
// unrelated duties independent.
func AdvanceAuthorityRetiredEraFloor(floors []AuthorityRetiredEraFloor, duty DutyID, scope DutyScope, retiredEra uint64) []AuthorityRetiredEraFloor {
	if duty == "" || retiredEra == 0 {
		return floors
	}
	for i := range floors {
		if floors[i].DutyID == duty && ScopeEqual(floors[i].Scope, scope) {
			if retiredEra > floors[i].RetiredEraFloor {
				floors[i].RetiredEraFloor = retiredEra
			}
			return floors
		}
	}
	return append(floors, AuthorityRetiredEraFloor{
		DutyID:          duty,
		Scope:           CloneDutyScope(scope),
		RetiredEraFloor: retiredEra,
	})
}

// AdvanceAuthorityRetiredEraFloorsForBounds advances every duty/scope covered
// by a retired grant. This is used when an inherited retirement becomes compact
// finality and each covered service duty must receive its own floor.
func AdvanceAuthorityRetiredEraFloorsForBounds(floors []AuthorityRetiredEraFloor, bounds []DutyGrant, retiredEra uint64) []AuthorityRetiredEraFloor {
	if retiredEra == 0 {
		return floors
	}
	for _, bound := range bounds {
		floors = AdvanceAuthorityRetiredEraFloor(floors, bound.DutyID, bound.Scope, retiredEra)
	}
	return floors
}

// CloneAuthorityRetiredEraFloors returns an isolated copy so snapshots, wire
// conversions, and audit reports cannot accidentally share mutable scope slices.
func CloneAuthorityRetiredEraFloors(floors []AuthorityRetiredEraFloor) []AuthorityRetiredEraFloor {
	if len(floors) == 0 {
		return nil
	}
	out := make([]AuthorityRetiredEraFloor, len(floors))
	for i, floor := range floors {
		floor.Scope = CloneDutyScope(floor.Scope)
		out[i] = floor
	}
	return out
}

func DutyBoundCovers(grant, usage DutyBound) bool {
	if grant.Kind != usage.Kind {
		return false
	}
	switch usage.Kind {
	case DutyBoundMonotone:
		return usage.MonotoneUpper <= grant.MonotoneUpper
	case DutyBoundVersion:
		return usage.DescriptorRevisionCeiling <= grant.DescriptorRevisionCeiling &&
			usage.MaxRootLag <= grant.MaxRootLag
	case DutyBoundBudget:
		return usage.Budget <= grant.Budget
	case DutyBoundEpoch:
		return usage.Epoch <= grant.Epoch
	default:
		return false
	}
}

func validateDutyScope(spec DutySpec, scope DutyScope) bool {
	if scope.Kind != spec.ScopeKind {
		return false
	}
	switch scope.Kind {
	case DutyScopeGlobal:
		return scope.MountID == "" &&
			scope.SubtreeRoot == 0 &&
			len(scope.StartKey) == 0 &&
			len(scope.EndKey) == 0
	case DutyScopeMount:
		return scope.MountID != "" &&
			scope.SubtreeRoot == 0 &&
			len(scope.StartKey) == 0 &&
			len(scope.EndKey) == 0
	case DutyScopeSubtree:
		return scope.MountID != "" &&
			scope.SubtreeRoot != 0 &&
			len(scope.StartKey) == 0 &&
			len(scope.EndKey) == 0
	case DutyScopeRegionRange:
		return len(scope.StartKey) != 0 || len(scope.EndKey) != 0
	default:
		return false
	}
}
