package protocol

import "bytes"

type DutySpec struct {
	ID        DutyID
	ScopeKind DutyScopeKind
	BoundKind DutyBoundKind
}

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
