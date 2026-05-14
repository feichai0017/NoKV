// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package specs

import (
	"strings"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile/specdsl"
	"github.com/stretchr/testify/require"
)

func TestOperationSpecsHaveStableGeneratedShape(t *testing.T) {
	specs := All()
	require.NotEmpty(t, specs)

	seenName := make(map[string]struct{}, len(specs))
	seenCompile := make(map[string]struct{}, len(specs))
	seenKind := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		require.NotEmpty(t, spec.Name)
		require.NotContains(t, seenName, spec.Name)
		seenName[spec.Name] = struct{}{}

		require.NotEmpty(t, spec.FileName, spec.Name)
		require.True(t, strings.HasSuffix(spec.FileName, ".peras.go"), spec.Name)
		require.NotEmpty(t, spec.ProgramType, spec.Name)
		require.NotEmpty(t, spec.RequestType, spec.Name)
		require.NotEmpty(t, spec.CompileName, spec.Name)
		require.NotContains(t, seenCompile, spec.CompileName)
		seenCompile[spec.CompileName] = struct{}{}
		require.NotEmpty(t, spec.PlanName, spec.Name)
		require.NotEmpty(t, spec.OperationKind, spec.Name)
		require.NotContains(t, seenKind, spec.OperationKind)
		seenKind[spec.OperationKind] = struct{}{}
		require.NotEmpty(t, spec.Durability, spec.Name)
		require.NotEmpty(t, spec.Eligibility, spec.Name)
		if spec.Eligibility == "EligibilitySlowPath" {
			require.NotEmpty(t, spec.SlowReason, spec.Name)
		}
		for _, fallback := range spec.SlowFallbacks {
			require.NotEmpty(t, fallback, spec.Name)
		}
		require.GreaterOrEqual(t, spec.PredicateCount, -1, spec.Name)
		require.GreaterOrEqual(t, spec.EffectCount, -1, spec.Name)
		if spec.PredicateCount >= 0 {
			require.Len(t, spec.Predicates, spec.PredicateCount, spec.Name)
		} else {
			require.NotEmpty(t, spec.Predicates, spec.Name)
		}
		if spec.EffectCount >= 0 {
			require.Len(t, spec.Effects, spec.EffectCount, spec.Name)
		}
		require.Contains(t, map[string]struct{}{"create": {}, "operation": {}}, spec.Emitter, spec.Name)
		if spec.Materialize != "" {
			require.NotEmpty(t, spec.ValuesType, spec.Name)
		}
		requireSemanticNames(t, spec.Name, spec.Predicates, spec.Effects, spec.Guards, spec.OptionalGuards)
	}
}

func requireSemanticNames(t *testing.T, op string, predicates []specdsl.PredicateSpec, effects []specdsl.EffectSpec, guards []specdsl.GuardSpec, optional []specdsl.GuardSpec) {
	t.Helper()
	for _, predicate := range predicates {
		require.NotEmpty(t, predicate.Name, op)
		require.NotEmpty(t, predicate.Kind, op)
		require.NotEmpty(t, predicate.Key, op)
	}
	for _, effect := range effects {
		require.NotEmpty(t, effect.Name, op)
		require.NotEmpty(t, effect.Kind, op)
		require.NotEmpty(t, effect.Key, op)
	}
	for _, guard := range append(append([]specdsl.GuardSpec(nil), guards...), optional...) {
		require.NotEmpty(t, guard.Name, op)
		require.NotEmpty(t, guard.Guard, op)
	}
}
