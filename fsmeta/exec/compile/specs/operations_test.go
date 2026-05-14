package specs

import (
	"strings"
	"testing"

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
		require.NotEmpty(t, spec.LoweringName, spec.Name)
		require.NotEmpty(t, spec.OperationKind, spec.Name)
		require.NotContains(t, seenKind, spec.OperationKind)
		seenKind[spec.OperationKind] = struct{}{}
		require.NotEmpty(t, spec.Durability, spec.Name)
		require.GreaterOrEqual(t, spec.PredicateCount, -1, spec.Name)
		require.GreaterOrEqual(t, spec.EffectCount, -1, spec.Name)
		require.Contains(t, map[string]struct{}{"create": {}, "operation": {}}, spec.Emitter, spec.Name)
		if spec.Materialize != "" {
			require.NotEmpty(t, spec.ValuesType, spec.Name)
		}
	}
}
