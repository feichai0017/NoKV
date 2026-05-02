package contract

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateScriptIsDeterministic(t *testing.T) {
	require.Equal(t, GenerateScript(7, 32), GenerateScript(7, 32))
	require.NotEqual(t, GenerateScript(7, 32), GenerateScript(8, 32))
}

func TestGenerateScriptUsesRegisteredMount(t *testing.T) {
	ops := GenerateScript(3, 24)
	require.NotEmpty(t, ops)
	for _, op := range ops {
		require.Equal(t, "vol", string(op.Mount))
	}
}
