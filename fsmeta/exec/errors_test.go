package exec

import (
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
)

func TestExecErrorsExposeStableKinds(t *testing.T) {
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errRunnerRequired))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errAuditorRunnerRequired))
	require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(errSubtreeHandoffWithoutFrontier))
}
