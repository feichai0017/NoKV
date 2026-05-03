package contract

import (
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
)

func TestContractErrorsExposeStableKinds(t *testing.T) {
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errExecutorRequired))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errModelRequired))
}
