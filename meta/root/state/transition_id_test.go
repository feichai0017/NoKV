package state_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestTransitionIDFromEventDistinguishesPeerAddAndRemove(t *testing.T) {
	add := rootstate.TransitionIDFromEvent(rootevent.PeerAdditionPlanned(11, 2, 201, testDescriptor(11, nil, nil)))
	remove := rootstate.TransitionIDFromEvent(rootevent.PeerRemovalPlanned(11, 2, 201, testDescriptor(11, nil, nil)))

	require.Equal(t, "peer:11:add:2:201", add)
	require.Equal(t, "peer:11:remove:2:201", remove)
	require.NotEqual(t, add, remove)
}
