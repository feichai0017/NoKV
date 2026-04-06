package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartRaftTickLoopNilNodeNoop(t *testing.T) {
	var node *Node
	require.NotPanics(t, func() {
		node.startRaftTickLoop(time.Millisecond)
	})
}

func TestStartRaftTickLoopWithoutStoreNoop(t *testing.T) {
	node := &Node{}
	node.startRaftTickLoop(time.Millisecond)
	require.Nil(t, node.tickStop)
	require.Zero(t, node.tickEvery)
}

func TestStartRaftTickLoopNonPositiveIntervalNoop(t *testing.T) {
	node := &Node{}
	node.startRaftTickLoop(0)
	require.Nil(t, node.tickStop)
	require.Zero(t, node.tickEvery)
}
