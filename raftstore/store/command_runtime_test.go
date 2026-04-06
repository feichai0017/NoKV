package store

import (
	"context"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommandRuntimeHelpers(t *testing.T) {
	var nilStore *Store
	require.Nil(t, nilStore.commandPipe())
	require.Nil(t, nilStore.commandApply())
	require.Zero(t, nilStore.commandWait())
	require.NotNil(t, nilStore.runtimeContext())

	applier := func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{}, nil
	}
	st := NewStore(Config{CommandApplier: applier, CommandTimeout: 2 * time.Second})
	t.Cleanup(func() { st.Close() })

	require.NotNil(t, st.commandPipe())
	require.NotNil(t, st.commandApply())
	require.Equal(t, 2*time.Second, st.commandWait())
	require.Equal(t, context.Background().Err(), st.runtimeContext().Err())

	empty := NewStore(Config{})
	t.Cleanup(func() { empty.Close() })
	require.Nil(t, empty.commandApply())
}
