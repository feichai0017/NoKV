package store

import (
	"context"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommandRuntimeHelpers(t *testing.T) {
	var nilStore *Store
	require.NotNil(t, nilStore.runtimeContext())
	require.Error(t, nilStore.applyEntries(nil))

	applier := func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{}, nil
	}
	st := NewStore(Config{CommandApplier: applier, CommandTimeout: 2 * time.Second})
	t.Cleanup(func() { st.Close() })

	require.NotNil(t, st.cmds)
	require.Equal(t, 2*time.Second, st.cmds.timeout)
	require.Equal(t, context.Background().Err(), st.runtimeContext().Err())
	require.NoError(t, st.applyEntries([]myraft.Entry{}))

	empty := NewStore(Config{})
	t.Cleanup(func() { empty.Close() })
	require.NoError(t, empty.applyEntries([]myraft.Entry{}))
}
