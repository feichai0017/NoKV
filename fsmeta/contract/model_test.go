package contract

import (
	"fmt"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestModelUnlinkKeepsSessionIndexesUntilSessionLifecycleRuns(t *testing.T) {
	model := NewModel("vol")
	require.NoError(t, model.Apply(Operation{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "file",
		Inode:  10,
		Type:   fsmeta.InodeTypeFile,
		Mode:   0o600,
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     10,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Minute),
	}).Err)

	require.NoError(t, model.Apply(Operation{
		Kind:   OpUnlink,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "file",
	}).Err)

	require.NoError(t, model.CheckInvariants())
	require.NotContains(t, model.inodes, fsmeta.InodeID(10))
	require.Contains(t, model.sessions, fsmeta.SessionID("writer-1"))
	require.Contains(t, model.owners, fsmeta.InodeID(10))

	require.NoError(t, model.Apply(Operation{
		Kind:    OpCloseSession,
		Mount:   "vol",
		Session: "writer-1",
	}).Err)
	require.NoError(t, model.CheckInvariants())
	require.Empty(t, model.sessions)
	require.Empty(t, model.owners)
}

func TestModelExpiresSessionsAfterTimeAdvance(t *testing.T) {
	model := NewModel("vol")
	require.NoError(t, model.Apply(Operation{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "file",
		Inode:  10,
		Type:   fsmeta.InodeTypeFile,
		Mode:   0o600,
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     10,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Second),
	}).Err)

	require.NoError(t, model.Apply(Operation{
		Kind:      OpAdvanceTime,
		Mount:     "vol",
		AdvanceNs: int64(2 * time.Second),
	}).Err)
	result := model.Apply(Operation{Kind: OpExpireSessions, Mount: "vol", Limit: 16})

	require.NoError(t, result.Err)
	require.Equal(t, uint64(1), result.Expired)
	require.Empty(t, model.sessions)
	require.Empty(t, model.owners)
	require.NoError(t, model.CheckInvariants())
}

func TestModelExpireStaleOwnerDoesNotRemoveReusedLiveSession(t *testing.T) {
	model := NewModel("vol")
	for _, inode := range []fsmeta.InodeID{10, 11} {
		require.NoError(t, model.Apply(Operation{
			Kind:   OpCreate,
			Mount:  "vol",
			Parent: model.Root,
			Name:   fmt.Sprintf("file-%d", inode),
			Inode:  inode,
			Type:   fsmeta.InodeTypeFile,
			Mode:   0o600,
		}).Err)
	}
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     10,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Second),
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpAdvanceTime,
		Mount:     "vol",
		AdvanceNs: int64(2 * time.Second),
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     11,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Minute),
	}).Err)

	result := model.Apply(Operation{Kind: OpExpireSessions, Mount: "vol", Limit: 16})

	require.NoError(t, result.Err)
	require.Equal(t, uint64(0), result.Expired)
	require.Equal(t, fsmeta.InodeID(11), model.sessions["writer-1"].Inode)
	require.NotContains(t, model.owners, fsmeta.InodeID(10))
	require.Contains(t, model.owners, fsmeta.InodeID(11))
	require.NoError(t, model.CheckInvariants())
}

func TestModelOpenWithStaleOwnerDoesNotRemoveReusedLiveSession(t *testing.T) {
	model := NewModel("vol")
	for _, inode := range []fsmeta.InodeID{10, 11} {
		require.NoError(t, model.Apply(Operation{
			Kind:   OpCreate,
			Mount:  "vol",
			Parent: model.Root,
			Name:   fmt.Sprintf("file-%d", inode),
			Inode:  inode,
			Type:   fsmeta.InodeTypeFile,
			Mode:   0o600,
		}).Err)
	}
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     10,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Second),
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpAdvanceTime,
		Mount:     "vol",
		AdvanceNs: int64(2 * time.Second),
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     11,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Minute),
	}).Err)

	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     10,
		Session:   "writer-2",
		ExpiresNs: model.NowUnixNs + int64(time.Minute),
	}).Err)

	require.Equal(t, fsmeta.InodeID(11), model.sessions["writer-1"].Inode)
	require.Equal(t, fsmeta.InodeID(10), model.sessions["writer-2"].Inode)
	require.Equal(t, fsmeta.SessionID("writer-1"), model.owners[11].Session)
	require.Equal(t, fsmeta.SessionID("writer-2"), model.owners[10].Session)
	require.NoError(t, model.CheckInvariants())
}

func TestEquivalentErrorMatchesWrappedSentinel(t *testing.T) {
	require.True(t, EquivalentError(fmt.Errorf("wrapped: %w", fsmeta.ErrNotFound), fsmeta.ErrNotFound))
	require.False(t, EquivalentError(fmt.Errorf("wrapped: %w", fsmeta.ErrNotFound), fsmeta.ErrExists))
}
