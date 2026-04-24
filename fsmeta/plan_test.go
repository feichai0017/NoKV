package fsmeta

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanCreateTouchesDentryAndInode(t *testing.T) {
	plan, err := PlanCreate(CreateRequest{
		Mount:  "vol",
		Parent: RootInode,
		Name:   "file",
		Inode:  22,
	})
	require.NoError(t, err)

	dentry, err := EncodeDentryKey("vol", RootInode, "file")
	require.NoError(t, err)
	inode, err := EncodeInodeKey("vol", 22)
	require.NoError(t, err)

	require.Equal(t, OperationCreate, plan.Kind)
	require.Equal(t, dentry, plan.PrimaryKey)
	require.Equal(t, [][]byte{dentry}, plan.ReadKeys)
	require.Equal(t, [][]byte{dentry, inode}, plan.MutateKeys)
}

func TestPlanReadDirScansOnlyDirectoryPrefix(t *testing.T) {
	plan, err := PlanReadDir(ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  128,
	})
	require.NoError(t, err)

	prefix, err := EncodeDentryPrefix("vol", 7)
	require.NoError(t, err)
	require.Equal(t, OperationReadDir, plan.Kind)
	require.Equal(t, prefix, plan.PrimaryKey)
	require.Equal(t, prefix, plan.StartKey)
	require.Equal(t, uint32(128), plan.Limit)
	require.Equal(t, [][]byte{prefix}, plan.ReadPrefixes)
	require.Empty(t, plan.ReadKeys)
	require.Empty(t, plan.MutateKeys)
}

func TestPlanReadDirDefaultsLimit(t *testing.T) {
	plan, err := PlanReadDir(ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
	})
	require.NoError(t, err)
	require.Equal(t, DefaultReadDirLimit, plan.Limit)
}

func TestPlanReadDirStartAfterBecomesInclusiveSeekKey(t *testing.T) {
	plan, err := PlanReadDir(ReadDirRequest{
		Mount:      "vol",
		Parent:     7,
		StartAfter: "a",
		Limit:      64,
	})
	require.NoError(t, err)

	cursor, err := EncodeDentryKey("vol", 7, "a")
	require.NoError(t, err)
	nextName, err := EncodeDentryKey("vol", 7, "aa")
	require.NoError(t, err)

	require.Equal(t, append(cursor, 0), plan.StartKey)
	require.Positive(t, bytes.Compare(plan.StartKey, cursor))
	require.Negative(t, bytes.Compare(plan.StartKey, nextName))
	require.Equal(t, uint32(64), plan.Limit)
}

func TestPlanReadDirRejectsOversizedPage(t *testing.T) {
	_, err := PlanReadDir(ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  MaxReadDirLimit + 1,
	})
	require.ErrorIs(t, err, ErrInvalidPageSize)
}

func TestPlanRenameTouchesSourceAndDestinationDentries(t *testing.T) {
	plan, err := PlanRename(RenameRequest{
		Mount:      "vol",
		FromParent: 2,
		FromName:   "old",
		ToParent:   3,
		ToName:     "new",
	})
	require.NoError(t, err)

	from, err := EncodeDentryKey("vol", 2, "old")
	require.NoError(t, err)
	to, err := EncodeDentryKey("vol", 3, "new")
	require.NoError(t, err)

	require.Equal(t, OperationRename, plan.Kind)
	require.Equal(t, from, plan.PrimaryKey)
	require.Equal(t, [][]byte{from, to}, plan.ReadKeys)
	require.Equal(t, [][]byte{from, to}, plan.MutateKeys)
}

func TestPlanRenameRejectsNoop(t *testing.T) {
	_, err := PlanRename(RenameRequest{
		Mount:      "vol",
		FromParent: 2,
		FromName:   "same",
		ToParent:   2,
		ToName:     "same",
	})
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestPlansCloneKeys(t *testing.T) {
	plan, err := PlanLookup(LookupRequest{Mount: "vol", Parent: RootInode, Name: "file"})
	require.NoError(t, err)
	original := append([]byte(nil), plan.PrimaryKey...)

	plan.PrimaryKey[0] ^= 0xff
	require.True(t, bytes.Equal(original, plan.ReadKeys[0]))
}

func TestPlanOpenWriteSessionTouchesInodeAndSession(t *testing.T) {
	plan, err := PlanOpenWriteSession(OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "client-1",
	})
	require.NoError(t, err)

	inode, err := EncodeInodeKey("vol", 44)
	require.NoError(t, err)
	session, err := EncodeSessionKey("vol", "client-1")
	require.NoError(t, err)

	require.Equal(t, OperationOpenWriteSession, plan.Kind)
	require.Equal(t, session, plan.PrimaryKey)
	require.Equal(t, [][]byte{inode}, plan.ReadKeys)
	require.Equal(t, [][]byte{session}, plan.MutateKeys)
}
