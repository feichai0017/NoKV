package lsm

import (
	"bytes"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/wal"
	"github.com/stretchr/testify/require"
)

func TestOpenMemTableReplayWithTypedRecords(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	const segID = uint32(77)
	require.NoError(t, lsm.wal.SwitchSegment(segID, true))

	entry := kv.NewEntry(kv.KeyWithTs([]byte("replay-key"), 9), []byte("replay-value"))
	defer entry.DecrRef()
	var buf bytes.Buffer
	payload, err := kv.EncodeEntry(&buf, entry)
	require.NoError(t, err)

	infos, err := lsm.wal.AppendRecords(
		wal.Record{Type: wal.RecordTypeRaftState, Payload: []byte("ignored")},
		wal.Record{Type: wal.RecordTypeEntry, Payload: payload},
	)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.NoError(t, lsm.wal.Sync())

	mt, err := lsm.openMemTable(uint64(segID))
	require.NoError(t, err)
	require.NotNil(t, mt)
	require.Equal(t, segID, mt.segmentID)
	require.Equal(t, uint64(9), mt.maxVersion)
	require.Equal(t, int64(infos[1].Length)+8, mt.walSize)

	got, err := mt.Get(entry.Key)
	require.NoError(t, err)
	require.Equal(t, []byte("replay-value"), got.Value)
	got.DecrRef()
}

func TestOpenMemTableReplayDecodeError(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	const segID = uint32(78)
	require.NoError(t, lsm.wal.SwitchSegment(segID, true))
	_, err := lsm.wal.AppendRecords(wal.Record{
		Type:    wal.RecordTypeEntry,
		Payload: []byte("bad-entry-payload"),
	})
	require.NoError(t, err)
	require.NoError(t, lsm.wal.Sync())

	_, err = lsm.openMemTable(uint64(segID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "while updating skiplist")
}
