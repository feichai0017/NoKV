package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
	"github.com/stretchr/testify/require"
)

func TestOpenMemTableReplayWithTypedRecords(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	const segID = uint32(77)
	require.NoError(t, lsm.wal.SwitchSegment(segID, true))

	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("replay-key"), 9), []byte("replay-value"))
	defer entry.DecrRef()
	payload, err := wal.EncodeEntryBatch([]*kv.Entry{entry})
	require.NoError(t, err)

	infos, err := lsm.wal.AppendRecords(
		wal.Record{Type: wal.RecordTypeRaftState, Payload: []byte("ignored")},
		wal.Record{Type: wal.RecordTypeEntryBatch, Payload: payload},
	)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.NoError(t, lsm.wal.Sync())

	mt, err := lsm.openMemTable(uint64(segID))
	require.NoError(t, err)
	require.NotNil(t, mt)
	require.Equal(t, segID, mt.segmentID)
	require.Equal(t, uint64(9), mt.maxVersion)
	require.Equal(t, int64(infos[1].Length)+8, mt.walSize.Load())

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
		Type:    wal.RecordTypeEntryBatch,
		Payload: []byte("bad-entry-payload"),
	})
	require.NoError(t, err)
	require.NoError(t, lsm.wal.Sync())

	_, err = lsm.openMemTable(uint64(segID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "while updating skiplist")
}

func TestMemTableSetRejectsInvalidInput(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	mt := lsm.memTable
	require.NotNil(t, mt)

	require.ErrorIs(t, mt.Set(nil), utils.ErrEmptyKey)
	require.ErrorIs(t, mt.Set(&kv.Entry{}), utils.ErrEmptyKey)

	var nilMem *memTable
	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("k"), 1), []byte("v"))
	defer entry.DecrRef()
	require.Error(t, nilMem.Set(entry))
}

func TestMemTableReservationAccounting(t *testing.T) {
	var mt memTable

	require.True(t, mt.canReserve(10, 20))
	require.True(t, mt.tryReserve(10, 20))
	require.False(t, mt.tryReserve(11, 20))

	mt.walSize.Store(5)
	require.False(t, mt.canReserve(6, 20))
	require.False(t, mt.tryReserve(6, 20))

	mt.releaseReserve(10)
	require.Equal(t, int64(0), mt.reservedSize.Load())
	require.True(t, mt.tryReserve(15, 20))
	mt.releaseReserve(15)
	require.Equal(t, int64(0), mt.reservedSize.Load())
}

func TestMemTableReservationUnderflowPanics(t *testing.T) {
	var mt memTable
	require.PanicsWithValue(t, "lsm: memtable reservation underflow", func() {
		mt.releaseReserve(1)
	})
}
