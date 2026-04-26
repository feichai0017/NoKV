package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestOpenMemTableReplayWithTypedRecords(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	const segID = uint32(77)
	shard := lsm.shards[0]
	require.NoError(t, shard.wal.SwitchSegment(segID, true))

	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("replay-key"), 9), []byte("replay-value"))
	defer entry.DecrRef()
	payload, err := wal.EncodeEntryBatch([]*kv.Entry{entry})
	require.NoError(t, err)

	infos, err := shard.wal.AppendRecords(wal.DurabilityBuffered,
		wal.Record{Type: wal.RecordTypeRaftState, Payload: []byte("ignored")},
		wal.Record{Type: wal.RecordTypeEntryBatch, Payload: payload},
	)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.NoError(t, shard.wal.Sync())

	mt, err := lsm.openMemTable(shard, uint64(segID))
	require.NoError(t, err)
	require.NotNil(t, mt)
	require.Equal(t, segID, mt.segmentID)
	require.Equal(t, uint64(9), mt.maxVersion.Load())
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
	shard := lsm.shards[0]
	require.NoError(t, shard.wal.SwitchSegment(segID, true))
	_, err := shard.wal.AppendRecords(wal.DurabilityBuffered, wal.Record{
		Type:    wal.RecordTypeEntryBatch,
		Payload: []byte("bad-entry-payload"),
	})
	require.NoError(t, err)
	require.NoError(t, shard.wal.Sync())

	_, err = lsm.openMemTable(shard, uint64(segID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "while updating memtable index")
}

func TestMemTableSetRejectsInvalidInput(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	mt := lsm.shards[0].memTable
	require.NotNil(t, mt)

	require.ErrorIs(t, mt.Set(nil), utils.ErrEmptyKey)
	require.ErrorIs(t, mt.Set(&kv.Entry{}), utils.ErrEmptyKey)

	var nilMem *memTable
	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("k"), 1), []byte("v"))
	defer entry.DecrRef()
	require.Error(t, nilMem.Set(entry))
}

func TestMemTableCanReserveUsesWALSize(t *testing.T) {
	var mt memTable

	require.True(t, mt.canReserve(10, 20))

	mt.walSize.Store(5)
	require.True(t, mt.canReserve(15, 20))
	require.False(t, mt.canReserve(16, 20))

	mt.walSize.Store(21)
	require.False(t, mt.canReserve(1, 20))
}
