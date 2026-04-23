package audit_test

import (
	"encoding/json"
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	"github.com/stretchr/testify/require"
)

func TestParseReplyTraceFormat(t *testing.T) {
	format, err := coordaudit.ParseReplyTraceFormat(" ")
	require.NoError(t, err)
	require.Equal(t, coordaudit.ReplyTraceFormatNoKV, format)

	for _, raw := range []string{
		string(coordaudit.ReplyTraceFormatNoKV),
		string(coordaudit.ReplyTraceFormatEtcdReadIndex),
		string(coordaudit.ReplyTraceFormatEtcdLeaseRenew),
		string(coordaudit.ReplyTraceFormatCRDBLeaseStart),
	} {
		format, err = coordaudit.ParseReplyTraceFormat(raw)
		require.NoError(t, err)
		require.Equal(t, coordaudit.ReplyTraceFormat(raw), format)
	}

	_, err = coordaudit.ParseReplyTraceFormat("unknown")
	require.Error(t, err)
}

func TestDecodeReplyTraceAdapters(t *testing.T) {
	nokvRecords := []coordaudit.ReplyTraceRecord{{
		Source:   "nokv",
		Duty:     "alloc_id",
		Epoch:    7,
		Accepted: true,
	}}
	raw, err := json.Marshal(nokvRecords)
	require.NoError(t, err)

	decoded, err := coordaudit.DecodeReplyTrace(raw, coordaudit.ReplyTraceFormatNoKV)
	require.NoError(t, err)
	require.Equal(t, nokvRecords, decoded)

	enveloped, err := json.Marshal(map[string]any{"records": nokvRecords})
	require.NoError(t, err)
	decoded, err = coordaudit.DecodeReplyTrace(enveloped, coordaudit.ReplyTraceFormatNoKV)
	require.NoError(t, err)
	require.Equal(t, nokvRecords, decoded)

	readIndexRaw, err := json.Marshal([]map[string]any{
		{"member_id": "n1", "read_state_generation": 9, "successor_epoch": 10, "accepted": true},
		{"member_id": "n2", "duty": "custom", "read_state_generation": 11, "successor_epoch": 11, "accepted": false},
	})
	require.NoError(t, err)
	decoded, err = coordaudit.DecodeReplyTrace(readIndexRaw, coordaudit.ReplyTraceFormatEtcdReadIndex)
	require.NoError(t, err)
	require.Len(t, decoded, 2)
	require.Equal(t, "etcd-read-index", decoded[0].Source)
	require.Equal(t, "read_index", decoded[0].Duty)
	require.Equal(t, uint64(9), decoded[0].Epoch)
	require.Equal(t, uint64(10), decoded[0].ObservedSuccessorEpoch)
	require.Equal(t, "custom", decoded[1].Duty)

	readIndexEnvelope, err := json.Marshal(map[string]any{
		"records": []map[string]any{
			{"read_state_generation": 12, "successor_epoch": 13, "accepted": true},
		},
	})
	require.NoError(t, err)
	decoded, err = coordaudit.DecodeReplyTrace(readIndexEnvelope, coordaudit.ReplyTraceFormatEtcdReadIndex)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, uint64(12), decoded[0].Epoch)

	_, err = coordaudit.DecodeReplyTrace([]byte("not-json"), coordaudit.ReplyTraceFormatEtcdReadIndex)
	require.Error(t, err)
	_, err = coordaudit.DecodeReplyTrace([]byte("[]"), coordaudit.ReplyTraceFormat("unsupported"))
	require.Error(t, err)
}
