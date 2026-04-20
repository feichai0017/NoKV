package audit_test

import (
	"encoding/json"
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestEvaluateReplyTrace(t *testing.T) {
	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 2,
		DutyMask:       rootproto.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
		SealedAtCursor: rootstate.Cursor{Term: 1, Index: 9},
	}
	sealDigest := rootstate.CoordinatorSealDigest(seal)
	report := coordaudit.BuildReport(coordstorage.Snapshot{
		CatchUpState: coordstorage.CatchUpStateFresh,
		Allocator: coordstorage.AllocatorState{
			IDCurrent: 12,
			TSCurrent: 34,
		},
		CoordinatorLease: rootstate.CoordinatorLease{
			HolderID:          "c1",
			ExpiresUnixNano:   2_000,
			CertGeneration:    3,
			DutyMask:          rootproto.CoordinatorDutyMaskDefault,
			PredecessorDigest: sealDigest,
		},
		CoordinatorSeal: seal,
		CoordinatorClosure: rootstate.CoordinatorClosure{
			HolderID:            "c1",
			SealGeneration:      2,
			SuccessorGeneration: 3,
			SealDigest:          sealDigest,
			Stage:               rootproto.CoordinatorClosureStageReattached,
		},
		Descriptors: map[uint64]descriptor.Descriptor{
			1: {RegionID: 1, RootEpoch: 7},
		},
	}, "c1", 1_000)

	anomalies := coordaudit.EvaluateReplyTrace(report, []coordaudit.ReplyTraceRecord{
		{Duty: "allocid", CertGeneration: 2, Accepted: true},
		{Duty: "allocid", CertGeneration: 1, Accepted: true},
		{Duty: "allocid", CertGeneration: 2, Accepted: false},
		{Duty: "allocid", CertGeneration: 3, Accepted: true},
	})
	require.Len(t, anomalies, 1)
	require.Equal(t, "post_seal_accepted_reply", anomalies[0].Kind)
	require.Equal(t, uint64(2), anomalies[0].CertGeneration)
}

func TestEvaluateReplyTraceFlagsAcceptedReplyBehindSuccessor(t *testing.T) {
	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, []coordaudit.ReplyTraceRecord{
		{
			Source:                      "etcd-read-index",
			Duty:                        "read_index",
			CertGeneration:              1,
			ObservedSuccessorGeneration: 2,
			Accepted:                    true,
		},
		{
			Source:                      "etcd-read-index",
			Duty:                        "read_index",
			CertGeneration:              2,
			ObservedSuccessorGeneration: 2,
			Accepted:                    true,
		},
	})
	require.Len(t, anomalies, 1)
	require.Equal(t, "accepted_read_index_behind_successor", anomalies[0].Kind)
	require.Equal(t, uint64(1), anomalies[0].CertGeneration)
}

func TestDecodeEtcdLeaseRenewTraceFlagsAcceptedReplyBehindRevoke(t *testing.T) {
	data, err := json.Marshal([]map[string]any{
		{
			"member_id":         "n1",
			"response_revision": 7,
			"revoke_revision":   8,
			"accepted":          true,
		},
		{
			"member_id":         "n1",
			"response_revision": 8,
			"revoke_revision":   8,
			"accepted":          true,
		},
	})
	require.NoError(t, err)

	records, err := coordaudit.DecodeReplyTrace(data, coordaudit.ReplyTraceFormatEtcdLeaseRenew)
	require.NoError(t, err)

	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, records)
	require.Len(t, anomalies, 1)
	require.Equal(t, "accepted_keepalive_success_after_revoke", anomalies[0].Kind)
	require.Equal(t, "lease_renew", anomalies[0].Duty)
	require.Equal(t, uint64(7), anomalies[0].CertGeneration)
}

func TestDecodeCRDBLeaseStartTraceFlagsCoverageViolation(t *testing.T) {
	data, err := json.Marshal([]map[string]any{
		{
			"key":                   "k",
			"successor_lease_start": 103,
			"served_timestamp":      105,
			"accepted":              true,
		},
		{
			"key":                   "k",
			"successor_lease_start": 106,
			"served_timestamp":      105,
			"accepted":              true,
		},
	})
	require.NoError(t, err)

	records, err := coordaudit.DecodeReplyTrace(data, coordaudit.ReplyTraceFormatCRDBLeaseStart)
	require.NoError(t, err)

	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, records)
	require.Len(t, anomalies, 1)
	require.Equal(t, "lease_start_coverage_violation", anomalies[0].Kind)
	require.Equal(t, "lease_start_coverage", anomalies[0].Duty)
	require.Equal(t, uint64(103), anomalies[0].CertGeneration)
	require.Contains(t, anomalies[0].Reason, "served timestamp 105")
}
