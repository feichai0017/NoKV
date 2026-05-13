package audit_test

import (
	"encoding/json"
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

func TestEvaluateReplyTraceFlagsRetiredGrantReply(t *testing.T) {
	report := coordaudit.BuildReport(rootview.Snapshot{
		RetiredGrants: []rootproto.GrantRetirement{
			{GrantID: "g1", HolderID: "c1", Era: 1, Mode: rootproto.GrantRetirementSealedExact, InheritedByGrantID: "g2"},
		},
	}, "c2", 1_000)

	anomalies := coordaudit.EvaluateReplyTrace(report, []coordaudit.ReplyTraceRecord{
		{Source: "nokv", Duty: "alloc_id", GrantID: "g1", Era: 1, EvidencePresent: true, SignatureValid: true, Accepted: true},
		{Source: "nokv", Duty: "alloc_id", GrantID: "g2", Era: 2, EvidencePresent: true, SignatureValid: true, Accepted: true},
	})
	require.Len(t, anomalies, 1)
	require.Equal(t, "accepted_retired_grant_reply", anomalies[0].Kind)
	require.Equal(t, uint64(1), anomalies[0].Era)
}

func TestEvaluateReplyTraceFlagsReplyOutsideGrant(t *testing.T) {
	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, []coordaudit.ReplyTraceRecord{
		{Source: "nokv", Duty: "alloc_id", Era: 1, UsageUpper: 11, EvidenceUsageUpper: 11, GrantUpper: 10, EvidencePresent: true, SignatureValid: true, Accepted: true},
		{Source: "nokv", Duty: "alloc_id", Era: 1, UsageUpper: 10, EvidenceUsageUpper: 10, GrantUpper: 10, EvidencePresent: true, SignatureValid: true, Accepted: true},
	})
	require.Len(t, anomalies, 1)
	require.Equal(t, "evidence_outside_grant", anomalies[0].Kind)
}

func TestEvaluateReplyTraceFlagsReplyUsageNotCoveredByEvidence(t *testing.T) {
	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, []coordaudit.ReplyTraceRecord{
		{Source: "nokv", Duty: "alloc_id", Era: 1, UsageUpper: 900, EvidenceUsageUpper: 100, GrantUpper: 1_000, EvidencePresent: true, SignatureValid: true, Accepted: true},
	})
	require.Len(t, anomalies, 1)
	require.Equal(t, "reply_usage_not_covered_by_evidence", anomalies[0].Kind)
}

func TestEvaluateReplyTraceFlagsMissingOrInvalidEvidence(t *testing.T) {
	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, []coordaudit.ReplyTraceRecord{
		{Source: "nokv", Duty: "tso", Era: 1, Accepted: true},
		{Source: "nokv", Duty: "tso", Era: 2, EvidencePresent: true, SignatureValid: false, Accepted: true},
	})
	require.Len(t, anomalies, 2)
	require.Equal(t, "accepted_missing_evidence", anomalies[0].Kind)
	require.Equal(t, "accepted_invalid_signature", anomalies[1].Kind)
}

func TestEvaluateReplyTraceFlagsClockViolations(t *testing.T) {
	report := coordaudit.Report{NowUnixNano: 2_000}
	anomalies := coordaudit.EvaluateReplyTrace(report, []coordaudit.ReplyTraceRecord{
		{Source: "nokv", Duty: "tso", Era: 1, ServedUnixNano: 1_100, GrantExpiresUnixNano: 1_000, EvidencePresent: true, SignatureValid: true, Accepted: true},
		{Source: "nokv", Duty: "tso", Era: 2, ServedUnixNano: 1_000, GrantExpiresUnixNano: 3_000, MaxReplyAgeNano: 500, EvidencePresent: true, SignatureValid: true, Accepted: true},
	})
	require.Len(t, anomalies, 2)
	require.Equal(t, "reply_served_after_grant_expiry", anomalies[0].Kind)
	require.Equal(t, "reply_exceeds_max_age", anomalies[1].Kind)
}

func TestEvaluateReplyTraceUsesObservedRetiredFloor(t *testing.T) {
	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, []coordaudit.ReplyTraceRecord{
		{Source: "nokv", Duty: "region_lookup", Era: 3, ObservedRetiredEraFloor: 3, EvidencePresent: true, SignatureValid: true, Accepted: true},
	})
	require.Len(t, anomalies, 1)
	require.Equal(t, "accepted_retired_era_reply", anomalies[0].Kind)
}

// TestEvaluateReplyTraceUsesDutyScopedReportRetiredFloor ensures audit traces do
// not accuse TSO replies merely because alloc_id has already compacted a higher
// retired era.
func TestEvaluateReplyTraceUsesDutyScopedReportRetiredFloor(t *testing.T) {
	report := coordaudit.BuildReport(rootview.Snapshot{
		RetiredEraFloor: 22,
		RetiredEraFloors: []rootproto.AuthorityRetiredEraFloor{{
			DutyID:          rootproto.DutyAllocID,
			Scope:           rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
			RetiredEraFloor: 22,
		}},
	}, "c1", 1_000)

	anomalies := coordaudit.EvaluateReplyTrace(report, []coordaudit.ReplyTraceRecord{
		{Source: "nokv", Duty: "tso", Era: 9, EvidencePresent: true, SignatureValid: true, Accepted: true},
		{Source: "nokv", Duty: "alloc_id", Era: 9, EvidencePresent: true, SignatureValid: true, Accepted: true},
	})
	require.Len(t, anomalies, 1)
	require.Equal(t, "accepted_retired_era_reply", anomalies[0].Kind)
	require.Equal(t, "alloc_id", anomalies[0].Duty)
}

func TestEvaluateReplyTraceFlagsAcceptedReplyBehindSuccessor(t *testing.T) {
	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, []coordaudit.ReplyTraceRecord{
		{
			Source:               "etcd-read-index",
			Duty:                 "read_index",
			Era:                  1,
			ObservedSuccessorEra: 2,
			Accepted:             true,
		},
		{
			Source:               "etcd-read-index",
			Duty:                 "read_index",
			Era:                  2,
			ObservedSuccessorEra: 2,
			Accepted:             true,
		},
	})
	require.Len(t, anomalies, 1)
	require.Equal(t, "accepted_read_index_behind_successor", anomalies[0].Kind)
	require.Equal(t, uint64(1), anomalies[0].Era)
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
	require.Equal(t, uint64(7), anomalies[0].Era)
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
	require.Equal(t, uint64(103), anomalies[0].Era)
	require.Contains(t, anomalies[0].Reason, "served timestamp 105")
}
