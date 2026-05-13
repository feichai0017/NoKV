package audit

import (
	"fmt"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

// ReplyTraceRecord is the minimal reply-level input consumed by the audit subsystem.
// It intentionally keeps the schema small so adapters can project external
// traces into a common authority-gap vocabulary.
type ReplyTraceRecord struct {
	Source                  string `json:"source,omitempty"`
	Duty                    string `json:"duty"`
	GrantID                 string `json:"grant_id,omitempty"`
	Era                     uint64 `json:"era"`
	UsageUpper              uint64 `json:"usage_upper,omitempty"`
	EvidenceUsageUpper      uint64 `json:"evidence_usage_upper,omitempty"`
	GrantUpper              uint64 `json:"grant_upper,omitempty"`
	ObservedRetiredEraFloor uint64 `json:"observed_retired_era_floor,omitempty"`
	ObservedSuccessorEra    uint64 `json:"observed_successor_era,omitempty"`
	ServedUnixNano          int64  `json:"served_unix_nano,omitempty"`
	GrantExpiresUnixNano    int64  `json:"grant_expires_unix_nano,omitempty"`
	MaxReplyAgeNano         int64  `json:"max_reply_age_nano,omitempty"`
	EvidencePresent         bool   `json:"evidence_present,omitempty"`
	SignatureValid          bool   `json:"signature_valid,omitempty"`
	Accepted                bool   `json:"accepted"`
}

// ReplyTraceAnomaly captures one reply-level legality violation discovered by
// the audit subsystem.
type ReplyTraceAnomaly struct {
	Index  int    `json:"index"`
	Kind   string `json:"kind"`
	Duty   string `json:"duty,omitempty"`
	Era    uint64 `json:"era"`
	Reason string `json:"reason"`
}

func acceptedReplyBehindSuccessorKind(record ReplyTraceRecord) string {
	switch {
	case record.Source == "crdb-lease-start" || record.Duty == "lease_start_coverage":
		return "lease_start_coverage_violation"
	case record.Source == "etcd-lease-renew" || record.Duty == "lease_renew":
		return "accepted_keepalive_success_after_revoke"
	case record.Source == "etcd-read-index" || record.Duty == "read_index":
		return "accepted_read_index_behind_successor"
	default:
		return "accepted_reply_behind_successor"
	}
}

func acceptedReplyBehindSuccessorReason(record ReplyTraceRecord) string {
	switch {
	case record.Source == "crdb-lease-start" || record.Duty == "lease_start_coverage":
		return fmt.Sprintf(
			"accepted successor lease_start %d behind carried served timestamp %d",
			record.Era,
			record.ObservedSuccessorEra,
		)
	case record.Source == "etcd-lease-renew" || record.Duty == "lease_renew":
		return fmt.Sprintf(
			"accepted keepalive revision %d behind revoke revision %d",
			record.Era,
			record.ObservedSuccessorEra,
		)
	case record.Source == "etcd-read-index" || record.Duty == "read_index":
		return fmt.Sprintf(
			"accepted read-index revision %d behind observed successor revision %d",
			record.Era,
			record.ObservedSuccessorEra,
		)
	default:
		return fmt.Sprintf(
			"accepted reply era %d behind observed successor era %d",
			record.Era,
			record.ObservedSuccessorEra,
		)
	}
}

// EvaluateReplyTrace projects reply-level trace records into the current
// finality semantics. The first version only flags accepted replies whose
// era is already illegal under the rooted grant retirement state.
func EvaluateReplyTrace(report Report, records []ReplyTraceRecord) []ReplyTraceAnomaly {
	if len(records) == 0 {
		return nil
	}
	anomalies := make([]ReplyTraceAnomaly, 0, len(records))
	retired := make(map[string]uint64, len(report.RetiredGrants))
	for _, retirement := range report.RetiredGrants {
		if retirement.GrantID != "" {
			retired[retirement.GrantID] = retirement.Era
		}
	}
	for idx, record := range records {
		if !record.Accepted {
			continue
		}
		if requiresEunomiaEvidence(record) {
			if !record.EvidencePresent {
				anomalies = append(anomalies, ReplyTraceAnomaly{
					Index:  idx,
					Kind:   "accepted_missing_evidence",
					Duty:   record.Duty,
					Era:    record.Era,
					Reason: "accepted NoKV authority reply without AuthorityEvidence",
				})
				continue
			}
			if !record.SignatureValid {
				anomalies = append(anomalies, ReplyTraceAnomaly{
					Index:  idx,
					Kind:   "accepted_invalid_signature",
					Duty:   record.Duty,
					Era:    record.Era,
					Reason: "accepted NoKV authority reply without a valid root grant signature",
				})
				continue
			}
		}
		if record.EvidenceUsageUpper != 0 && record.UsageUpper > record.EvidenceUsageUpper {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "reply_usage_not_covered_by_evidence",
				Duty:   record.Duty,
				Era:    record.Era,
				Reason: fmt.Sprintf("accepted usage %d outside evidence usage %d", record.UsageUpper, record.EvidenceUsageUpper),
			})
			continue
		}
		if record.GrantUpper != 0 && record.EvidenceUsageUpper > record.GrantUpper {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "evidence_outside_grant",
				Duty:   record.Duty,
				Era:    record.Era,
				Reason: fmt.Sprintf("accepted evidence usage %d outside grant upper bound %d", record.EvidenceUsageUpper, record.GrantUpper),
			})
			continue
		}
		if record.GrantUpper != 0 && record.UsageUpper > record.GrantUpper {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "reply_outside_grant",
				Duty:   record.Duty,
				Era:    record.Era,
				Reason: fmt.Sprintf("accepted usage %d outside grant upper bound %d", record.UsageUpper, record.GrantUpper),
			})
			continue
		}
		if record.GrantExpiresUnixNano > 0 && record.ServedUnixNano > record.GrantExpiresUnixNano {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "reply_served_after_grant_expiry",
				Duty:   record.Duty,
				Era:    record.Era,
				Reason: fmt.Sprintf("accepted reply served at %d after grant expiry %d", record.ServedUnixNano, record.GrantExpiresUnixNano),
			})
			continue
		}
		if record.MaxReplyAgeNano > 0 && report.NowUnixNano > 0 && record.ServedUnixNano > 0 &&
			report.NowUnixNano-record.ServedUnixNano > record.MaxReplyAgeNano {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "reply_exceeds_max_age",
				Duty:   record.Duty,
				Era:    record.Era,
				Reason: fmt.Sprintf("accepted reply age %d exceeds max age %d", report.NowUnixNano-record.ServedUnixNano, record.MaxReplyAgeNano),
			})
			continue
		}
		if record.GrantID != "" {
			if retiredEra, ok := retired[record.GrantID]; ok {
				anomalies = append(anomalies, ReplyTraceAnomaly{
					Index:  idx,
					Kind:   "accepted_retired_grant_reply",
					Duty:   record.Duty,
					Era:    record.Era,
					Reason: fmt.Sprintf("accepted reply from retired grant %s era %d", record.GrantID, retiredEra),
				})
				continue
			}
		}
		retiredFloor := maxUint64(report.RetiredEraFloorFor(rootprotoDuty(record.Duty), rootprotoGlobalScope()), record.ObservedRetiredEraFloor)
		// Trace validation uses both root audit state and the floor attached to
		// the reply. Either source is enough to prove the reply came from an
		// era that clients should already reject.
		if retiredFloor != 0 && record.Era <= retiredFloor {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "accepted_retired_era_reply",
				Duty:   record.Duty,
				Era:    record.Era,
				Reason: fmt.Sprintf("accepted reply era %d at or below retired era floor %d", record.Era, retiredFloor),
			})
			continue
		}
		if record.ObservedSuccessorEra != 0 && record.Era < record.ObservedSuccessorEra {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   acceptedReplyBehindSuccessorKind(record),
				Duty:   record.Duty,
				Era:    record.Era,
				Reason: acceptedReplyBehindSuccessorReason(record),
			})
			continue
		}
	}
	return anomalies
}

// rootprotoDuty converts the trace's text duty name into the protocol key used
// by scoped retired-floor lookups.
func rootprotoDuty(duty string) rootproto.DutyID {
	return rootproto.DutyID(duty)
}

// rootprotoGlobalScope is the current scope for built-in coordinator duties.
// Keeping it explicit makes future non-global duties easier to audit.
func rootprotoGlobalScope() rootproto.DutyScope {
	return rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}
}

func requiresEunomiaEvidence(record ReplyTraceRecord) bool {
	source := record.Source
	return source == "" || source == "nokv"
}

func maxUint64(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}
