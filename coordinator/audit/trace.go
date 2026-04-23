package audit

import "fmt"

// ReplyTraceRecord is the minimal reply-level input consumed by succession-audit.
// It intentionally keeps the schema small so adapters can project external
// traces into a common authority-gap vocabulary.
type ReplyTraceRecord struct {
	Source               string `json:"source,omitempty"`
	Duty                 string `json:"duty"`
	Era                  uint64 `json:"era"`
	ObservedSuccessorEra uint64 `json:"observed_successor_era,omitempty"`
	Accepted             bool   `json:"accepted"`
}

// ReplyTraceAnomaly captures one reply-level legality violation discovered by
// succession-audit.
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
// era is already illegal under the rooted handover state.
func EvaluateReplyTrace(report Report, records []ReplyTraceRecord) []ReplyTraceAnomaly {
	if len(records) == 0 {
		return nil
	}
	anomalies := make([]ReplyTraceAnomaly, 0, len(records))
	for idx, record := range records {
		if !record.Accepted {
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
		if report.HandoverWitness.LegacyEra != 0 && record.Era == report.HandoverWitness.LegacyEra {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "post_seal_accepted_reply",
				Duty:   record.Duty,
				Era:    record.Era,
				Reason: fmt.Sprintf("accepted reply at sealed era %d after rooted seal", report.HandoverWitness.LegacyEra),
			})
			continue
		}
		if !report.HandoverWitness.ReplyEraLegal(record.Era) {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "illegal_reply_era",
				Duty:   record.Duty,
				Era:    record.Era,
				Reason: fmt.Sprintf("accepted reply era %d is not legal under current handover state", record.Era),
			})
		}
	}
	return anomalies
}
