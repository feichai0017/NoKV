package audit

import "fmt"

// ReplyTraceRecord is the minimal reply-level input consumed by ccc-audit.
// It intentionally keeps the schema small so adapters can project external
// traces into a common authority-gap vocabulary.
type ReplyTraceRecord struct {
	Source                      string `json:"source,omitempty"`
	Duty                        string `json:"duty"`
	CertGeneration              uint64 `json:"cert_generation"`
	ObservedSuccessorGeneration uint64 `json:"observed_successor_generation,omitempty"`
	Accepted                    bool   `json:"accepted"`
}

// ReplyTraceAnomaly captures one reply-level legality violation discovered by
// ccc-audit.
type ReplyTraceAnomaly struct {
	Index          int    `json:"index"`
	Kind           string `json:"kind"`
	Duty           string `json:"duty,omitempty"`
	CertGeneration uint64 `json:"cert_generation"`
	Reason         string `json:"reason"`
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
			record.CertGeneration,
			record.ObservedSuccessorGeneration,
		)
	case record.Source == "etcd-lease-renew" || record.Duty == "lease_renew":
		return fmt.Sprintf(
			"accepted keepalive revision %d behind revoke revision %d",
			record.CertGeneration,
			record.ObservedSuccessorGeneration,
		)
	case record.Source == "etcd-read-index" || record.Duty == "read_index":
		return fmt.Sprintf(
			"accepted read-index revision %d behind observed successor revision %d",
			record.CertGeneration,
			record.ObservedSuccessorGeneration,
		)
	default:
		return fmt.Sprintf(
			"accepted reply generation %d behind observed successor generation %d",
			record.CertGeneration,
			record.ObservedSuccessorGeneration,
		)
	}
}

// EvaluateReplyTrace projects reply-level trace records into the current
// closure semantics. The first version only flags accepted replies whose
// generation is already illegal under the rooted closure state.
func EvaluateReplyTrace(report Report, records []ReplyTraceRecord) []ReplyTraceAnomaly {
	if len(records) == 0 {
		return nil
	}
	anomalies := make([]ReplyTraceAnomaly, 0, len(records))
	for idx, record := range records {
		if !record.Accepted {
			continue
		}
		if record.ObservedSuccessorGeneration != 0 && record.CertGeneration < record.ObservedSuccessorGeneration {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:          idx,
				Kind:           acceptedReplyBehindSuccessorKind(record),
				Duty:           record.Duty,
				CertGeneration: record.CertGeneration,
				Reason:         acceptedReplyBehindSuccessorReason(record),
			})
			continue
		}
		if report.ClosureAudit.SealGeneration != 0 && record.CertGeneration == report.ClosureAudit.SealGeneration {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:          idx,
				Kind:           "post_seal_accepted_reply",
				Duty:           record.Duty,
				CertGeneration: record.CertGeneration,
				Reason:         fmt.Sprintf("accepted reply at sealed generation %d after rooted seal", report.ClosureAudit.SealGeneration),
			})
			continue
		}
		if !report.ClosureAudit.ReplyGenerationLegal(record.CertGeneration) {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:          idx,
				Kind:           "illegal_reply_generation",
				Duty:           record.Duty,
				CertGeneration: record.CertGeneration,
				Reason:         fmt.Sprintf("accepted reply generation %d is not legal under current closure state", record.CertGeneration),
			})
		}
	}
	return anomalies
}
