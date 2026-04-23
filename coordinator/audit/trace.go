package audit

import "fmt"

// ReplyTraceRecord is the minimal reply-level input consumed by succession-audit.
// It intentionally keeps the schema small so adapters can project external
// traces into a common authority-gap vocabulary.
type ReplyTraceRecord struct {
	Source                 string `json:"source,omitempty"`
	Duty                   string `json:"duty"`
	Epoch                  uint64 `json:"epoch"`
	ObservedSuccessorEpoch uint64 `json:"observed_successor_epoch,omitempty"`
	Accepted               bool   `json:"accepted"`
}

// ReplyTraceAnomaly captures one reply-level legality violation discovered by
// succession-audit.
type ReplyTraceAnomaly struct {
	Index  int    `json:"index"`
	Kind   string `json:"kind"`
	Duty   string `json:"duty,omitempty"`
	Epoch  uint64 `json:"epoch"`
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
			record.Epoch,
			record.ObservedSuccessorEpoch,
		)
	case record.Source == "etcd-lease-renew" || record.Duty == "lease_renew":
		return fmt.Sprintf(
			"accepted keepalive revision %d behind revoke revision %d",
			record.Epoch,
			record.ObservedSuccessorEpoch,
		)
	case record.Source == "etcd-read-index" || record.Duty == "read_index":
		return fmt.Sprintf(
			"accepted read-index revision %d behind observed successor revision %d",
			record.Epoch,
			record.ObservedSuccessorEpoch,
		)
	default:
		return fmt.Sprintf(
			"accepted reply generation %d behind observed successor generation %d",
			record.Epoch,
			record.ObservedSuccessorEpoch,
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
		if record.ObservedSuccessorEpoch != 0 && record.Epoch < record.ObservedSuccessorEpoch {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   acceptedReplyBehindSuccessorKind(record),
				Duty:   record.Duty,
				Epoch:  record.Epoch,
				Reason: acceptedReplyBehindSuccessorReason(record),
			})
			continue
		}
		if report.TransitWitness.LegacyEpoch != 0 && record.Epoch == report.TransitWitness.LegacyEpoch {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "post_seal_accepted_reply",
				Duty:   record.Duty,
				Epoch:  record.Epoch,
				Reason: fmt.Sprintf("accepted reply at sealed generation %d after rooted seal", report.TransitWitness.LegacyEpoch),
			})
			continue
		}
		if !report.TransitWitness.ReplyGenerationLegal(record.Epoch) {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "illegal_reply_generation",
				Duty:   record.Duty,
				Epoch:  record.Epoch,
				Reason: fmt.Sprintf("accepted reply generation %d is not legal under current closure state", record.Epoch),
			})
		}
	}
	return anomalies
}
