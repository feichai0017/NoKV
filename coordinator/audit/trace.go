package audit

import "fmt"

// ReplyTraceRecord is the minimal reply-level input consumed by the audit subsystem.
// It intentionally keeps the schema small so adapters can project external
// traces into a common authority-gap vocabulary.
type ReplyTraceRecord struct {
	Source               string `json:"source,omitempty"`
	Duty                 string `json:"duty"`
	GrantID              string `json:"grant_id,omitempty"`
	Era                  uint64 `json:"era"`
	UsageUpper           uint64 `json:"usage_upper,omitempty"`
	GrantUpper           uint64 `json:"grant_upper,omitempty"`
	ObservedSuccessorEra uint64 `json:"observed_successor_era,omitempty"`
	Accepted             bool   `json:"accepted"`
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
		if report.RetiredEraFloor != 0 && record.Era <= report.RetiredEraFloor {
			anomalies = append(anomalies, ReplyTraceAnomaly{
				Index:  idx,
				Kind:   "accepted_retired_era_reply",
				Duty:   record.Duty,
				Era:    record.Era,
				Reason: fmt.Sprintf("accepted reply era %d at or below retired era floor %d", record.Era, report.RetiredEraFloor),
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
