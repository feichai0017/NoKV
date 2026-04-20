package audit

import (
	"encoding/json"
	"fmt"
	"strings"
)

type ReplyTraceFormat string

const (
	ReplyTraceFormatNoKV           ReplyTraceFormat = "nokv"
	ReplyTraceFormatEtcdReadIndex  ReplyTraceFormat = "etcd-read-index"
	ReplyTraceFormatEtcdLeaseRenew ReplyTraceFormat = "etcd-lease-renew"
	ReplyTraceFormatCRDBLeaseStart ReplyTraceFormat = "crdb-lease-start"
)

func ParseReplyTraceFormat(raw string) (ReplyTraceFormat, error) {
	format := ReplyTraceFormat(strings.TrimSpace(raw))
	if format == "" {
		return ReplyTraceFormatNoKV, nil
	}
	switch format {
	case ReplyTraceFormatNoKV, ReplyTraceFormatEtcdReadIndex, ReplyTraceFormatEtcdLeaseRenew, ReplyTraceFormatCRDBLeaseStart:
		return format, nil
	default:
		return "", fmt.Errorf("unknown reply trace format %q", raw)
	}
}

func DecodeReplyTrace(data []byte, format ReplyTraceFormat) ([]ReplyTraceRecord, error) {
	switch format {
	case ReplyTraceFormatNoKV:
		return decodeNoKVReplyTrace(data)
	case ReplyTraceFormatEtcdReadIndex:
		return decodeEtcdReadIndexTrace(data)
	case ReplyTraceFormatEtcdLeaseRenew:
		return decodeEtcdLeaseRenewTrace(data)
	case ReplyTraceFormatCRDBLeaseStart:
		return decodeCRDBLeaseStartTrace(data)
	default:
		return nil, fmt.Errorf("unsupported reply trace format %q", format)
	}
}

func decodeNoKVReplyTrace(data []byte) ([]ReplyTraceRecord, error) {
	var records []ReplyTraceRecord
	if err := json.Unmarshal(data, &records); err == nil {
		return records, nil
	}
	var envelope struct {
		Records []ReplyTraceRecord `json:"records"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	return envelope.Records, nil
}

type etcdReadIndexTraceRecord struct {
	MemberID            string `json:"member_id,omitempty"`
	Duty                string `json:"duty,omitempty"`
	ReadStateGeneration uint64 `json:"read_state_generation"`
	SuccessorGeneration uint64 `json:"successor_generation"`
	Accepted            bool   `json:"accepted"`
}

func decodeEtcdReadIndexTrace(data []byte) ([]ReplyTraceRecord, error) {
	var records []etcdReadIndexTraceRecord
	if err := json.Unmarshal(data, &records); err == nil {
		return projectEtcdReadIndexTrace(records), nil
	}
	var envelope struct {
		Records []etcdReadIndexTraceRecord `json:"records"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	return projectEtcdReadIndexTrace(envelope.Records), nil
}

func projectEtcdReadIndexTrace(records []etcdReadIndexTraceRecord) []ReplyTraceRecord {
	out := make([]ReplyTraceRecord, 0, len(records))
	for _, record := range records {
		duty := strings.TrimSpace(record.Duty)
		if duty == "" {
			duty = "read_index"
		}
		out = append(out, ReplyTraceRecord{
			Source:                      "etcd-read-index",
			Duty:                        duty,
			CertGeneration:              record.ReadStateGeneration,
			ObservedSuccessorGeneration: record.SuccessorGeneration,
			Accepted:                    record.Accepted,
		})
	}
	return out
}

type etcdLeaseRenewTraceRecord struct {
	MemberID         string `json:"member_id,omitempty"`
	Duty             string `json:"duty,omitempty"`
	ResponseRevision uint64 `json:"response_revision"`
	RevokeRevision   uint64 `json:"revoke_revision"`
	Accepted         bool   `json:"accepted"`
}

func decodeEtcdLeaseRenewTrace(data []byte) ([]ReplyTraceRecord, error) {
	var records []etcdLeaseRenewTraceRecord
	if err := json.Unmarshal(data, &records); err == nil {
		return projectEtcdLeaseRenewTrace(records), nil
	}
	var envelope struct {
		Records []etcdLeaseRenewTraceRecord `json:"records"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	return projectEtcdLeaseRenewTrace(envelope.Records), nil
}

func projectEtcdLeaseRenewTrace(records []etcdLeaseRenewTraceRecord) []ReplyTraceRecord {
	out := make([]ReplyTraceRecord, 0, len(records))
	for _, record := range records {
		duty := strings.TrimSpace(record.Duty)
		if duty == "" {
			duty = "lease_renew"
		}
		out = append(out, ReplyTraceRecord{
			Source:                      "etcd-lease-renew",
			Duty:                        duty,
			CertGeneration:              record.ResponseRevision,
			ObservedSuccessorGeneration: record.RevokeRevision,
			Accepted:                    record.Accepted,
		})
	}
	return out
}

type crdbLeaseStartTraceRecord struct {
	Key                 string `json:"key,omitempty"`
	Duty                string `json:"duty,omitempty"`
	SuccessorLeaseStart uint64 `json:"successor_lease_start"`
	ServedTimestamp     uint64 `json:"served_timestamp"`
	Accepted            bool   `json:"accepted"`
}

func decodeCRDBLeaseStartTrace(data []byte) ([]ReplyTraceRecord, error) {
	var records []crdbLeaseStartTraceRecord
	if err := json.Unmarshal(data, &records); err == nil {
		return projectCRDBLeaseStartTrace(records), nil
	}
	var envelope struct {
		Records []crdbLeaseStartTraceRecord `json:"records"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	return projectCRDBLeaseStartTrace(envelope.Records), nil
}

func projectCRDBLeaseStartTrace(records []crdbLeaseStartTraceRecord) []ReplyTraceRecord {
	out := make([]ReplyTraceRecord, 0, len(records))
	for _, record := range records {
		duty := strings.TrimSpace(record.Duty)
		if duty == "" {
			duty = "lease_start_coverage"
		}
		out = append(out, ReplyTraceRecord{
			Source:                      "crdb-lease-start",
			Duty:                        duty,
			CertGeneration:              record.SuccessorLeaseStart,
			ObservedSuccessorGeneration: record.ServedTimestamp,
			Accepted:                    record.Accepted,
		})
	}
	return out
}
