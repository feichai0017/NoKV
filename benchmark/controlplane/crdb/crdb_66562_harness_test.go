package crdb

import (
	"encoding/json"
	"fmt"
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	protocol "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	"github.com/stretchr/testify/require"
)

type crdb66562Config struct {
	DisableCoverage bool
}

type crdb66562Write struct {
	HolderID     string
	Key          string
	Timestamp    uint64
	Value        string
	Accepted     bool
	RejectReason string
}

type crdb66562TraceRecord struct {
	Key                 string `json:"key,omitempty"`
	Duty                string `json:"duty,omitempty"`
	SuccessorLeaseStart uint64 `json:"successor_lease_start"`
	ServedTimestamp     uint64 `json:"served_timestamp"`
	Accepted            bool   `json:"accepted"`
}

type crdb66562LeaseState struct {
	Lease      protocol.LeaseView
	Summary    protocol.ReadSummary
	CacheFloor uint64
}

type crdb66562Harness struct {
	config crdb66562Config
	active crdb66562LeaseState
	sealed crdb66562LeaseState
	writes []crdb66562Write
}

func newCRDB66562Harness(config crdb66562Config) *crdb66562Harness {
	return &crdb66562Harness{config: config}
}

func (h *crdb66562Harness) AcquireFreshLease(holderID string, leaseStart, leaseExpiration uint64) error {
	if err := validateLeaseWindow(leaseStart, leaseExpiration); err != nil {
		return err
	}
	if h.active.Lease.HolderID != "" {
		return fmt.Errorf("active lease still held by %s", h.active.Lease.HolderID)
	}
	candidate := protocol.LeaseView{
		HolderID:        holderID,
		LeaseStart:      leaseStart,
		LeaseExpiration: leaseExpiration,
		Acquisition:     protocol.LeaseAcquisitionFresh,
	}
	if h.sealed.Lease.HolderID != "" && !h.config.DisableCoverage {
		if err := protocol.ValidateLeaseStartCoverage(candidate, h.sealed.Summary); err != nil {
			return err
		}
	}
	h.active = crdb66562LeaseState{
		Lease:      candidate,
		CacheFloor: leaseStart,
	}
	return nil
}

func (h *crdb66562Harness) TransferLease(fromHolderID, toHolderID string, leaseStart, leaseExpiration uint64) error {
	if err := validateLeaseWindow(leaseStart, leaseExpiration); err != nil {
		return err
	}
	if h.active.Lease.HolderID != fromHolderID {
		return fmt.Errorf("lease holder mismatch: active=%s requested=%s", h.active.Lease.HolderID, fromHolderID)
	}
	if leaseStart > h.active.Lease.LeaseExpiration {
		return fmt.Errorf(
			"transfer lease start=%d exceeds predecessor expiration=%d",
			leaseStart,
			h.active.Lease.LeaseExpiration,
		)
	}
	h.active = crdb66562LeaseState{
		Lease: protocol.LeaseView{
			HolderID:        toHolderID,
			LeaseStart:      leaseStart,
			LeaseExpiration: leaseExpiration,
			Acquisition:     protocol.LeaseAcquisitionTransfer,
		},
		Summary:    h.active.Summary,
		CacheFloor: leaseStart,
	}
	return nil
}

func (h *crdb66562Harness) ServeFutureRead(holderID, key string, timestamp uint64) error {
	if h.active.Lease.HolderID != holderID {
		return fmt.Errorf("lease holder mismatch: active=%s requested=%s", h.active.Lease.HolderID, holderID)
	}
	if timestamp > h.active.Lease.LeaseExpiration {
		return fmt.Errorf(
			"future read ts=%d exceeds lease expiration=%d",
			timestamp,
			h.active.Lease.LeaseExpiration,
		)
	}
	h.active.Summary = h.active.Summary.WithRead(key, timestamp)
	return nil
}

func (h *crdb66562Harness) ExpireLease(holderID string, now uint64) error {
	if h.active.Lease.HolderID != holderID {
		return fmt.Errorf("lease holder mismatch: active=%s requested=%s", h.active.Lease.HolderID, holderID)
	}
	if now != h.active.Lease.LeaseExpiration {
		return fmt.Errorf("lease %s expires at %d, got %d", holderID, h.active.Lease.LeaseExpiration, now)
	}
	h.sealed = h.active
	h.active = crdb66562LeaseState{}
	return nil
}

func (h *crdb66562Harness) ServeWrite(holderID, key string, timestamp uint64, value string) error {
	if h.active.Lease.HolderID != holderID {
		return fmt.Errorf("lease holder mismatch: active=%s requested=%s", h.active.Lease.HolderID, holderID)
	}
	write := crdb66562Write{
		HolderID:  holderID,
		Key:       key,
		Timestamp: timestamp,
		Value:     value,
		Accepted:  true,
	}
	if timestamp < h.active.CacheFloor {
		write.Accepted = false
		write.RejectReason = fmt.Sprintf("write ts=%d is below cache floor=%d", timestamp, h.active.CacheFloor)
	}
	if servedTimestamp, ok := h.active.Summary.MaxTimestampForKey(key); ok && timestamp <= servedTimestamp {
		write.Accepted = false
		write.RejectReason = fmt.Sprintf("write ts=%d is behind served read ts=%d", timestamp, servedTimestamp)
	}
	h.writes = append(h.writes, write)
	if !write.Accepted {
		return fmt.Errorf("%s", write.RejectReason)
	}
	return nil
}

func (h *crdb66562Harness) CoverageReport(successor protocol.LeaseView) coordaudit.LeaseStartCoverageReport {
	return coordaudit.BuildLeaseStartCoverageReport(h.sealed.Lease, successor, h.sealed.Summary)
}

func (h *crdb66562Harness) TraceRecord(successor protocol.LeaseView, accepted bool) crdb66562TraceRecord {
	servedTimestamp := h.sealed.Summary.MaxTimestamp()
	key := ""
	if len(h.sealed.Summary.Reads) != 0 {
		key = h.sealed.Summary.Reads[0].Key
	}
	return crdb66562TraceRecord{
		Key:                 key,
		Duty:                "lease_start_coverage",
		SuccessorLeaseStart: successor.LeaseStart,
		ServedTimestamp:     servedTimestamp,
		Accepted:            accepted,
	}
}

func mustDecodeCRDBLeaseStartTrace(t *testing.T, raw []crdb66562TraceRecord) []coordaudit.ReplyTraceRecord {
	t.Helper()
	data, err := json.Marshal(raw)
	require.NoError(t, err)
	records, err := coordaudit.DecodeReplyTrace(data, coordaudit.ReplyTraceFormatCRDBLeaseStart)
	require.NoError(t, err)
	return records
}

func validateLeaseWindow(leaseStart, leaseExpiration uint64) error {
	if leaseExpiration < leaseStart {
		return fmt.Errorf("invalid lease window start=%d expiration=%d", leaseStart, leaseExpiration)
	}
	return nil
}
