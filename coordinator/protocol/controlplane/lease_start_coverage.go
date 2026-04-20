package controlplane

import (
	"errors"
	"fmt"
	"sort"
)

var ErrLeaseStartCoverage = errors.New("coordinator/protocol/controlplane: successor lease_start does not cover predecessor served summary")

type LeaseAcquisitionKind uint8

const (
	LeaseAcquisitionUnknown LeaseAcquisitionKind = iota
	LeaseAcquisitionFresh
	LeaseAcquisitionTransfer
)

func (k LeaseAcquisitionKind) String() string {
	switch k {
	case LeaseAcquisitionFresh:
		return "fresh"
	case LeaseAcquisitionTransfer:
		return "transfer"
	default:
		return "unknown"
	}
}

// ServedRead records one predecessor read that exposed a future timestamp for a
// specific key under the old lease holder.
type ServedRead struct {
	Key       string
	Timestamp uint64
}

// ReadSummary is the minimal served-read summary that one authority instance
// leaves behind for its successor.
type ReadSummary struct {
	Reads []ServedRead
}

// LeaseView is the minimal authority view needed by the lease-start coverage
// check. It intentionally stays protocol-side and detached from rooted schema.
type LeaseView struct {
	HolderID        string
	LeaseStart      uint64
	LeaseExpiration uint64
	Acquisition     LeaseAcquisitionKind
}

// LeaseStartCoverageCheck captures one served-read element and whether the
// successor lease start strictly covers it.
type LeaseStartCoverageCheck struct {
	Key                 string
	ServedTimestamp     uint64
	SuccessorLeaseStart uint64
	Covered             bool
}

// LeaseStartCoverageStatus is the structured coverage projection for a
// successor campaign against one predecessor read summary.
type LeaseStartCoverageStatus struct {
	Checks []LeaseStartCoverageCheck
}

// NewReadSummary normalizes the served-read set into a stable key/timestamp
// order so reports and tests remain deterministic.
func NewReadSummary(reads ...ServedRead) ReadSummary {
	if len(reads) == 0 {
		return ReadSummary{}
	}
	out := make([]ServedRead, 0, len(reads))
	for _, read := range reads {
		if read.Key == "" {
			continue
		}
		out = append(out, read)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key == out[j].Key {
			return out[i].Timestamp < out[j].Timestamp
		}
		return out[i].Key < out[j].Key
	})
	return ReadSummary{Reads: out}
}

func (s ReadSummary) WithRead(key string, timestamp uint64) ReadSummary {
	reads := make([]ServedRead, 0, len(s.Reads)+1)
	reads = append(reads, s.Reads...)
	reads = append(reads, ServedRead{Key: key, Timestamp: timestamp})
	return NewReadSummary(reads...)
}

func (s ReadSummary) MaxTimestamp() uint64 {
	var maxTimestamp uint64
	for _, read := range s.Reads {
		if read.Timestamp > maxTimestamp {
			maxTimestamp = read.Timestamp
		}
	}
	return maxTimestamp
}

func (s ReadSummary) MaxTimestampForKey(key string) (uint64, bool) {
	var (
		maxTimestamp uint64
		found        bool
	)
	for _, read := range s.Reads {
		if read.Key != key {
			continue
		}
		if !found || read.Timestamp > maxTimestamp {
			maxTimestamp = read.Timestamp
			found = true
		}
	}
	return maxTimestamp, found
}

func EvaluateLeaseStartCoverage(successor LeaseView, summary ReadSummary) LeaseStartCoverageStatus {
	if len(summary.Reads) == 0 {
		return LeaseStartCoverageStatus{}
	}
	checks := make([]LeaseStartCoverageCheck, 0, len(summary.Reads))
	for _, read := range summary.Reads {
		checks = append(checks, LeaseStartCoverageCheck{
			Key:                 read.Key,
			ServedTimestamp:     read.Timestamp,
			SuccessorLeaseStart: successor.LeaseStart,
			Covered:             successor.LeaseStart > read.Timestamp,
		})
	}
	return LeaseStartCoverageStatus{Checks: checks}
}

func (s LeaseStartCoverageStatus) Covered() bool {
	for _, check := range s.Checks {
		if !check.Covered {
			return false
		}
	}
	return true
}

func (s LeaseStartCoverageStatus) Violations() []LeaseStartCoverageCheck {
	if len(s.Checks) == 0 {
		return nil
	}
	out := make([]LeaseStartCoverageCheck, 0, len(s.Checks))
	for _, check := range s.Checks {
		if !check.Covered {
			out = append(out, check)
		}
	}
	return out
}

func ValidateLeaseStartCoverage(successor LeaseView, summary ReadSummary) error {
	status := EvaluateLeaseStartCoverage(successor, summary)
	for _, check := range status.Checks {
		if check.Covered {
			continue
		}
		return fmt.Errorf(
			"%w: successor lease_start=%d does not cover served read key=%q ts=%d",
			ErrLeaseStartCoverage,
			check.SuccessorLeaseStart,
			check.Key,
			check.ServedTimestamp,
		)
	}
	return nil
}
