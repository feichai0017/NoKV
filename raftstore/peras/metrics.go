// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"sync/atomic"
	"time"
)

type witnessNodeMetrics struct {
	appendTotal                atomic.Uint64
	appendErrorTotal           atomic.Uint64
	appendRecordsTotal         atomic.Uint64
	appendRecordsLast          atomic.Uint64
	appendRecordsMax           atomic.Uint64
	appendLatencyTotal         atomic.Uint64
	appendLatencyLast          atomic.Uint64
	appendLatencyMax           atomic.Uint64
	authorityCheckBatchTotal   atomic.Uint64
	authorityCheckRecordTotal  atomic.Uint64
	authorityCheckLatencyTotal atomic.Uint64
	authorityCheckLatencyLast  atomic.Uint64
	authorityCheckLatencyMax   atomic.Uint64
	dedupeSkipTotal            atomic.Uint64
	inflightWaitTotal          atomic.Uint64
	pendingAppendRecordsTotal  atomic.Uint64
	pendingAppendRecordsLast   atomic.Uint64
	pendingAppendRecordsMax    atomic.Uint64
}

func (m *witnessNodeMetrics) recordAppend(records int, d time.Duration, err error) {
	if m == nil {
		return
	}
	m.appendTotal.Add(1)
	if err != nil {
		m.appendErrorTotal.Add(1)
	}
	n := uint64(nonNegative(records))
	m.appendRecordsTotal.Add(n)
	m.appendRecordsLast.Store(n)
	recordWitnessMax(&m.appendRecordsMax, n)
	recordWitnessDuration(&m.appendLatencyTotal, &m.appendLatencyLast, &m.appendLatencyMax, d)
}

func (m *witnessNodeMetrics) recordAuthorityCheck(records int, d time.Duration) {
	if m == nil {
		return
	}
	m.authorityCheckBatchTotal.Add(1)
	m.authorityCheckRecordTotal.Add(uint64(nonNegative(records)))
	recordWitnessDuration(&m.authorityCheckLatencyTotal, &m.authorityCheckLatencyLast, &m.authorityCheckLatencyMax, d)
}

func (m *witnessNodeMetrics) recordDedupe(skipped, waiting, pending int) {
	if m == nil {
		return
	}
	m.dedupeSkipTotal.Add(uint64(nonNegative(skipped)))
	m.inflightWaitTotal.Add(uint64(nonNegative(waiting)))
	n := uint64(nonNegative(pending))
	m.pendingAppendRecordsTotal.Add(n)
	m.pendingAppendRecordsLast.Store(n)
	recordWitnessMax(&m.pendingAppendRecordsMax, n)
}

func (m *witnessNodeMetrics) Stats() map[string]any {
	if m == nil {
		return emptyWitnessNodeStats()
	}
	appendTotal := m.appendTotal.Load()
	appendLatencyTotal := m.appendLatencyTotal.Load()
	authorityCheckBatchTotal := m.authorityCheckBatchTotal.Load()
	authorityCheckLatencyTotal := m.authorityCheckLatencyTotal.Load()
	return map[string]any{
		"append_total":                               appendTotal,
		"append_error_total":                         m.appendErrorTotal.Load(),
		"append_records_total":                       m.appendRecordsTotal.Load(),
		"append_records_last":                        m.appendRecordsLast.Load(),
		"append_records_max":                         m.appendRecordsMax.Load(),
		"append_latency_total_nanosecond":            appendLatencyTotal,
		"append_latency_last_nanosecond":             m.appendLatencyLast.Load(),
		"append_latency_max_nanosecond":              m.appendLatencyMax.Load(),
		"append_latency_average_nanosecond":          averageWitnessDuration(appendLatencyTotal, appendTotal),
		"authority_check_batches_total":              authorityCheckBatchTotal,
		"authority_check_records_total":              m.authorityCheckRecordTotal.Load(),
		"authority_check_latency_total_nanosecond":   authorityCheckLatencyTotal,
		"authority_check_latency_last_nanosecond":    m.authorityCheckLatencyLast.Load(),
		"authority_check_latency_max_nanosecond":     m.authorityCheckLatencyMax.Load(),
		"authority_check_latency_average_nanosecond": averageWitnessDuration(authorityCheckLatencyTotal, authorityCheckBatchTotal),
		"dedupe_skip_total":                          m.dedupeSkipTotal.Load(),
		"inflight_wait_total":                        m.inflightWaitTotal.Load(),
		"pending_append_records_total":               m.pendingAppendRecordsTotal.Load(),
		"pending_append_records_last":                m.pendingAppendRecordsLast.Load(),
		"pending_append_records_max":                 m.pendingAppendRecordsMax.Load(),
	}
}

func emptyWitnessNodeStats() map[string]any {
	return map[string]any{
		"append_total":                               uint64(0),
		"append_error_total":                         uint64(0),
		"append_records_total":                       uint64(0),
		"append_records_last":                        uint64(0),
		"append_records_max":                         uint64(0),
		"append_latency_total_nanosecond":            uint64(0),
		"append_latency_last_nanosecond":             uint64(0),
		"append_latency_max_nanosecond":              uint64(0),
		"append_latency_average_nanosecond":          uint64(0),
		"authority_check_batches_total":              uint64(0),
		"authority_check_records_total":              uint64(0),
		"authority_check_latency_total_nanosecond":   uint64(0),
		"authority_check_latency_last_nanosecond":    uint64(0),
		"authority_check_latency_max_nanosecond":     uint64(0),
		"authority_check_latency_average_nanosecond": uint64(0),
		"dedupe_skip_total":                          uint64(0),
		"inflight_wait_total":                        uint64(0),
		"pending_append_records_total":               uint64(0),
		"pending_append_records_last":                uint64(0),
		"pending_append_records_max":                 uint64(0),
	}
}

type witnessLogMetrics struct {
	appendTotal           atomic.Uint64
	appendErrorTotal      atomic.Uint64
	appendRecordsTotal    atomic.Uint64
	appendRecordsLast     atomic.Uint64
	appendRecordsMax      atomic.Uint64
	appendBytesTotal      atomic.Uint64
	appendBytesLast       atomic.Uint64
	appendBytesMax        atomic.Uint64
	encodeLatencyTotal    atomic.Uint64
	encodeLatencyLast     atomic.Uint64
	encodeLatencyMax      atomic.Uint64
	walAppendLatencyTotal atomic.Uint64
	walAppendLatencyLast  atomic.Uint64
	walAppendLatencyMax   atomic.Uint64
}

func (m *witnessLogMetrics) recordAppend(records, bytes int, err error) {
	if m == nil {
		return
	}
	m.appendTotal.Add(1)
	if err != nil {
		m.appendErrorTotal.Add(1)
	}
	recordCount := uint64(nonNegative(records))
	byteCount := uint64(nonNegative(bytes))
	m.appendRecordsTotal.Add(recordCount)
	m.appendRecordsLast.Store(recordCount)
	recordWitnessMax(&m.appendRecordsMax, recordCount)
	m.appendBytesTotal.Add(byteCount)
	m.appendBytesLast.Store(byteCount)
	recordWitnessMax(&m.appendBytesMax, byteCount)
}

func (m *witnessLogMetrics) recordEncode(d time.Duration) {
	if m == nil {
		return
	}
	recordWitnessDuration(&m.encodeLatencyTotal, &m.encodeLatencyLast, &m.encodeLatencyMax, d)
}

func (m *witnessLogMetrics) recordWALAppend(d time.Duration) {
	if m == nil {
		return
	}
	recordWitnessDuration(&m.walAppendLatencyTotal, &m.walAppendLatencyLast, &m.walAppendLatencyMax, d)
}

func (m *witnessLogMetrics) Stats() map[string]any {
	if m == nil {
		return emptyWitnessLogStats()
	}
	appendTotal := m.appendTotal.Load()
	encodeLatencyTotal := m.encodeLatencyTotal.Load()
	walAppendLatencyTotal := m.walAppendLatencyTotal.Load()
	return map[string]any{
		"wal_append_total":                              appendTotal,
		"wal_append_error_total":                        m.appendErrorTotal.Load(),
		"wal_append_records_total":                      m.appendRecordsTotal.Load(),
		"wal_append_records_last":                       m.appendRecordsLast.Load(),
		"wal_append_records_max":                        m.appendRecordsMax.Load(),
		"wal_append_bytes_total":                        m.appendBytesTotal.Load(),
		"wal_append_bytes_last":                         m.appendBytesLast.Load(),
		"wal_append_bytes_max":                          m.appendBytesMax.Load(),
		"wal_encode_latency_total_nanosecond":           encodeLatencyTotal,
		"wal_encode_latency_last_nanosecond":            m.encodeLatencyLast.Load(),
		"wal_encode_latency_max_nanosecond":             m.encodeLatencyMax.Load(),
		"wal_encode_latency_average_nanosecond":         averageWitnessDuration(encodeLatencyTotal, appendTotal),
		"wal_manager_append_latency_total_nanosecond":   walAppendLatencyTotal,
		"wal_manager_append_latency_last_nanosecond":    m.walAppendLatencyLast.Load(),
		"wal_manager_append_latency_max_nanosecond":     m.walAppendLatencyMax.Load(),
		"wal_manager_append_latency_average_nanosecond": averageWitnessDuration(walAppendLatencyTotal, appendTotal),
	}
}

func emptyWitnessLogStats() map[string]any {
	return map[string]any{
		"wal_append_total":                              uint64(0),
		"wal_append_error_total":                        uint64(0),
		"wal_append_records_total":                      uint64(0),
		"wal_append_records_last":                       uint64(0),
		"wal_append_records_max":                        uint64(0),
		"wal_append_bytes_total":                        uint64(0),
		"wal_append_bytes_last":                         uint64(0),
		"wal_append_bytes_max":                          uint64(0),
		"wal_encode_latency_total_nanosecond":           uint64(0),
		"wal_encode_latency_last_nanosecond":            uint64(0),
		"wal_encode_latency_max_nanosecond":             uint64(0),
		"wal_encode_latency_average_nanosecond":         uint64(0),
		"wal_manager_append_latency_total_nanosecond":   uint64(0),
		"wal_manager_append_latency_last_nanosecond":    uint64(0),
		"wal_manager_append_latency_max_nanosecond":     uint64(0),
		"wal_manager_append_latency_average_nanosecond": uint64(0),
	}
}

func recordWitnessDuration(total, last, max *atomic.Uint64, d time.Duration) {
	if d < 0 {
		d = 0
	}
	ns := uint64(d.Nanoseconds())
	total.Add(ns)
	last.Store(ns)
	recordWitnessMax(max, ns)
}

func recordWitnessMax(max *atomic.Uint64, value uint64) {
	if max == nil {
		return
	}
	for {
		old := max.Load()
		if value <= old {
			return
		}
		if max.CompareAndSwap(old, value) {
			return
		}
	}
}

func averageWitnessDuration(total, count uint64) uint64 {
	if count == 0 {
		return 0
	}
	return total / count
}

func nonNegative(value int) int {
	if value < 0 {
		return 0
	}
	return value
}
