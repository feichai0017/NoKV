// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"sync"
	"sync/atomic"
	"time"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

type runtimeMetrics struct {
	commitTotal          atomic.Uint64
	flushTotal           atomic.Uint64
	segmentTotal         atomic.Uint64
	segmentOpsTotal      atomic.Uint64
	segmentEntryTotal    atomic.Uint64
	sealTotal            atomic.Uint64
	flushLatencyTotal    atomic.Uint64
	flushLatencyLast     atomic.Uint64
	flushLatencyMax      atomic.Uint64
	witnessLatencyTotal  atomic.Uint64
	witnessLatencyLast   atomic.Uint64
	witnessLatencyMax    atomic.Uint64
	installLatencyTotal  atomic.Uint64
	installLatencyLast   atomic.Uint64
	installLatencyMax    atomic.Uint64
	installPayloadTotal  atomic.Uint64
	installPayloadLast   atomic.Uint64
	installPayloadMax    atomic.Uint64
	installRoutesTotal   atomic.Uint64
	installRoutesLast    atomic.Uint64
	installRoutesMax     atomic.Uint64
	sealLatencyTotal     atomic.Uint64
	sealLatencyLast      atomic.Uint64
	sealLatencyMax       atomic.Uint64
	flushBatchTotal      atomic.Uint64
	flushJobTotal        atomic.Uint64
	flushJobLast         atomic.Uint64
	flushJobMax          atomic.Uint64
	errorTotal           atomic.Uint64
	retryTotal           atomic.Uint64
	retryUnavailable     atomic.Uint64
	retryRouting         atomic.Uint64
	retryStaleEpoch      atomic.Uint64
	retryOther           atomic.Uint64
	bgSkipTotal          atomic.Uint64
	bgErrorTotal         atomic.Uint64
	catalogLoadTotal     atomic.Uint64
	rootSealTotal        atomic.Uint64
	rootSealMissingTotal atomic.Uint64
	recoveryInstallTotal atomic.Uint64
	recoverySkipTotal    atomic.Uint64

	statsMu          sync.RWMutex
	lastSegmentStats fsperas.SegmentStats
	lastSegmentRoot  [32]byte
	lastError        string
}

func (c *Runtime) recordFlushLatency(d time.Duration) {
	recordPerasDuration(&c.metrics.flushLatencyTotal, &c.metrics.flushLatencyLast, &c.metrics.flushLatencyMax, d)
}

func (c *Runtime) recordWitnessLatency(d time.Duration) {
	recordPerasDuration(&c.metrics.witnessLatencyTotal, &c.metrics.witnessLatencyLast, &c.metrics.witnessLatencyMax, d)
}

func (c *Runtime) recordInstallLatency(d time.Duration) {
	recordPerasDuration(&c.metrics.installLatencyTotal, &c.metrics.installLatencyLast, &c.metrics.installLatencyMax, d)
}

func (c *Runtime) recordInstallShape(payloadBytes, routeKeys int) {
	if c == nil {
		return
	}
	if payloadBytes < 0 {
		payloadBytes = 0
	}
	if routeKeys < 0 {
		routeKeys = 0
	}
	payload := uint64(payloadBytes)
	routes := uint64(routeKeys)
	c.metrics.installPayloadTotal.Add(payload)
	c.metrics.installPayloadLast.Store(payload)
	recordPerasMax(&c.metrics.installPayloadMax, payload)
	c.metrics.installRoutesTotal.Add(routes)
	c.metrics.installRoutesLast.Store(routes)
	recordPerasMax(&c.metrics.installRoutesMax, routes)
}

func (c *Runtime) recordSealLatency(d time.Duration) {
	recordPerasDuration(&c.metrics.sealLatencyTotal, &c.metrics.sealLatencyLast, &c.metrics.sealLatencyMax, d)
}

func (c *Runtime) recordFlushBatch(jobs int) {
	if jobs <= 0 {
		return
	}
	n := uint64(jobs)
	c.metrics.flushBatchTotal.Add(1)
	c.metrics.flushJobTotal.Add(n)
	c.metrics.flushJobLast.Store(n)
	recordPerasMax(&c.metrics.flushJobMax, n)
}

func recordPerasDuration(total, last, max *atomic.Uint64, d time.Duration) {
	if d < 0 {
		d = 0
	}
	ns := uint64(d.Nanoseconds())
	total.Add(ns)
	last.Store(ns)
	recordPerasMax(max, ns)
}

func recordPerasMax(max *atomic.Uint64, value uint64) {
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

func averagePerasDuration(total, count uint64) uint64 {
	if count == 0 {
		return 0
	}
	return total / count
}

func (c *Runtime) Stats() map[string]any {
	if c == nil {
		return map[string]any{
			"commit_total":                       uint64(0),
			"flush_total":                        uint64(0),
			"segment_total":                      uint64(0),
			"seal_total":                         uint64(0),
			"segment_operations_total":           uint64(0),
			"segment_entries_total":              uint64(0),
			"flush_latency_total_nanosecond":     uint64(0),
			"flush_latency_last_nanosecond":      uint64(0),
			"flush_latency_max_nanosecond":       uint64(0),
			"flush_latency_average_nanosecond":   uint64(0),
			"witness_latency_total_nanosecond":   uint64(0),
			"witness_latency_last_nanosecond":    uint64(0),
			"witness_latency_max_nanosecond":     uint64(0),
			"witness_latency_average_nanosecond": uint64(0),
			"install_latency_total_nanosecond":   uint64(0),
			"install_latency_last_nanosecond":    uint64(0),
			"install_latency_max_nanosecond":     uint64(0),
			"install_latency_average_nanosecond": uint64(0),
			"install_payload_bytes_total":        uint64(0),
			"install_payload_bytes_last":         uint64(0),
			"install_payload_bytes_max":          uint64(0),
			"install_route_keys_total":           uint64(0),
			"install_route_keys_last":            uint64(0),
			"install_route_keys_max":             uint64(0),
			"seal_latency_total_nanosecond":      uint64(0),
			"seal_latency_last_nanosecond":       uint64(0),
			"seal_latency_max_nanosecond":        uint64(0),
			"seal_latency_average_nanosecond":    uint64(0),
			"flush_batch_total":                  uint64(0),
			"flush_jobs_total":                   uint64(0),
			"flush_jobs_last":                    uint64(0),
			"flush_jobs_max":                     uint64(0),
			"last_segment_operations":            uint64(0),
			"last_segment_input_mutations":       uint64(0),
			"last_segment_entries":               uint64(0),
			"last_segment_coalesced":             uint64(0),
			"last_segment_compression_x100":      uint64(0),
			"last_segment_root":                  [32]byte{},
			"last_error":                         "",
			"error_total":                        uint64(0),
			"retry_total":                        uint64(0),
			"retry_unavailable_total":            uint64(0),
			"retry_routing_total":                uint64(0),
			"retry_stale_epoch_total":            uint64(0),
			"retry_other_total":                  uint64(0),
			"background_skip_total":              uint64(0),
			"background_error_total":             uint64(0),
			"segment_catalog_load_total":         uint64(0),
			"root_sealed_segment_total":          uint64(0),
			"root_sealed_segment_missing_total":  uint64(0),
			"segment_recovery_install_total":     uint64(0),
			"segment_recovery_skip_total":        uint64(0),
			"overlay_keys":                       0,
			"segment_keys":                       0,
			"predicate_known_keys":               0,
			"predicate_empty_dirs":               0,
			"predicate_empty_sessions":           0,
			"holders":                            0,
			"pending":                            0,
			"segment_install_parallelism":        0,
			"segment_install_queue_depth":        0,
			"segment_install_queue_capacity":     0,
			"segment_seal_queue_depth":           0,
			"segment_seal_queue_capacity":        0,
			"witness_count":                      0,
			"quorum":                             0,
		}
	}
	var overlayKeys, knownKeys, emptyDirs, emptySessions int
	var segmentKeys int
	if c.read != nil {
		overlayKeys, knownKeys, emptyDirs, emptySessions = c.read.overlay.Stats()
		segmentKeys, _, _, _ = c.read.sealed.Stats()
	}
	holders, pending := 0, 0
	if c.epochs != nil {
		holders, pending = c.epochs.stats()
	}
	c.metrics.statsMu.RLock()
	lastSegmentStats := c.metrics.lastSegmentStats
	lastSegmentRoot := c.metrics.lastSegmentRoot
	lastError := c.metrics.lastError
	c.metrics.statsMu.RUnlock()
	installQueueDepth := 0
	installQueueCapacity := 0
	if c.installQ != nil {
		installQueueDepth = c.installQ.depth()
		installQueueCapacity = c.installQ.capacity()
	}
	sealQueueDepth := 0
	sealQueueCapacity := 0
	if c.sealQ != nil {
		sealQueueDepth = c.sealQ.depth()
		sealQueueCapacity = c.sealQ.capacity()
	}
	flushTotal := c.metrics.flushTotal.Load()
	sealTotal := c.metrics.sealTotal.Load()
	flushLatencyTotal := c.metrics.flushLatencyTotal.Load()
	witnessLatencyTotal := c.metrics.witnessLatencyTotal.Load()
	installLatencyTotal := c.metrics.installLatencyTotal.Load()
	sealLatencyTotal := c.metrics.sealLatencyTotal.Load()
	return map[string]any{
		"commit_total":                       c.metrics.commitTotal.Load(),
		"flush_total":                        flushTotal,
		"segment_total":                      c.metrics.segmentTotal.Load(),
		"seal_total":                         sealTotal,
		"segment_operations_total":           c.metrics.segmentOpsTotal.Load(),
		"segment_entries_total":              c.metrics.segmentEntryTotal.Load(),
		"flush_latency_total_nanosecond":     flushLatencyTotal,
		"flush_latency_last_nanosecond":      c.metrics.flushLatencyLast.Load(),
		"flush_latency_max_nanosecond":       c.metrics.flushLatencyMax.Load(),
		"flush_latency_average_nanosecond":   averagePerasDuration(flushLatencyTotal, flushTotal),
		"witness_latency_total_nanosecond":   witnessLatencyTotal,
		"witness_latency_last_nanosecond":    c.metrics.witnessLatencyLast.Load(),
		"witness_latency_max_nanosecond":     c.metrics.witnessLatencyMax.Load(),
		"witness_latency_average_nanosecond": averagePerasDuration(witnessLatencyTotal, flushTotal),
		"install_latency_total_nanosecond":   installLatencyTotal,
		"install_latency_last_nanosecond":    c.metrics.installLatencyLast.Load(),
		"install_latency_max_nanosecond":     c.metrics.installLatencyMax.Load(),
		"install_latency_average_nanosecond": averagePerasDuration(installLatencyTotal, flushTotal),
		"install_payload_bytes_total":        c.metrics.installPayloadTotal.Load(),
		"install_payload_bytes_last":         c.metrics.installPayloadLast.Load(),
		"install_payload_bytes_max":          c.metrics.installPayloadMax.Load(),
		"install_route_keys_total":           c.metrics.installRoutesTotal.Load(),
		"install_route_keys_last":            c.metrics.installRoutesLast.Load(),
		"install_route_keys_max":             c.metrics.installRoutesMax.Load(),
		"seal_latency_total_nanosecond":      sealLatencyTotal,
		"seal_latency_last_nanosecond":       c.metrics.sealLatencyLast.Load(),
		"seal_latency_max_nanosecond":        c.metrics.sealLatencyMax.Load(),
		"seal_latency_average_nanosecond":    averagePerasDuration(sealLatencyTotal, sealTotal),
		"flush_batch_total":                  c.metrics.flushBatchTotal.Load(),
		"flush_jobs_total":                   c.metrics.flushJobTotal.Load(),
		"flush_jobs_last":                    c.metrics.flushJobLast.Load(),
		"flush_jobs_max":                     c.metrics.flushJobMax.Load(),
		"last_segment_operations":            lastSegmentStats.OperationCount,
		"last_segment_input_mutations":       lastSegmentStats.InputMutationCount,
		"last_segment_entries":               lastSegmentStats.EntryCount,
		"last_segment_coalesced":             lastSegmentStats.CoalescedMutations,
		"last_segment_compression_x100":      uint64(lastSegmentStats.CompressionRatio * 100),
		"last_segment_root":                  lastSegmentRoot,
		"last_error":                         lastError,
		"error_total":                        c.metrics.errorTotal.Load(),
		"retry_total":                        c.metrics.retryTotal.Load(),
		"retry_unavailable_total":            c.metrics.retryUnavailable.Load(),
		"retry_routing_total":                c.metrics.retryRouting.Load(),
		"retry_stale_epoch_total":            c.metrics.retryStaleEpoch.Load(),
		"retry_other_total":                  c.metrics.retryOther.Load(),
		"background_skip_total":              c.metrics.bgSkipTotal.Load(),
		"background_error_total":             c.metrics.bgErrorTotal.Load(),
		"segment_catalog_load_total":         c.metrics.catalogLoadTotal.Load(),
		"root_sealed_segment_total":          c.metrics.rootSealTotal.Load(),
		"root_sealed_segment_missing_total":  c.metrics.rootSealMissingTotal.Load(),
		"segment_recovery_install_total":     c.metrics.recoveryInstallTotal.Load(),
		"segment_recovery_skip_total":        c.metrics.recoverySkipTotal.Load(),
		"overlay_keys":                       overlayKeys,
		"segment_keys":                       segmentKeys,
		"predicate_known_keys":               knownKeys,
		"predicate_empty_dirs":               emptyDirs,
		"predicate_empty_sessions":           emptySessions,
		"holders":                            holders,
		"pending":                            pending,
		"segment_install_parallelism":        c.installN,
		"segment_install_queue_depth":        installQueueDepth,
		"segment_install_queue_capacity":     installQueueCapacity,
		"segment_seal_queue_depth":           sealQueueDepth,
		"segment_seal_queue_capacity":        sealQueueCapacity,
		"witness_count":                      len(c.witnesses),
		"quorum":                             c.quorum,
	}
}
