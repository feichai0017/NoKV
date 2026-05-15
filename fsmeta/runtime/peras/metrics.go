// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

func (c *Runtime) recordErrorf(format string, args ...any) error {
	return c.recordError(fmt.Errorf(format, args...))
}

func (c *Runtime) recordError(err error) error {
	if c == nil || err == nil {
		return err
	}
	c.metrics.errorTotal.Add(1)
	c.metrics.statsMu.Lock()
	c.metrics.lastError = err.Error()
	c.metrics.statsMu.Unlock()
	return err
}

func (c *Runtime) recordInstallRetry(err error) {
	if c == nil {
		return
	}
	c.metrics.retryTotal.Add(1)
	switch nokverrors.KindOf(err) {
	case nokverrors.KindUnavailable, nokverrors.KindRouteUnavailable:
		c.metrics.retryUnavailable.Add(1)
	case nokverrors.KindRegionRouting, nokverrors.KindNotLeader:
		c.metrics.retryRouting.Add(1)
	case nokverrors.KindStaleEpoch:
		c.metrics.retryStaleEpoch.Add(1)
	default:
		c.metrics.retryOther.Add(1)
	}
}

type runtimeMetrics struct {
	commitTotal                    atomic.Uint64
	flushTotal                     atomic.Uint64
	segmentTotal                   atomic.Uint64
	segmentOpsTotal                atomic.Uint64
	segmentEntryTotal              atomic.Uint64
	sealTotal                      atomic.Uint64
	flushLatencyTotal              atomic.Uint64
	flushLatencyLast               atomic.Uint64
	flushLatencyMax                atomic.Uint64
	witnessLatencyTotal            atomic.Uint64
	witnessLatencyLast             atomic.Uint64
	witnessLatencyMax              atomic.Uint64
	witnessBatchTotal              atomic.Uint64
	witnessBatchRecordsTotal       atomic.Uint64
	witnessBatchRecordsLast        atomic.Uint64
	witnessBatchRecordsMax         atomic.Uint64
	witnessBatchBytesTotal         atomic.Uint64
	witnessBatchBytesLast          atomic.Uint64
	witnessBatchBytesMax           atomic.Uint64
	witnessReplicaAppendTotal      atomic.Uint64
	witnessReplicaAppendErrorTotal atomic.Uint64
	witnessReplicaLatencyTotal     atomic.Uint64
	witnessReplicaLatencyLast      atomic.Uint64
	witnessReplicaLatencyMax       atomic.Uint64
	witnessQuorumTotal             atomic.Uint64
	witnessQuorumLatencyTotal      atomic.Uint64
	witnessQuorumLatencyLast       atomic.Uint64
	witnessQuorumLatencyMax        atomic.Uint64
	witnessQuorumAcksLast          atomic.Uint64
	witnessQuorumAcksMax           atomic.Uint64
	installLatencyTotal            atomic.Uint64
	installLatencyLast             atomic.Uint64
	installLatencyMax              atomic.Uint64
	installPayloadTotal            atomic.Uint64
	installPayloadLast             atomic.Uint64
	installPayloadMax              atomic.Uint64
	installRoutesTotal             atomic.Uint64
	installRoutesLast              atomic.Uint64
	installRoutesMax               atomic.Uint64
	sealLatencyTotal               atomic.Uint64
	sealLatencyLast                atomic.Uint64
	sealLatencyMax                 atomic.Uint64
	flushBatchTotal                atomic.Uint64
	flushJobTotal                  atomic.Uint64
	flushJobLast                   atomic.Uint64
	flushJobMax                    atomic.Uint64
	errorTotal                     atomic.Uint64
	retryTotal                     atomic.Uint64
	retryUnavailable               atomic.Uint64
	retryRouting                   atomic.Uint64
	retryStaleEpoch                atomic.Uint64
	retryOther                     atomic.Uint64
	bgSkipTotal                    atomic.Uint64
	bgErrorTotal                   atomic.Uint64
	catalogLoadTotal               atomic.Uint64
	rootSealTotal                  atomic.Uint64
	rootSealMissingTotal           atomic.Uint64
	recoveryInstallTotal           atomic.Uint64
	recoverySkipTotal              atomic.Uint64
	visibleLogRecoverTotal         atomic.Uint64
	visibleLogRecoverSkipTotal     atomic.Uint64
	visibleLogRecoverOldEpochTotal atomic.Uint64
	visibleLogApplyMarkerTotal     atomic.Uint64
	visibleLogApplyErrorTotal      atomic.Uint64
	admissionWaitTotal             atomic.Uint64
	admissionWaiting               atomic.Int64
	admissionWaitLatencyTotal      atomic.Uint64
	admissionWaitLatencyLast       atomic.Uint64
	admissionWaitLatencyMax        atomic.Uint64

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

func (c *Runtime) recordWitnessBatch(records []fsperas.SegmentWitnessRecord) {
	if c == nil || len(records) == 0 {
		return
	}
	count := uint64(len(records))
	var bytes uint64
	for _, record := range records {
		bytes += uint64(segmentWitnessRecordBatchSize(record))
	}
	c.metrics.witnessBatchTotal.Add(1)
	c.metrics.witnessBatchRecordsTotal.Add(count)
	c.metrics.witnessBatchRecordsLast.Store(count)
	recordPerasMax(&c.metrics.witnessBatchRecordsMax, count)
	c.metrics.witnessBatchBytesTotal.Add(bytes)
	c.metrics.witnessBatchBytesLast.Store(bytes)
	recordPerasMax(&c.metrics.witnessBatchBytesMax, bytes)
}

func (c *Runtime) recordWitnessReplicaAppend(d time.Duration, err error) {
	if c == nil {
		return
	}
	c.metrics.witnessReplicaAppendTotal.Add(1)
	if err != nil {
		c.metrics.witnessReplicaAppendErrorTotal.Add(1)
	}
	recordPerasDuration(&c.metrics.witnessReplicaLatencyTotal, &c.metrics.witnessReplicaLatencyLast, &c.metrics.witnessReplicaLatencyMax, d)
}

func (c *Runtime) recordWitnessQuorum(d time.Duration, acks int) {
	if c == nil {
		return
	}
	c.metrics.witnessQuorumTotal.Add(1)
	recordPerasDuration(&c.metrics.witnessQuorumLatencyTotal, &c.metrics.witnessQuorumLatencyLast, &c.metrics.witnessQuorumLatencyMax, d)
	if acks < 0 {
		acks = 0
	}
	n := uint64(acks)
	c.metrics.witnessQuorumAcksLast.Store(n)
	recordPerasMax(&c.metrics.witnessQuorumAcksMax, n)
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

func (c *Runtime) recordAdmissionWait(d time.Duration) {
	recordPerasDuration(&c.metrics.admissionWaitLatencyTotal, &c.metrics.admissionWaitLatencyLast, &c.metrics.admissionWaitLatencyMax, d)
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
