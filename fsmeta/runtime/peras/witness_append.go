// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

func (c *Runtime) appendSegmentWitnessBatchWithRetry(ctx context.Context, batch perasFlushBatch) error {
	var last error
	attempts := c.retries + 1
	for attempt := range attempts {
		err := c.appendSegmentWitnessBatch(ctx, batch)
		if err == nil {
			return nil
		}
		last = err
		if !errors.Is(err, fsperas.ErrSegmentWitnessQuorumUnavailable) || attempt == attempts-1 {
			break
		}
		c.metrics.retryTotal.Add(1)
		if !sleepContext(ctx, c.backoff) {
			return ctx.Err()
		}
	}
	return last
}

func (c *Runtime) appendSegmentWitnessBatch(ctx context.Context, batch perasFlushBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(batch.jobs) == 0 {
		return nil
	}
	grant, err := c.segmentWitnessGrant(ctx, batch.scope, batch.holder)
	if err != nil {
		return err
	}
	records := make([]fsperas.SegmentWitnessRecord, 0, len(batch.jobs))
	timestamp := batch.witnessUnixNano
	if timestamp <= 0 {
		timestamp = c.nextWitnessUnixNano()
	}
	for idx, job := range batch.jobs {
		records = append(records, c.segmentWitnessRecord(grant, batch.holder, job.segment, job.payload, job.digest, timestamp+int64(idx)))
	}
	return c.appendSegmentWitnessRecordsBounded(ctx, batch.scope, records)
}

func (c *Runtime) segmentWitnessGrant(ctx context.Context, scope compile.AuthorityScope, holder *fsperas.Holder) (rootproto.PerasAuthorityGrant, error) {
	grant, ok := c.epochs.grant(holder.EpochID())
	if !ok && c.authority != nil {
		active, owned, err := c.authority.Acquire(ctx, scope)
		if err != nil {
			return rootproto.PerasAuthorityGrant{}, err
		}
		if owned && active.EpochID == holder.EpochID() && active.HolderID == holder.HolderID() {
			grant = active
			ok = true
			c.epochs.installHolder(active, holder)
		}
	}
	if !ok {
		return rootproto.PerasAuthorityGrant{}, fmt.Errorf("peras witness grant missing for epoch %d: %w", holder.EpochID(), ErrRuntimeInvalid)
	}
	return grant, nil
}

func (c *Runtime) segmentWitnessRecord(grant rootproto.PerasAuthorityGrant, holder *fsperas.Holder, segment fsperas.PerasSegment, payload []byte, digest [32]byte, timestampUnixNano int64) fsperas.SegmentWitnessRecord {
	stats := segment.Stats()
	return fsperas.SegmentWitnessRecord{
		EpochID:              holder.EpochID(),
		SegmentRoot:          segment.Root,
		SegmentPayloadDigest: digest,
		PredecessorDigest:    grant.PredecessorDigest,
		SegmentPayloadSize:   uint64(len(payload)),
		SegmentPayload:       cloneBytes(payload),
		OperationCount:       stats.OperationCount,
		EntryCount:           stats.EntryCount,
		TimestampUnixNano:    timestampUnixNano,
		HolderID:             holder.HolderID(),
	}
}

func (c *Runtime) appendSegmentWitnessRecords(ctx context.Context, scope compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	if len(records) == 0 {
		return nil
	}
	type result struct {
		id      string
		latency time.Duration
		err     error
	}
	start := time.Now()
	witnesses := preferredWitnessQuorumOrder(c.witnesses, records)
	acks := make([]string, 0, len(c.witnesses))
	failures := make([]error, 0, len(c.witnesses))

	appendTo := func(targets []fsperas.WitnessReplica) bool {
		if len(targets) == 0 {
			return len(acks) >= c.quorum
		}
		resultCh := make(chan result, len(targets))
		for _, witness := range targets {
			go func(witness fsperas.WitnessReplica) {
				replicaStart := time.Now()
				err := witness.AppendSegments(ctx, scope, records)
				resultCh <- result{id: witness.ID(), latency: time.Since(replicaStart), err: err}
			}(witness)
		}
		for range targets {
			res := <-resultCh
			c.recordWitnessReplicaAppend(res.latency, res.err)
			if res.err == nil {
				acks = append(acks, res.id)
				continue
			}
			failures = append(failures, fmt.Errorf("%s: %w", res.id, res.err))
		}
		return len(acks) >= c.quorum
	}

	preferred := min(c.quorum, len(witnesses))
	if appendTo(witnesses[:preferred]) || appendTo(witnesses[preferred:]) {
		slices.Sort(acks)
		c.recordWitnessQuorum(time.Since(start), len(acks))
		c.recordWitnessBatch(records)
		return nil
	}
	c.recordWitnessQuorum(time.Since(start), len(acks))
	if len(failures) == 0 {
		return fsperas.ErrSegmentWitnessQuorumUnavailable
	}
	return errors.Join(append([]error{fsperas.ErrSegmentWitnessQuorumUnavailable}, failures...)...)
}

type witnessQuorumCandidate struct {
	witness fsperas.WitnessReplica
	score   uint64
	id      string
}

func preferredWitnessQuorumOrder(witnesses []fsperas.WitnessReplica, records []fsperas.SegmentWitnessRecord) []fsperas.WitnessReplica {
	if len(witnesses) == 0 {
		return nil
	}
	candidates := make([]witnessQuorumCandidate, 0, len(witnesses))
	for _, witness := range witnesses {
		if witness == nil {
			continue
		}
		candidates = append(candidates, witnessQuorumCandidate{
			witness: witness,
			score:   witnessQuorumScore(witness.ID(), records),
			id:      witness.ID(),
		})
	}
	slices.SortFunc(candidates, func(left, right witnessQuorumCandidate) int {
		if left.score < right.score {
			return -1
		}
		if left.score > right.score {
			return 1
		}
		if left.id < right.id {
			return -1
		}
		if left.id > right.id {
			return 1
		}
		return 0
	})
	out := make([]fsperas.WitnessReplica, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.witness)
	}
	return out
}

func witnessQuorumScore(id string, records []fsperas.SegmentWitnessRecord) uint64 {
	h := sha256.New()
	_, _ = h.Write([]byte(id))
	for _, record := range records {
		_, _ = h.Write(record.SegmentRoot[:])
		_, _ = h.Write(record.SegmentPayloadDigest[:])
	}
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

func (c *Runtime) appendSegmentWitnessRecordsBounded(ctx context.Context, scope compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	for _, batch := range splitSegmentWitnessRecords(records, defaultPerasWitnessBatchMaxBytes) {
		if err := c.appendSegmentWitnessRecords(ctx, scope, batch); err != nil {
			return err
		}
	}
	return nil
}

func splitSegmentWitnessRecords(records []fsperas.SegmentWitnessRecord, maxBytes int) [][]fsperas.SegmentWitnessRecord {
	if len(records) == 0 {
		return nil
	}
	if maxBytes <= 0 {
		return [][]fsperas.SegmentWitnessRecord{records}
	}
	out := make([][]fsperas.SegmentWitnessRecord, 0, 1)
	start := 0
	size := 0
	for idx, record := range records {
		recordSize := segmentWitnessRecordBatchSize(record)
		if idx > start && size+recordSize > maxBytes {
			out = append(out, records[start:idx])
			start = idx
			size = 0
		}
		size += recordSize
	}
	out = append(out, records[start:])
	return out
}

func segmentWitnessRecordBatchSize(record fsperas.SegmentWitnessRecord) int {
	return 256 + len(record.SegmentPointer) + len(record.SegmentPayload)
}

func (c *Runtime) collectWitnessSegments(ctx context.Context, epochID uint64) ([]fsperas.SegmentWitnessRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		id       string
		snapshot fsperas.WitnessSnapshot
		err      error
	}
	resultCh := make(chan result, len(c.witnesses))
	for _, witness := range c.witnesses {
		go func() {
			snapshot, err := witness.Probe(ctx, epochID)
			resultCh <- result{id: witness.ID(), snapshot: snapshot, err: err}
		}()
	}
	type key struct {
		root   [32]byte
		digest [32]byte
	}
	records := make(map[key]fsperas.SegmentWitnessRecord)
	failures := make([]error, 0, len(c.witnesses))
	successes := 0
	for range c.witnesses {
		res := <-resultCh
		if res.err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", res.id, res.err))
			continue
		}
		successes++
		for _, record := range res.snapshot.Segments {
			if record.EpochID != epochID {
				continue
			}
			k := key{root: record.SegmentRoot, digest: record.SegmentPayloadDigest}
			current, ok := records[k]
			if !ok || len(record.SegmentPayload) > len(current.SegmentPayload) {
				records[k] = record
			}
		}
	}
	if successes == 0 {
		if len(failures) == 0 {
			return nil, fsperas.ErrSegmentWitnessQuorumUnavailable
		}
		return nil, errors.Join(append([]error{fsperas.ErrSegmentWitnessQuorumUnavailable}, failures...)...)
	}
	out := make([]fsperas.SegmentWitnessRecord, 0, len(records))
	for _, record := range records {
		out = append(out, record)
	}
	slices.SortFunc(out, func(a, b fsperas.SegmentWitnessRecord) int {
		if a.TimestampUnixNano < b.TimestampUnixNano {
			return -1
		}
		if a.TimestampUnixNano > b.TimestampUnixNano {
			return 1
		}
		return bytes.Compare(a.SegmentRoot[:], b.SegmentRoot[:])
	})
	return out, nil
}

func (c *Runtime) collectWitnessSegment(ctx context.Context, ref fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return fsperas.SegmentWitnessRecord{}, false, err
	}
	if !ref.Valid() {
		return fsperas.SegmentWitnessRecord{}, false, fsperas.ErrInvalidWitnessRecord
	}
	type result struct {
		id     string
		record fsperas.SegmentWitnessRecord
		found  bool
		err    error
	}
	resultCh := make(chan result, len(c.witnesses))
	for _, witness := range c.witnesses {
		go func(witness fsperas.WitnessReplica) {
			record, found, err := probeWitnessSegment(ctx, witness, ref)
			resultCh <- result{id: witness.ID(), record: record, found: found, err: err}
		}(witness)
	}
	failures := make([]error, 0, len(c.witnesses))
	successes := 0
	var selected fsperas.SegmentWitnessRecord
	found := false
	for range c.witnesses {
		res := <-resultCh
		if res.err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", res.id, res.err))
			continue
		}
		successes++
		if !res.found {
			continue
		}
		if !found || len(res.record.SegmentPayload) > len(selected.SegmentPayload) {
			selected = res.record
			found = true
		}
	}
	if successes == 0 {
		if len(failures) == 0 {
			return fsperas.SegmentWitnessRecord{}, false, fsperas.ErrSegmentWitnessQuorumUnavailable
		}
		return fsperas.SegmentWitnessRecord{}, false, errors.Join(append([]error{fsperas.ErrSegmentWitnessQuorumUnavailable}, failures...)...)
	}
	return selected, found, nil
}

func probeWitnessSegment(ctx context.Context, witness fsperas.WitnessReplica, ref fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	if prober, ok := witness.(fsperas.WitnessSegmentProber); ok {
		return prober.ProbeSegment(ctx, ref)
	}
	snapshot, err := witness.Probe(ctx, ref.EpochID)
	if err != nil {
		return fsperas.SegmentWitnessRecord{}, false, err
	}
	for _, record := range snapshot.Segments {
		if record.EpochID == ref.EpochID && record.SegmentRoot == ref.SegmentRoot && record.SegmentPayloadDigest == ref.SegmentPayloadDigest {
			return record, true, nil
		}
	}
	return fsperas.SegmentWitnessRecord{}, false, nil
}
