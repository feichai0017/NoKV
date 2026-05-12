package raftstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

func (c *RemotePerasCommitter) appendSegmentWitnessesWithRetry(ctx context.Context, scope compile.AuthorityScope, holder *fsperas.Holder, segment fsperas.PerasSegment, payload []byte, digest [32]byte) error {
	var last error
	attempts := c.retries + 1
	for attempt := range attempts {
		err := c.appendSegmentWitnesses(ctx, scope, holder, segment, payload, digest)
		if err == nil {
			return nil
		}
		last = err
		if !errors.Is(err, fsperas.ErrSegmentWitnessQuorumUnavailable) || attempt == attempts-1 {
			break
		}
		c.retryTotal.Add(1)
		if !sleepContext(ctx, c.backoff) {
			return ctx.Err()
		}
	}
	return last
}

func (c *RemotePerasCommitter) appendSegmentWitnesses(ctx context.Context, scope compile.AuthorityScope, holder *fsperas.Holder, segment fsperas.PerasSegment, payload []byte, digest [32]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stats := segment.Stats()
	record := fsperas.SegmentWitnessRecord{
		EpochID:              holder.EpochID(),
		SegmentRoot:          segment.Root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   uint64(len(payload)),
		SegmentPayload:       runtimeCloneBytes(payload),
		OperationCount:       stats.OperationCount,
		EntryCount:           stats.EntryCount,
		TimestampUnixNano:    c.now().UnixNano(),
		HolderID:             holder.HolderID(),
	}
	broadcastCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		id  string
		err error
	}
	resultCh := make(chan result, len(c.witnesses))
	for _, witness := range c.witnesses {
		go func() {
			err := witness.AppendSegment(broadcastCtx, scope, record)
			resultCh <- result{id: witness.ID(), err: err}
		}()
	}
	acks := make([]string, 0, len(c.witnesses))
	failures := make([]error, 0, len(c.witnesses))
	for range c.witnesses {
		res := <-resultCh
		if res.err == nil {
			acks = append(acks, res.id)
			if len(acks) >= c.quorum {
				cancel()
				slices.Sort(acks)
				return nil
			}
			continue
		}
		failures = append(failures, fmt.Errorf("%s: %w", res.id, res.err))
	}
	if len(failures) == 0 {
		return fsperas.ErrSegmentWitnessQuorumUnavailable
	}
	return errors.Join(append([]error{fsperas.ErrSegmentWitnessQuorumUnavailable}, failures...)...)
}

func (c *RemotePerasCommitter) collectWitnessSegments(ctx context.Context, epochID uint64) ([]fsperas.SegmentWitnessRecord, error) {
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
