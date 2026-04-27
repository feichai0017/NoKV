package vlog

// Discard-stats live behind the Consumer: per-segment "discarded bytes"
// counters that LSM compaction maintains via the host-provided
// DiscardStatsCh. The Consumer aggregates flushes coming through that
// channel and persists the merged map into the LSM under DiscardStatsKey
// so a fresh Open can warm them via PopulateDiscardStats.

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

type discardStats struct {
	sync.RWMutex
	m                 map[manifest.ValueLogID]int64
	flushChan         chan map[manifest.ValueLogID]int64
	closer            *utils.Closer
	updatesSinceFlush int
	flushThreshold    int
}

func newDiscardStats(flushChan chan map[manifest.ValueLogID]int64, flushThreshold int) *discardStats {
	if flushThreshold <= 0 {
		flushThreshold = DefaultDiscardStatsFlushThreshold
	}
	return &discardStats{
		m:              make(map[manifest.ValueLogID]int64),
		flushChan:      flushChan,
		closer:         utils.NewCloser(),
		flushThreshold: flushThreshold,
	}
}

// EncodeDiscardStats serializes the per-segment counters as JSON. Keys are
// rendered as "bucket:fid" since JSON requires string keys.
func EncodeDiscardStats(stats map[manifest.ValueLogID]int64) ([]byte, error) {
	wire := make(map[string]int64, len(stats))
	for id, count := range stats {
		wire[fmt.Sprintf("%d:%d", id.Bucket, id.FileID)] = count
	}
	return json.Marshal(wire)
}

// DecodeDiscardStats is the inverse of EncodeDiscardStats. Returns
// (nil, nil) for empty input so callers can short-circuit cleanly.
func DecodeDiscardStats(data []byte) (map[manifest.ValueLogID]int64, error) {
	if len(data) == 0 {
		return nil, nil
	}
	wire := make(map[string]int64)
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, err
	}
	out := make(map[manifest.ValueLogID]int64, len(wire))
	for key, count := range wire {
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid discard stat key: %s", key)
		}
		bucket, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid discard stat bucket: %w", err)
		}
		fid, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid discard stat fid: %w", err)
		}
		out[manifest.ValueLogID{Bucket: uint32(bucket), FileID: uint32(fid)}] = count
	}
	return out, nil
}

// flushDiscardStats is the long-running goroutine that drains
// DiscardStatsCh, merges incoming counters into the in-memory map, and
// persists the snapshot through SendToWriteCh whenever updatesSinceFlush
// exceeds flushThreshold (or on shutdown drain).
func (c *Consumer) flushDiscardStats() {
	defer c.lfDiscardStats.closer.Done()

	mergeStats := func(stats map[manifest.ValueLogID]int64, force bool) ([]byte, error) {
		c.lfDiscardStats.Lock()
		defer c.lfDiscardStats.Unlock()
		for fid, count := range stats {
			c.lfDiscardStats.m[fid] += count
			c.lfDiscardStats.updatesSinceFlush++
		}

		threshold := c.lfDiscardStats.flushThreshold

		if !force && c.lfDiscardStats.updatesSinceFlush < threshold {
			return nil, nil
		}
		if c.lfDiscardStats.updatesSinceFlush == 0 {
			return nil, nil
		}

		encodedDS, err := EncodeDiscardStats(c.lfDiscardStats.m)
		if err != nil {
			return nil, err
		}
		c.lfDiscardStats.updatesSinceFlush = 0
		return encodedDS, nil
	}

	process := func(stats map[manifest.ValueLogID]int64, force bool) error {
		encodedDS, err := mergeStats(stats, force)
		if err != nil || encodedDS == nil {
			return err
		}

		entry := kv.NewInternalEntry(kv.CFDefault, DiscardStatsKey, 1, encodedDS, 0, 0)
		entries := []*kv.Entry{entry}
		req, err := c.deps.SendToWriteCh(entries, false)
		if err != nil {
			entry.DecrRef()
			return errors.Wrapf(err, "failed to push discard stats to write channel")
		}
		return req.Wait()
	}

	closer := c.lfDiscardStats.closer
	for {
		select {
		case <-closer.Closed():
			for {
				select {
				case stats := <-c.lfDiscardStats.flushChan:
					if err := process(stats, false); err != nil {
						slog.Default().Error("process discard stats", "error", err)
					}
				default:
					goto drainComplete
				}
			}
		drainComplete:
			if err := process(nil, true); err != nil {
				slog.Default().Error("process discard stats", "error", err)
			}
			return
		case stats := <-c.lfDiscardStats.flushChan:
			if err := process(stats, false); err != nil {
				slog.Default().Error("process discard stats", "error", err)
			}
		}
	}
}

// PopulateDiscardStats is called once at Open to warm the discard-stats
// map from the LSM-persisted snapshot. The snapshot lives at
// DiscardStatsKey under CFDefault as either an inline value or, if the
// payload is large, a value pointer.
func (c *Consumer) PopulateDiscardStats() error {
	if c == nil || c.deps.GetInternalEntry == nil {
		return nil
	}
	vs, err := c.deps.GetInternalEntry(kv.CFDefault, DiscardStatsKey, kv.MaxVersion)
	if err != nil {
		return err
	}
	defer vs.DecrRef()
	if vs.Meta == 0 && len(vs.Value) == 0 {
		return nil
	}
	val := vs.Value
	if kv.IsValuePtr(vs) {
		var vp kv.ValuePtr
		vp.Decode(val)
		result, cb, err := c.Read(&vp)
		val = kv.SafeCopy(nil, result)
		if cb != nil {
			cb()
		}
		if err != nil {
			return err
		}
	}
	if len(val) == 0 {
		return nil
	}
	statsMap, err := DecodeDiscardStats(val)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal discard stats")
	}
	c.Logf("Value Log Discard stats: %v", statsMap)
	c.lfDiscardStats.flushChan <- statsMap
	return nil
}
