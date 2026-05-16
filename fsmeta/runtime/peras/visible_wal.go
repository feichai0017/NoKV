// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

type WALVisibleLog struct {
	wal        *wal.Manager
	durability wal.DurabilityPolicy
	mu         sync.RWMutex
	records    []fsperas.VisibleOperationRecord
	pending    map[visibleOperationLogKey]visibleOperationLogEntry
	replayed   bool
}

type visibleOperationLogKey struct {
	epoch  uint64
	holder string
	id     fsperas.OperationID
}

type visibleAppliedLogKey struct {
	epoch  uint64
	holder string
}

type visibleOperationLogEntry struct {
	record   fsperas.VisibleOperationRecord
	position visibleOperationLogPosition
}

type visibleOperationLogPosition struct {
	segmentID   uint32
	startOffset uint64
	endOffset   uint64
}

func NewWALVisibleLog(manager *wal.Manager, durability wal.DurabilityPolicy) (*WALVisibleLog, error) {
	if manager == nil {
		return nil, fsperas.ErrVisibleLogRequired
	}
	return &WALVisibleLog{
		wal:        manager,
		durability: durability,
		pending:    make(map[visibleOperationLogKey]visibleOperationLogEntry),
	}, nil
}

func (l *WALVisibleLog) VisibleLogPolicy() string {
	if l == nil {
		return "disabled"
	}
	return walDurabilityPolicyName(l.durability)
}

func (l *WALVisibleLog) AppendVisible(ctx context.Context, record fsperas.VisibleOperationRecord) error {
	if err := visibleLogContextErr(ctx); err != nil {
		return err
	}
	payload, err := fsperas.EncodeVisibleOperationRecord(record)
	if err != nil {
		return fmt.Errorf("encode peras visible record: %w", err)
	}
	if err := l.appendVisibleRecord(record, payload); err != nil {
		return fmt.Errorf("append peras visible WAL: %w", err)
	}
	return nil
}

func (l *WALVisibleLog) appendVisibleRecord(record fsperas.VisibleOperationRecord, payload []byte) error {
	info, err := l.appendPayload(payload)
	if err != nil {
		return err
	}
	position, err := visibleOperationLogPositionFromEntry(info)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.rememberVisibleRecordLocked(record, position)
	l.mu.Unlock()
	return nil
}

func (l *WALVisibleLog) rememberVisibleRecordLocked(record fsperas.VisibleOperationRecord, position visibleOperationLogPosition) {
	l.initPendingLocked()
	cloned := cloneVisibleOperationRecord(record)
	l.records = append(l.records, cloned)
	l.pending[visibleOperationLogKeyForRecord(cloned)] = visibleOperationLogEntry{
		record:   cloneVisibleOperationRecord(cloned),
		position: position,
	}
}

func (l *WALVisibleLog) AppendVisibleApplied(ctx context.Context, record fsperas.VisibleAppliedRecord) error {
	if err := visibleLogContextErr(ctx); err != nil {
		return err
	}
	payload, err := fsperas.EncodeVisibleAppliedRecord(record)
	if err != nil {
		return err
	}
	if _, err := l.appendPayload(payload); err != nil {
		return err
	}
	l.mu.Lock()
	l.records = removeVisibleAppliedRecords(l.records, l.pending, record)
	l.removeAppliedPendingLocked(record)
	canCompact := l.replayed
	l.mu.Unlock()
	if canCompact {
		_ = l.CompactApplied()
	}
	return nil
}

func (l *WALVisibleLog) AppendVisibleReplayPlanApplied(ctx context.Context, epochID uint64, holderID string, plan fsperas.ReplayPlan) error {
	if err := visibleLogContextErr(ctx); err != nil {
		return err
	}
	record, err := l.visibleAppliedRecordForReplayPlan(epochID, holderID, plan)
	if err != nil {
		return err
	}
	return l.AppendVisibleApplied(ctx, record)
}

func (l *WALVisibleLog) visibleAppliedRecordForReplayPlan(epochID uint64, holderID string, plan fsperas.ReplayPlan) (fsperas.VisibleAppliedRecord, error) {
	if l == nil || l.wal == nil {
		return fsperas.VisibleAppliedRecord{}, fsperas.ErrVisibleLogRequired
	}
	if epochID == 0 || holderID == "" || len(plan.Operations) == 0 {
		return fsperas.VisibleAppliedRecord{}, fsperas.ErrInvalidWitnessRecord
	}
	positions := make([]visibleOperationLogPosition, 0, len(plan.Operations))
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, op := range plan.Operations {
		if !op.OpID.Valid() {
			return fsperas.VisibleAppliedRecord{}, fsperas.ErrInvalidOperationID
		}
		key := visibleOperationLogKey{epoch: epochID, holder: holderID, id: op.OpID}
		entry, ok := l.pending[key]
		if !ok {
			return fsperas.VisibleAppliedRecord{}, fsperas.ErrInvalidWitnessRecord
		}
		if err := visibleOperationMatchesReplay(entry.record.Operation, op); err != nil {
			return fsperas.VisibleAppliedRecord{}, err
		}
		positions = append(positions, entry.position)
	}
	return fsperas.VisibleAppliedRecord{
		EpochID:  epochID,
		HolderID: holderID,
		Ranges:   coalesceVisibleAppliedRanges(positions),
	}, nil
}

func (l *WALVisibleLog) ReplayVisible(ctx context.Context) ([]fsperas.VisibleOperationRecord, error) {
	if l == nil || l.wal == nil {
		return nil, fsperas.ErrVisibleLogRequired
	}
	records := make([]fsperas.VisibleOperationRecord, 0)
	latest := make(map[visibleOperationLogKey]int)
	pending := make(map[visibleOperationLogKey]visibleOperationLogEntry)
	applied := make(map[visibleAppliedLogKey][]fsperas.VisibleAppliedRange)
	err := l.wal.ReplayFiltered(func(info wal.EntryInfo) bool {
		return info.Type == wal.RecordTypePerasVisible
	}, func(info wal.EntryInfo, payload []byte) error {
		if err := visibleLogContextErr(ctx); err != nil {
			return err
		}
		if record, err := fsperas.DecodeVisibleAppliedRecord(payload); err == nil {
			key := visibleAppliedLogKey{epoch: record.EpochID, holder: record.HolderID}
			applied[key] = append(applied[key], record.Ranges...)
			return nil
		}
		record, err := fsperas.DecodeVisibleOperationRecord(payload)
		if err != nil {
			return err
		}
		key := visibleOperationLogKeyForRecord(record)
		if idx, ok := latest[key]; ok {
			records[idx] = record
		} else {
			latest[key] = len(records)
			records = append(records, record)
		}
		position, err := visibleOperationLogPositionFromEntry(info)
		if err != nil {
			return err
		}
		pending[key] = visibleOperationLogEntry{record: cloneVisibleOperationRecord(record), position: position}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := records[:0]
	for _, record := range records {
		key := visibleOperationLogKeyForRecord(record)
		entry, ok := pending[key]
		if ok && visibleOperationPositionApplied(entry.position, applied[visibleAppliedLogKey{epoch: key.epoch, holder: key.holder}]) {
			delete(pending, key)
			continue
		}
		out = append(out, record)
	}
	l.mu.Lock()
	l.records = cloneVisibleOperationRecords(out)
	l.pending = pending
	l.replayed = true
	l.mu.Unlock()
	return cloneVisibleOperationRecords(out), nil
}

func (l *WALVisibleLog) Records() []fsperas.VisibleOperationRecord {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return cloneVisibleOperationRecords(l.records)
}

func (l *WALVisibleLog) CompactApplied() error {
	if l == nil || l.wal == nil {
		return fsperas.ErrVisibleLogRequired
	}
	retain := l.firstRetainedSegment()
	if retain <= 1 {
		return nil
	}
	files, err := l.wal.ListSegments()
	if err != nil {
		return err
	}
	for _, file := range files {
		id, ok := visibleWALSegmentID(file)
		if !ok || id >= retain {
			continue
		}
		if err := l.wal.RemoveSegment(id); err != nil && !errors.Is(err, wal.ErrSegmentRetained) {
			return err
		}
	}
	return nil
}

func (l *WALVisibleLog) firstRetainedSegment() uint32 {
	active := l.wal.ActiveSegment()
	retain := active
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, entry := range l.pending {
		if entry.position.segmentID == 0 {
			continue
		}
		if retain == 0 || entry.position.segmentID < retain {
			retain = entry.position.segmentID
		}
	}
	return retain
}

func (l *WALVisibleLog) appendPayload(payload []byte) (wal.EntryInfo, error) {
	if l == nil || l.wal == nil {
		return wal.EntryInfo{}, fsperas.ErrVisibleLogRequired
	}
	infos, err := l.wal.AppendRecords(l.durability, wal.Record{
		Type:    wal.RecordTypePerasVisible,
		Payload: payload,
	})
	if err != nil {
		return wal.EntryInfo{}, err
	}
	if len(infos) != 1 {
		return wal.EntryInfo{}, fsperas.ErrInvalidWitnessRecord
	}
	return infos[0], nil
}

func visibleLogContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func visibleOperationMatchesReplay(current, applied fsperas.ReplayOperation) error {
	currentRef, err := fsperas.VisibleOperationReferenceFromReplay(current)
	if err != nil {
		return err
	}
	appliedRef, err := fsperas.VisibleOperationReferenceFromReplay(applied)
	if err != nil {
		return err
	}
	if currentRef != appliedRef {
		return fsperas.ErrInvalidWitnessRecord
	}
	return nil
}

func visibleOperationLogPositionFromEntry(info wal.EntryInfo) (visibleOperationLogPosition, error) {
	if info.SegmentID == 0 || info.Offset < 0 || info.Length == 0 {
		return visibleOperationLogPosition{}, fsperas.ErrInvalidWitnessRecord
	}
	start := uint64(info.Offset)
	end := start + uint64(info.Length)
	if end <= start {
		return visibleOperationLogPosition{}, fsperas.ErrInvalidWitnessRecord
	}
	return visibleOperationLogPosition{
		segmentID:   info.SegmentID,
		startOffset: start,
		endOffset:   end,
	}, nil
}

func coalesceVisibleAppliedRanges(positions []visibleOperationLogPosition) []fsperas.VisibleAppliedRange {
	if len(positions) == 0 {
		return nil
	}
	sort.Slice(positions, func(i, j int) bool {
		if positions[i].segmentID != positions[j].segmentID {
			return positions[i].segmentID < positions[j].segmentID
		}
		if positions[i].startOffset != positions[j].startOffset {
			return positions[i].startOffset < positions[j].startOffset
		}
		return positions[i].endOffset < positions[j].endOffset
	})
	ranges := make([]fsperas.VisibleAppliedRange, 0, len(positions))
	for _, position := range positions {
		if len(ranges) > 0 {
			last := &ranges[len(ranges)-1]
			if last.SegmentID == position.segmentID && last.EndOffset == position.startOffset {
				last.EndOffset = position.endOffset
				continue
			}
			if last.SegmentID == position.segmentID && last.StartOffset == position.startOffset && last.EndOffset == position.endOffset {
				continue
			}
		}
		ranges = append(ranges, fsperas.VisibleAppliedRange{
			SegmentID:   position.segmentID,
			StartOffset: position.startOffset,
			EndOffset:   position.endOffset,
		})
	}
	return ranges
}

func visibleOperationPositionApplied(position visibleOperationLogPosition, ranges []fsperas.VisibleAppliedRange) bool {
	if position.segmentID == 0 || len(ranges) == 0 {
		return false
	}
	for _, applied := range ranges {
		if applied.SegmentID == position.segmentID &&
			applied.StartOffset <= position.startOffset &&
			applied.EndOffset >= position.endOffset {
			return true
		}
	}
	return false
}

func removeVisibleAppliedRecords(records []fsperas.VisibleOperationRecord, pending map[visibleOperationLogKey]visibleOperationLogEntry, applied fsperas.VisibleAppliedRecord) []fsperas.VisibleOperationRecord {
	if len(records) == 0 || len(applied.Ranges) == 0 {
		return records
	}
	out := records[:0]
	for _, record := range records {
		if record.EpochID == applied.EpochID && record.HolderID == applied.HolderID {
			key := visibleOperationLogKeyForRecord(record)
			entry, ok := pending[key]
			if ok && visibleOperationPositionApplied(entry.position, applied.Ranges) {
				continue
			}
		}
		out = append(out, record)
	}
	return out
}

func (l *WALVisibleLog) removeAppliedPendingLocked(applied fsperas.VisibleAppliedRecord) {
	l.initPendingLocked()
	for key, entry := range l.pending {
		if key.epoch == applied.EpochID && key.holder == applied.HolderID && visibleOperationPositionApplied(entry.position, applied.Ranges) {
			delete(l.pending, key)
		}
	}
}

func (l *WALVisibleLog) initPendingLocked() {
	if l.pending == nil {
		l.pending = make(map[visibleOperationLogKey]visibleOperationLogEntry)
	}
}

func visibleOperationLogKeyForRecord(record fsperas.VisibleOperationRecord) visibleOperationLogKey {
	return visibleOperationLogKey{
		epoch:  record.EpochID,
		holder: record.HolderID,
		id:     record.Operation.OpID,
	}
}

func visibleWALSegmentID(path string) (uint32, bool) {
	name := filepath.Base(path)
	name = strings.TrimSuffix(name, ".wal")
	id, err := strconv.ParseUint(name, 10, 32)
	if err != nil || id == 0 {
		return 0, false
	}
	return uint32(id), true
}

func walDurabilityPolicyName(policy wal.DurabilityPolicy) string {
	switch policy {
	case wal.DurabilityBuffered:
		return "buffered"
	case wal.DurabilityFlushed:
		return "flushed"
	case wal.DurabilityFsync:
		return "fsync"
	case wal.DurabilityFsyncBatched:
		return "fsync-batched"
	default:
		return "unknown"
	}
}

func cloneVisibleOperationRecords(in []fsperas.VisibleOperationRecord) []fsperas.VisibleOperationRecord {
	if len(in) == 0 {
		return nil
	}
	out := make([]fsperas.VisibleOperationRecord, len(in))
	for i, record := range in {
		out[i] = cloneVisibleOperationRecord(record)
	}
	return out
}

func cloneVisibleOperationRecord(record fsperas.VisibleOperationRecord) fsperas.VisibleOperationRecord {
	record.Scope = CloneScope(record.Scope)
	record.Operation = cloneReplayOperation(record.Operation)
	return record
}

func cloneReplayOperation(op fsperas.ReplayOperation) fsperas.ReplayOperation {
	op.PredicateProofs = clonePredicateProofs(op.PredicateProofs)
	op.GuardProofs = append([]proof.GuardProof(nil), op.GuardProofs...)
	op.Atomicity.Members = append([]compile.MutationID(nil), op.Atomicity.Members...)
	op.Mutations = cloneReplayMutations(op.Mutations)
	return op
}

func clonePredicateProofs(in []proof.PredicateProof) []proof.PredicateProof {
	if len(in) == 0 {
		return nil
	}
	out := make([]proof.PredicateProof, len(in))
	for i, predicate := range in {
		out[i] = predicate
		out[i].Key = cloneBytes(predicate.Key)
		out[i].Value = cloneBytes(predicate.Value)
	}
	return out
}

func cloneReplayMutations(in []fsperas.ReplayMutation) []fsperas.ReplayMutation {
	if len(in) == 0 {
		return nil
	}
	out := make([]fsperas.ReplayMutation, len(in))
	for i, mutation := range in {
		out[i] = fsperas.ReplayMutation{
			Key:    cloneBytes(mutation.Key),
			Value:  cloneBytes(mutation.Value),
			Delete: mutation.Delete,
		}
	}
	return out
}

var (
	_ fsperas.VisibleLog         = (*WALVisibleLog)(nil)
	_ fsperas.VisibleLogApplier  = (*WALVisibleLog)(nil)
	_ fsperas.VisibleLogReplayer = (*WALVisibleLog)(nil)
	_ visibleReplayPlanApplier   = (*WALVisibleLog)(nil)
)
