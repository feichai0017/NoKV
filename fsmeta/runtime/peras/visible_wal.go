// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
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

type visibleOperationLogEntry struct {
	record    fsperas.VisibleOperationRecord
	segmentID uint32
}

func NewWALVisibleLog(manager *wal.Manager, durability wal.DurabilityPolicy) (*WALVisibleLog, error) {
	if manager == nil {
		return nil, fsperas.ErrVisibleLogRequired
	}
	return &WALVisibleLog{wal: manager, durability: durability, pending: make(map[visibleOperationLogKey]visibleOperationLogEntry)}, nil
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
	info, err := l.appendPayload(payload)
	if err != nil {
		return fmt.Errorf("append peras visible WAL: %w", err)
	}
	l.mu.Lock()
	l.records = append(l.records, cloneVisibleOperationRecord(record))
	l.initPendingLocked()
	l.pending[visibleOperationLogKeyForRecord(record)] = visibleOperationLogEntry{
		record:    cloneVisibleOperationRecord(record),
		segmentID: info.SegmentID,
	}
	l.mu.Unlock()
	return nil
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
	l.records = removeVisibleAppliedRecords(l.records, record)
	l.removeAppliedPendingLocked(record)
	canCompact := l.replayed
	l.mu.Unlock()
	if canCompact {
		_ = l.CompactApplied()
	}
	return nil
}

func (l *WALVisibleLog) ReplayVisible(ctx context.Context) ([]fsperas.VisibleOperationRecord, error) {
	if l == nil || l.wal == nil {
		return nil, fsperas.ErrVisibleLogRequired
	}
	records := make([]fsperas.VisibleOperationRecord, 0)
	latest := make(map[visibleOperationLogKey]int)
	pending := make(map[visibleOperationLogKey]visibleOperationLogEntry)
	applied := make(map[visibleOperationLogKey]fsperas.VisibleOperationReference)
	err := l.wal.ReplayFiltered(func(info wal.EntryInfo) bool {
		return info.Type == wal.RecordTypePerasVisible
	}, func(info wal.EntryInfo, payload []byte) error {
		if err := visibleLogContextErr(ctx); err != nil {
			return err
		}
		if record, err := fsperas.DecodeVisibleAppliedRecord(payload); err == nil {
			for _, ref := range record.Operations {
				applied[visibleOperationLogKey{epoch: record.EpochID, holder: record.HolderID, id: ref.OpID}] = ref
			}
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
		pending[key] = visibleOperationLogEntry{record: cloneVisibleOperationRecord(record), segmentID: info.SegmentID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := records[:0]
	for _, record := range records {
		key := visibleOperationLogKeyForRecord(record)
		ref, ok := applied[key]
		if ok {
			current, err := fsperas.VisibleOperationReferenceFromReplay(record.Operation)
			if err == nil && current == ref {
				delete(pending, key)
				continue
			}
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
		if entry.segmentID == 0 {
			continue
		}
		if retain == 0 || entry.segmentID < retain {
			retain = entry.segmentID
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

func removeVisibleAppliedRecords(records []fsperas.VisibleOperationRecord, applied fsperas.VisibleAppliedRecord) []fsperas.VisibleOperationRecord {
	if len(records) == 0 || len(applied.Operations) == 0 {
		return records
	}
	refs := make(map[fsperas.OperationID]fsperas.VisibleOperationReference, len(applied.Operations))
	for _, ref := range applied.Operations {
		refs[ref.OpID] = ref
	}
	out := records[:0]
	for _, record := range records {
		if record.EpochID == applied.EpochID && record.HolderID == applied.HolderID {
			if ref, ok := refs[record.Operation.OpID]; ok {
				current, err := fsperas.VisibleOperationReferenceFromReplay(record.Operation)
				if err == nil && current == ref {
					continue
				}
			}
		}
		out = append(out, record)
	}
	return out
}

func (l *WALVisibleLog) removeAppliedPendingLocked(applied fsperas.VisibleAppliedRecord) {
	l.initPendingLocked()
	for _, ref := range applied.Operations {
		key := visibleOperationLogKey{epoch: applied.EpochID, holder: applied.HolderID, id: ref.OpID}
		entry, ok := l.pending[key]
		if !ok {
			continue
		}
		current, err := fsperas.VisibleOperationReferenceFromReplay(entry.record.Operation)
		if err == nil && current == ref {
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
)
