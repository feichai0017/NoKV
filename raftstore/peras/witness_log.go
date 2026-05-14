// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"sync"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

type LocalWitnessReplica struct {
	id  string
	log *WALWitnessLog
}

func NewLocalWitnessReplica(id string, log *WALWitnessLog) (*LocalWitnessReplica, error) {
	if id == "" || log == nil {
		return nil, fsperas.ErrWitnessReplicaInvalid
	}
	return &LocalWitnessReplica{id: id, log: log}, nil
}

func (r *LocalWitnessReplica) ID() string {
	if r == nil {
		return ""
	}
	return r.id
}

func (r *LocalWitnessReplica) AppendSegment(ctx context.Context, _ compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	if r == nil || r.log == nil {
		return fsperas.ErrWitnessLogRequired
	}
	_, err := r.log.AppendSegment(ctx, record)
	return err
}

func (r *LocalWitnessReplica) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if r == nil || r.log == nil {
		return fsperas.WitnessSnapshot{}, fsperas.ErrWitnessLogRequired
	}
	return r.log.Probe(ctx, epochID)
}

type WALWitnessLog struct {
	wal        *wal.Manager
	durability wal.DurabilityPolicy
	mu         sync.RWMutex
	segments   []fsperas.SegmentWitnessRecord
}

func NewWALWitnessLog(manager *wal.Manager, durability wal.DurabilityPolicy) (*WALWitnessLog, error) {
	if manager == nil {
		return nil, fsperas.ErrWitnessLogRequired
	}
	return &WALWitnessLog{wal: manager, durability: durability}, nil
}

func (l *WALWitnessLog) AppendSegment(ctx context.Context, record fsperas.SegmentWitnessRecord) (wal.EntryInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return wal.EntryInfo{}, err
	}
	payload, err := fsperas.EncodeSegmentWitnessRecord(record)
	if err != nil {
		return wal.EntryInfo{}, err
	}
	info, err := l.appendPayload(payload)
	if err != nil {
		return wal.EntryInfo{}, err
	}
	l.mu.Lock()
	l.segments = append(l.segments, record)
	l.mu.Unlock()
	return info, nil
}

func (l *WALWitnessLog) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if l == nil || l.wal == nil {
		return fsperas.WitnessSnapshot{}, fsperas.ErrWitnessLogRequired
	}
	segments := make(map[[32]byte]fsperas.SegmentWitnessRecord)
	err := l.wal.ReplayFiltered(
		func(info wal.EntryInfo) bool {
			return info.Type == wal.RecordTypePerasWitness
		},
		func(_ wal.EntryInfo, payload []byte) error {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			frame, err := fsperas.DecodeWitnessFrame(payload)
			if err != nil {
				return err
			}
			if frame.Kind != fsperas.WitnessRecordSegment {
				return fsperas.ErrInvalidWitnessRecord
			}
			if frame.Segment.EpochID == epochID {
				segments[frame.Segment.SegmentRoot] = frame.Segment
			}
			return nil
		},
	)
	if err != nil {
		return fsperas.WitnessSnapshot{}, err
	}
	l.mu.RLock()
	for _, segment := range l.segments {
		if segment.EpochID == epochID {
			segments[segment.SegmentRoot] = segment
		}
	}
	l.mu.RUnlock()
	out := fsperas.WitnessSnapshot{Segments: make([]fsperas.SegmentWitnessRecord, 0, len(segments))}
	for _, segment := range segments {
		out.Segments = append(out.Segments, segment)
	}
	return out, nil
}

func (l *WALWitnessLog) appendPayload(payload []byte) (wal.EntryInfo, error) {
	if l == nil || l.wal == nil {
		return wal.EntryInfo{}, fsperas.ErrWitnessLogRequired
	}
	infos, err := l.wal.AppendRecords(l.durability, wal.Record{
		Type:    wal.RecordTypePerasWitness,
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

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
