// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"sync/atomic"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

// SnapshotPublisher records distributed fsmeta snapshot epochs into rooted
// truth through the coordinator. The data plane keeps MVCC versions; root owns
// the cross-gateway retention claim.
type SnapshotPublisher struct {
	coordinator CoordinatorClient

	publishTotal   atomic.Uint64
	publishErrors  atomic.Uint64
	publishLatency atomic.Uint64
	retireTotal    atomic.Uint64
	retireErrors   atomic.Uint64
	retireLatency  atomic.Uint64
	rootRejected   atomic.Uint64
}

func NewSnapshotPublisher(coordinator CoordinatorClient) (*SnapshotPublisher, error) {
	if coordinator == nil {
		return nil, errCoordinatorRequired
	}
	return &SnapshotPublisher{coordinator: coordinator}, nil
}

func (p *SnapshotPublisher) PublishSnapshotSubtree(ctx context.Context, token model.SnapshotSubtreeToken) error {
	if p == nil || p.coordinator == nil {
		return errCoordinatorRequired
	}
	if err := model.ValidateSnapshotValue(token); err != nil {
		return err
	}
	started := time.Now()
	err := p.publish(ctx, rootevent.SnapshotEpochPublishedWithRuntimeEvidence(
		string(token.Mount),
		uint64(token.MountKeyID),
		uint64(token.RootInode),
		token.ReadVersion,
		snapshotEvidenceRefsToRoot(token.RuntimeEvidence),
	))
	p.publishTotal.Add(1)
	p.publishLatency.Add(uint64(time.Since(started)))
	if err != nil {
		p.publishErrors.Add(1)
	}
	return err
}

func (p *SnapshotPublisher) RetireSnapshotSubtree(ctx context.Context, token model.SnapshotSubtreeToken) error {
	if p == nil || p.coordinator == nil {
		return errCoordinatorRequired
	}
	if err := model.ValidateSnapshotValue(token); err != nil {
		return err
	}
	started := time.Now()
	err := p.publish(ctx, rootevent.SnapshotEpochRetired(
		string(token.Mount),
		uint64(token.MountKeyID),
		uint64(token.RootInode),
		token.ReadVersion,
	))
	p.retireTotal.Add(1)
	p.retireLatency.Add(uint64(time.Since(started)))
	if err != nil {
		p.retireErrors.Add(1)
	}
	return err
}

// Stats returns distributed snapshot retention diagnostics.
func (p *SnapshotPublisher) Stats() map[string]any {
	if p == nil {
		return map[string]any{
			"publish_total":        uint64(0),
			"publish_error_total":  uint64(0),
			"retire_total":         uint64(0),
			"retire_error_total":   uint64(0),
			"root_rejected_total":  uint64(0),
			"durability_authority": "none",
		}
	}
	publishTotal := p.publishTotal.Load()
	retireTotal := p.retireTotal.Load()
	return map[string]any{
		"publish_total":                      publishTotal,
		"publish_error_total":                p.publishErrors.Load(),
		"publish_latency_total_nanosecond":   p.publishLatency.Load(),
		"publish_latency_average_nanosecond": averageUint64(p.publishLatency.Load(), publishTotal),
		"retire_total":                       retireTotal,
		"retire_error_total":                 p.retireErrors.Load(),
		"retire_latency_total_nanosecond":    p.retireLatency.Load(),
		"retire_latency_average_nanosecond":  averageUint64(p.retireLatency.Load(), retireTotal),
		"root_rejected_total":                p.rootRejected.Load(),
		"persistent":                         true,
		"durability_authority":               "root_snapshot_epoch",
	}
}

func (p *SnapshotPublisher) publish(ctx context.Context, event rootevent.Event) error {
	resp, err := p.coordinator.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	if err != nil {
		return err
	}
	if !resp.GetAccepted() {
		p.rootRejected.Add(1)
		return nokverrors.New(nokverrors.KindAborted, "fsmeta/runtime/raftstore: root rejected snapshot epoch event")
	}
	return nil
}

func averageUint64(total, count uint64) uint64 {
	if count == 0 {
		return 0
	}
	return total / count
}

func snapshotEvidenceRefsToRoot(refs []model.SnapshotEvidenceRef) []rootproto.SnapshotEvidenceRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]rootproto.SnapshotEvidenceRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, rootproto.SnapshotEvidenceRef{
			EpochID:       ref.EpochID,
			EvidenceRoot:  ref.EvidenceRoot,
			PayloadDigest: ref.PayloadDigest,
		})
	}
	return out
}
