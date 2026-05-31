// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"

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
	return p.publish(ctx, rootevent.SnapshotEpochPublishedWithRuntimeEvidence(
		string(token.Mount),
		uint64(token.MountKeyID),
		uint64(token.RootInode),
		token.ReadVersion,
		snapshotEvidenceRefsToRoot(token.RuntimeEvidence),
	))
}

func (p *SnapshotPublisher) RetireSnapshotSubtree(ctx context.Context, token model.SnapshotSubtreeToken) error {
	if p == nil || p.coordinator == nil {
		return errCoordinatorRequired
	}
	if err := model.ValidateSnapshotValue(token); err != nil {
		return err
	}
	return p.publish(ctx, rootevent.SnapshotEpochRetired(
		string(token.Mount),
		uint64(token.MountKeyID),
		uint64(token.RootInode),
		token.ReadVersion,
	))
}

func (p *SnapshotPublisher) publish(ctx context.Context, event rootevent.Event) error {
	resp, err := p.coordinator.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	if err != nil {
		return err
	}
	if !resp.GetAccepted() {
		return nokverrors.New(nokverrors.KindAborted, "fsmeta/runtime/raftstore: root rejected snapshot epoch event")
	}
	return nil
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
