// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	rsperas "github.com/feichai0017/NoKV/raftstore/peras"
)

const (
	perasWitnessProbeMaxPageLimit      = 32
	perasWitnessProbeMaxPayloadBytes   = 32 << 20
	perasWitnessProbeRecordFixedBytes  = 32 + 32 + 32 + 8 + 8 + 8 + 8
	perasWitnessProbeRecordStringBytes = 4
)

func (s *Service) PerasWitnessSegment(ctx context.Context, req *kvrpcpb.PerasWitnessSegmentRequest) (*kvrpcpb.PerasWitnessSegmentResponse, error) {
	if s == nil || s.perasWitness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: peras witness is not configured")
	}
	scope, err := rsperas.ScopeFromProto(req.GetScope())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	record, err := rsperas.SegmentWitnessRecordFromProto(req.GetRecord())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if err := s.perasWitness.AppendSegment(ctx, scope, record); err != nil {
		return nil, rpcPerasWitnessStatus(err)
	}
	return &kvrpcpb.PerasWitnessSegmentResponse{}, nil
}

func (s *Service) PerasWitnessProbe(ctx context.Context, req *kvrpcpb.PerasWitnessProbeRequest) (*kvrpcpb.PerasWitnessProbeResponse, error) {
	if s == nil || s.perasWitness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: peras witness is not configured")
	}
	if req.GetEpochId() == 0 {
		return nil, rpcInvalidArgument("peras witness probe requires epoch_id")
	}
	ref, targeted, err := perasWitnessProbeTarget(req)
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if targeted {
		return s.probePerasWitnessSegment(ctx, ref)
	}
	snapshot, err := s.perasWitness.Probe(ctx, req.GetEpochId())
	if err != nil {
		return nil, rpcPerasWitnessStatus(err)
	}
	return perasWitnessProbePage(snapshot, req)
}

func (s *Service) probePerasWitnessSegment(ctx context.Context, ref fsperas.WitnessSegmentRef) (*kvrpcpb.PerasWitnessProbeResponse, error) {
	if prober, ok := s.perasWitness.(fsperas.WitnessSegmentProber); ok {
		record, found, err := prober.ProbeSegment(ctx, ref)
		if err != nil {
			return nil, rpcPerasWitnessStatus(err)
		}
		if !found {
			return &kvrpcpb.PerasWitnessProbeResponse{}, nil
		}
		return &kvrpcpb.PerasWitnessProbeResponse{
			Segments: []*kvrpcpb.PerasSegmentWitnessRecord{rsperas.SegmentWitnessRecordToProto(record)},
		}, nil
	}
	snapshot, err := s.perasWitness.Probe(ctx, ref.EpochID)
	if err != nil {
		return nil, rpcPerasWitnessStatus(err)
	}
	for _, record := range snapshot.Segments {
		if record.EpochID == ref.EpochID && record.SegmentRoot == ref.SegmentRoot && record.SegmentPayloadDigest == ref.SegmentPayloadDigest {
			return &kvrpcpb.PerasWitnessProbeResponse{
				Segments: []*kvrpcpb.PerasSegmentWitnessRecord{rsperas.SegmentWitnessRecordToProto(record)},
			}, nil
		}
	}
	return &kvrpcpb.PerasWitnessProbeResponse{}, nil
}

func perasWitnessProbeTarget(req *kvrpcpb.PerasWitnessProbeRequest) (fsperas.WitnessSegmentRef, bool, error) {
	root := req.GetSegmentRoot()
	digest := req.GetSegmentPayloadDigest()
	if len(root) == 0 && len(digest) == 0 {
		return fsperas.WitnessSegmentRef{}, false, nil
	}
	if len(root) == 0 || len(digest) == 0 {
		return fsperas.WitnessSegmentRef{}, false, fmt.Errorf("peras witness probe target requires both segment_root and segment_payload_digest")
	}
	ref := fsperas.WitnessSegmentRef{EpochID: req.GetEpochId()}
	if err := copyWitnessProbeDigest(ref.SegmentRoot[:], root, "segment_root"); err != nil {
		return fsperas.WitnessSegmentRef{}, false, err
	}
	if err := copyWitnessProbeDigest(ref.SegmentPayloadDigest[:], digest, "segment_payload_digest"); err != nil {
		return fsperas.WitnessSegmentRef{}, false, err
	}
	if !ref.Valid() {
		return fsperas.WitnessSegmentRef{}, false, fsperas.ErrInvalidWitnessRecord
	}
	return ref, true, nil
}

func perasWitnessProbePage(snapshot fsperas.WitnessSnapshot, req *kvrpcpb.PerasWitnessProbeRequest) (*kvrpcpb.PerasWitnessProbeResponse, error) {
	cursor, hasCursor, err := perasWitnessProbeCursor(req)
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	limit := int(req.GetLimit())
	if limit <= 0 || limit > perasWitnessProbeMaxPageLimit {
		limit = perasWitnessProbeMaxPageLimit
	}
	records := make([]fsperas.SegmentWitnessRecord, 0, len(snapshot.Segments))
	for _, record := range snapshot.Segments {
		if record.EpochID == req.GetEpochId() {
			records = append(records, record)
		}
	}
	slices.SortFunc(records, compareWitnessProbeRecords)
	start := 0
	if hasCursor {
		start = len(records)
		for i, record := range records {
			if compareWitnessProbeRecordRef(record, cursor) > 0 {
				start = i
				break
			}
		}
	}
	resp := &kvrpcpb.PerasWitnessProbeResponse{}
	payloadBytes := 0
	end := start
	for end < len(records) {
		record := records[end]
		recordBytes := witnessProbeRecordBytes(record)
		if len(resp.Segments) > 0 && (len(resp.Segments) >= limit || payloadBytes+recordBytes > perasWitnessProbeMaxPayloadBytes) {
			break
		}
		resp.Segments = append(resp.Segments, rsperas.SegmentWitnessRecordToProto(record))
		payloadBytes += recordBytes
		end++
		if len(resp.Segments) >= limit {
			break
		}
	}
	resp.More = end < len(records)
	if resp.More && len(resp.Segments) > 0 {
		last := records[end-1]
		resp.NextSegmentRoot = append([]byte(nil), last.SegmentRoot[:]...)
		resp.NextSegmentPayloadDigest = append([]byte(nil), last.SegmentPayloadDigest[:]...)
	}
	return resp, nil
}

func perasWitnessProbeCursor(req *kvrpcpb.PerasWitnessProbeRequest) (fsperas.WitnessSegmentRef, bool, error) {
	root := req.GetAfterSegmentRoot()
	digest := req.GetAfterSegmentPayloadDigest()
	if len(root) == 0 && len(digest) == 0 {
		return fsperas.WitnessSegmentRef{}, false, nil
	}
	if len(root) == 0 || len(digest) == 0 {
		return fsperas.WitnessSegmentRef{}, false, fmt.Errorf("peras witness probe cursor requires both after_segment_root and after_segment_payload_digest")
	}
	ref := fsperas.WitnessSegmentRef{EpochID: req.GetEpochId()}
	if err := copyWitnessProbeDigest(ref.SegmentRoot[:], root, "after_segment_root"); err != nil {
		return fsperas.WitnessSegmentRef{}, false, err
	}
	if err := copyWitnessProbeDigest(ref.SegmentPayloadDigest[:], digest, "after_segment_payload_digest"); err != nil {
		return fsperas.WitnessSegmentRef{}, false, err
	}
	if !ref.Valid() {
		return fsperas.WitnessSegmentRef{}, false, fsperas.ErrInvalidWitnessRecord
	}
	return ref, true, nil
}

func copyWitnessProbeDigest(dst []byte, src []byte, field string) error {
	if len(src) != len(dst) {
		return fmt.Errorf("peras witness probe %s length %d != %d", field, len(src), len(dst))
	}
	copy(dst, src)
	return nil
}

func compareWitnessProbeRecords(a, b fsperas.SegmentWitnessRecord) int {
	if cmp := bytes.Compare(a.SegmentRoot[:], b.SegmentRoot[:]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(a.SegmentPayloadDigest[:], b.SegmentPayloadDigest[:])
}

func compareWitnessProbeRecordRef(record fsperas.SegmentWitnessRecord, ref fsperas.WitnessSegmentRef) int {
	if cmp := bytes.Compare(record.SegmentRoot[:], ref.SegmentRoot[:]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(record.SegmentPayloadDigest[:], ref.SegmentPayloadDigest[:])
}

func witnessProbeRecordBytes(record fsperas.SegmentWitnessRecord) int {
	return perasWitnessProbeRecordFixedBytes +
		perasWitnessProbeRecordStringBytes + len(record.SegmentPointer) +
		perasWitnessProbeRecordStringBytes + len(record.HolderID) +
		len(record.SegmentPayload)
}

func rpcPerasWitnessStatus(err error) error {
	switch {
	case errors.Is(err, rsperas.ErrWitnessNodeConfigInvalid),
		errors.Is(err, rsperas.ErrWitnessAuthorityMissing),
		errors.Is(err, rsperas.ErrWitnessAuthorityMismatch):
		return rpcProtocolPrecondition(err.Error())
	default:
		return rpcStatus(err)
	}
}
