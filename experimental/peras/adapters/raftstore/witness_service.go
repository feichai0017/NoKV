// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	segmentWitnessProbeMaxPageLimit      = 32
	segmentWitnessProbeMaxPayloadBytes   = 32 << 20
	segmentWitnessProbeRecordFixedBytes  = 32 + 32 + 32 + 8 + 8 + 8 + 8
	segmentWitnessProbeRecordStringBytes = 4
)

type WitnessService struct {
	kvrpcpb.UnimplementedSegmentWitnessServer
	witness Witness
}

type Witness interface {
	AppendSegments(context.Context, compile.AuthorityScope, []fsperas.SegmentWitnessRecord) error
	Probe(context.Context, uint64) (fsperas.WitnessSnapshot, error)
}

type witnessStats interface {
	Stats() map[string]any
}

func NewWitnessService(witness Witness) *WitnessService {
	return &WitnessService{witness: witness}
}

func (s *WitnessService) Stats() map[string]any {
	if s == nil || s.witness == nil {
		return map[string]any{}
	}
	reporter, ok := s.witness.(witnessStats)
	if !ok {
		return map[string]any{}
	}
	return reporter.Stats()
}

func (s *WitnessService) AppendSegmentWitness(ctx context.Context, req *kvrpcpb.AppendSegmentWitnessRequest) (*kvrpcpb.AppendSegmentWitnessResponse, error) {
	if s == nil || s.witness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: peras witness is not configured")
	}
	scope, err := ScopeFromProto(req.GetScope())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	records, err := SegmentWitnessRecordsFromProto(req.GetRecords())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if len(records) == 0 {
		return nil, rpcInvalidArgument("peras witness batch requires at least one record")
	}
	if err := s.witness.AppendSegments(ctx, scope, records); err != nil {
		return nil, rpcSegmentWitnessStatus(err)
	}
	return &kvrpcpb.AppendSegmentWitnessResponse{}, nil
}

func (s *WitnessService) ProbeSegmentWitness(ctx context.Context, req *kvrpcpb.ProbeSegmentWitnessRequest) (*kvrpcpb.ProbeSegmentWitnessResponse, error) {
	if s == nil || s.witness == nil {
		return nil, rpcProtocolPrecondition("raftstore/kv: peras witness is not configured")
	}
	if req.GetEpochId() == 0 {
		return nil, rpcInvalidArgument("peras witness probe requires epoch_id")
	}
	ref, targeted, err := segmentWitnessProbeTarget(req)
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if targeted {
		return s.probeSegmentWitnessSegment(ctx, ref)
	}
	snapshot, err := s.witness.Probe(ctx, req.GetEpochId())
	if err != nil {
		return nil, rpcSegmentWitnessStatus(err)
	}
	return segmentWitnessProbePage(snapshot, req)
}

func (s *WitnessService) probeSegmentWitnessSegment(ctx context.Context, ref fsperas.WitnessSegmentRef) (*kvrpcpb.ProbeSegmentWitnessResponse, error) {
	if prober, ok := s.witness.(fsperas.WitnessSegmentProber); ok {
		record, found, err := prober.ProbeSegment(ctx, ref)
		if err != nil {
			return nil, rpcSegmentWitnessStatus(err)
		}
		if !found {
			return &kvrpcpb.ProbeSegmentWitnessResponse{}, nil
		}
		return &kvrpcpb.ProbeSegmentWitnessResponse{
			Segments: []*kvrpcpb.SegmentWitnessRecord{SegmentWitnessRecordToProto(record)},
		}, nil
	}
	snapshot, err := s.witness.Probe(ctx, ref.EpochID)
	if err != nil {
		return nil, rpcSegmentWitnessStatus(err)
	}
	for _, record := range snapshot.Segments {
		if record.EpochID == ref.EpochID && record.SegmentRoot == ref.SegmentRoot && record.SegmentPayloadDigest == ref.SegmentPayloadDigest {
			return &kvrpcpb.ProbeSegmentWitnessResponse{
				Segments: []*kvrpcpb.SegmentWitnessRecord{SegmentWitnessRecordToProto(record)},
			}, nil
		}
	}
	return &kvrpcpb.ProbeSegmentWitnessResponse{}, nil
}

func segmentWitnessProbeTarget(req *kvrpcpb.ProbeSegmentWitnessRequest) (fsperas.WitnessSegmentRef, bool, error) {
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

func segmentWitnessProbePage(snapshot fsperas.WitnessSnapshot, req *kvrpcpb.ProbeSegmentWitnessRequest) (*kvrpcpb.ProbeSegmentWitnessResponse, error) {
	cursor, hasCursor, err := segmentWitnessProbeCursor(req)
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	limit := int(req.GetLimit())
	if limit <= 0 || limit > segmentWitnessProbeMaxPageLimit {
		limit = segmentWitnessProbeMaxPageLimit
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
	resp := &kvrpcpb.ProbeSegmentWitnessResponse{}
	payloadBytes := 0
	end := start
	for end < len(records) {
		record := records[end]
		recordBytes := witnessProbeRecordBytes(record)
		if len(resp.Segments) > 0 && (len(resp.Segments) >= limit || payloadBytes+recordBytes > segmentWitnessProbeMaxPayloadBytes) {
			break
		}
		resp.Segments = append(resp.Segments, SegmentWitnessRecordToProto(record))
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

func segmentWitnessProbeCursor(req *kvrpcpb.ProbeSegmentWitnessRequest) (fsperas.WitnessSegmentRef, bool, error) {
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
	return segmentWitnessProbeRecordFixedBytes +
		segmentWitnessProbeRecordStringBytes + len(record.SegmentPointer) +
		segmentWitnessProbeRecordStringBytes + len(record.HolderID) +
		len(record.SegmentPayload)
}

func rpcSegmentWitnessStatus(err error) error {
	switch {
	case errors.Is(err, ErrWitnessNodeConfigInvalid),
		errors.Is(err, ErrWitnessAuthorityMissing),
		errors.Is(err, ErrWitnessAuthorityMismatch):
		return rpcProtocolPrecondition(err.Error())
	default:
		return rpcStatus(err)
	}
}

func rpcInvalidArgument(message string) error {
	return status.Error(codes.InvalidArgument, message)
}

func rpcProtocolPrecondition(message string) error {
	return status.Error(codes.FailedPrecondition, message)
}

func rpcStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}
