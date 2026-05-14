// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv

import (
	"bytes"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	rsperas "github.com/feichai0017/NoKV/raftstore/peras"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
)

func applyPerasInstallSegment(db txnstore.Store, req *kvrpcpb.PerasInstallSegmentRequest) (*kvrpcpb.PerasInstallSegmentResponse, error) {
	info, err := rsperas.InspectInstallRequest(req)
	if err != nil {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
	}
	if len(info.RoutingKey) == 0 || info.InstallVersion == 0 {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(rsperas.ErrInvalidInstallRequest.Error())}, nil
	}
	if !info.MaterializeMVCC && !info.HasPayload {
		return applyPerasInstallSegmentIndexRoute(db, info)
	}
	segment, digest, err := rsperas.DecodeInstallSegmentPayload(req)
	if err != nil {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
	}
	if !info.MaterializeMVCC {
		if ok, err := LoadPerasSegmentCatalogInstallForObjectKey(db, segment, info.RoutingKey); err != nil {
			return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
		} else if ok {
			stats := segment.Stats()
			return &kvrpcpb.PerasInstallSegmentResponse{
				SegmentRoot:    append([]byte(nil), segment.Root[:]...),
				OperationCount: stats.OperationCount,
				EntryCount:     stats.EntryCount,
				AppliedEntries: 1,
			}, nil
		}
	}
	var entries []*entrykv.Entry
	if info.MaterializeMVCC {
		entries, err = buildMVCCSegmentInstallEntriesWithVerifiedPayload(segment, info.InstallVersion, info.Payload, digest)
	} else {
		entries, err = buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKey(segment, info.InstallVersion, info.Payload, digest, info.RoutingKey)
	}
	if err != nil {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
	}
	defer releaseEntries(entries)
	if len(entries) > 0 {
		if err := db.ApplyInternalEntries(entries); err != nil {
			return nil, err
		}
	}
	stats := segment.Stats()
	return &kvrpcpb.PerasInstallSegmentResponse{
		SegmentRoot:    append([]byte(nil), segment.Root[:]...),
		OperationCount: stats.OperationCount,
		EntryCount:     stats.EntryCount,
		AppliedEntries: uint64(len(entries)),
	}, nil
}

func applyPerasInstallSegmentIndexRoute(db txnstore.Store, info rsperas.InstallRequestInfo) (*kvrpcpb.PerasInstallSegmentResponse, error) {
	if info.SegmentEpochID == 0 || info.SegmentOperationCount == 0 || info.SegmentEntryCount == 0 || info.SegmentPayloadSize == 0 || len(info.CanonicalObjectKey) == 0 {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort("missing segment catalog index metadata")}, nil
	}
	if bytes.Equal(info.RoutingKey, info.CanonicalObjectKey) {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort("canonical segment route requires payload")}, nil
	}
	if ok, err := LoadPerasSegmentCatalogIndexInstall(db, info.Root, info.RoutingKey, info.CanonicalObjectKey); err != nil {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
	} else if ok {
		return &kvrpcpb.PerasInstallSegmentResponse{
			SegmentRoot:    append([]byte(nil), info.Root[:]...),
			OperationCount: info.SegmentOperationCount,
			EntryCount:     info.SegmentEntryCount,
			AppliedEntries: 1,
		}, nil
	}
	entries, err := buildMVCCSegmentCatalogIndexInstallEntries(
		info.Root,
		info.PayloadDigest,
		info.SegmentEpochID,
		info.InstallVersion,
		info.SegmentPayloadSize,
		info.RoutingKey,
		info.CanonicalObjectKey,
	)
	if err != nil {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
	}
	defer releaseEntries(entries)
	if err := db.ApplyInternalEntries(entries); err != nil {
		return nil, err
	}
	return &kvrpcpb.PerasInstallSegmentResponse{
		SegmentRoot:    append([]byte(nil), info.Root[:]...),
		OperationCount: info.SegmentOperationCount,
		EntryCount:     info.SegmentEntryCount,
		AppliedEntries: uint64(len(entries)),
	}, nil
}

func perasInstallAbort(msg string) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{Abort: msg}
}
