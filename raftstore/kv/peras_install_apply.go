// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv

import (
	"bytes"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
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
		return applyPerasInstallSegmentIndexRoutes(db, info)
	}
	segment, digest, err := rsperas.DecodeInstallSegmentPayload(req)
	if err != nil {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
	}
	var catalogRoutingKeys [][]byte
	if !info.MaterializeMVCC {
		routingKeys, err := rsperas.CatalogInstallRoutingKeys(info)
		if err != nil {
			return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
		}
		catalogRoutingKeys = routingKeys
		if ok, err := loadPerasSegmentCatalogInstallForObjectKeys(db, segment, routingKeys); err != nil {
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
		entries, err = buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKeys(segment, info.InstallVersion, info.Payload, digest, catalogRoutingKeys)
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

func applyPerasInstallSegmentIndexRoutes(db txnstore.Store, info rsperas.InstallRequestInfo) (*kvrpcpb.PerasInstallSegmentResponse, error) {
	if info.SegmentEpochID == 0 || info.SegmentOperationCount == 0 || info.SegmentEntryCount == 0 || info.SegmentPayloadSize == 0 || len(info.CanonicalObjectKey) == 0 {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort("missing segment catalog index metadata")}, nil
	}
	routingKeys, err := rsperas.CatalogInstallRoutingKeys(info)
	if err != nil {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
	}
	for _, routingKey := range routingKeys {
		if bytes.Equal(routingKey, info.CanonicalObjectKey) {
			return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort("canonical segment route requires payload")}, nil
		}
	}
	if ok, err := loadPerasSegmentCatalogIndexInstallForObjectKeys(db, info.Root, routingKeys, info.CanonicalObjectKey); err != nil {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
	} else if ok {
		return &kvrpcpb.PerasInstallSegmentResponse{
			SegmentRoot:    append([]byte(nil), info.Root[:]...),
			OperationCount: info.SegmentOperationCount,
			EntryCount:     info.SegmentEntryCount,
			AppliedEntries: 1,
		}, nil
	}
	entries, err := buildMVCCSegmentCatalogIndexInstallEntriesForObjectKeys(info.Root, info.PayloadDigest, info.SegmentEpochID, info.InstallVersion, info.SegmentPayloadSize, routingKeys, info.CanonicalObjectKey)
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

func loadPerasSegmentCatalogInstallForObjectKeys(store SegmentCatalogStore, segment fsperas.PerasSegment, objectKeys [][]byte) (bool, error) {
	if len(objectKeys) == 0 {
		return false, fsperas.ErrInvalidPerasSegment
	}
	for _, objectKey := range objectKeys {
		ok, err := LoadPerasSegmentCatalogInstallForObjectKey(store, segment, objectKey)
		if err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

func loadPerasSegmentCatalogIndexInstallForObjectKeys(store SegmentCatalogStore, root [32]byte, routingKeys [][]byte, canonicalObjectKey []byte) (bool, error) {
	if len(routingKeys) == 0 {
		return false, fsperas.ErrInvalidPerasSegment
	}
	for _, routingKey := range routingKeys {
		ok, err := LoadPerasSegmentCatalogIndexInstall(store, root, routingKey, canonicalObjectKey)
		if err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}
