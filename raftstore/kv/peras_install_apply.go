// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv

import (
	"bytes"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	rsperas "github.com/feichai0017/NoKV/experimental/peras/raftstore"
	"github.com/feichai0017/NoKV/fsmeta"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
)

type perasInstallSegmentApplyPlan struct {
	response *kvrpcpb.PerasInstallSegmentResponse
	install  *kvrpcpb.InstallPreparedMVCCEntriesRequest
}

type perasInstallBatchStaging struct {
	catalogIndex map[string][]byte
}

func applyPerasInstallSegmentBatch(db txnstore.Store, reqs []*kvrpcpb.PerasInstallSegmentRequest) ([]*kvrpcpb.PerasInstallSegmentResponse, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	responses := make([]*kvrpcpb.PerasInstallSegmentResponse, len(reqs))
	installs := make([]*kvrpcpb.InstallPreparedMVCCEntriesRequest, 0, len(reqs))
	installIndexes := make([]int, 0, len(reqs))
	staging := &perasInstallBatchStaging{catalogIndex: make(map[string][]byte)}
	for idx, req := range reqs {
		plan, err := planPerasInstallSegment(db, req, staging)
		if err != nil {
			return nil, err
		}
		responses[idx] = plan.response
		if plan.install != nil {
			installs = append(installs, plan.install)
			installIndexes = append(installIndexes, idx)
		}
	}
	if len(installs) > 0 {
		installResponses, err := applyInstallPreparedMVCCEntriesBatch(db, installs)
		if err != nil {
			return nil, err
		}
		if len(installResponses) != len(installs) {
			return nil, rsperas.ErrInvalidInstallRequest
		}
		for idx, installResponse := range installResponses {
			response := responses[installIndexes[idx]]
			if installResponse.GetError() != nil {
				response.Error = installResponse.GetError()
				continue
			}
			response.AppliedEntries = installResponse.GetAppliedEntries()
			response.CommitVersion = installResponse.GetCommitVersion()
		}
	}
	return responses, nil
}

func planPerasInstallSegment(db txnstore.Store, req *kvrpcpb.PerasInstallSegmentRequest, staging *perasInstallBatchStaging) (perasInstallSegmentApplyPlan, error) {
	info, err := rsperas.InspectInstallRequest(req)
	if err != nil {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
	}
	if len(info.RoutingKey) == 0 || info.InstallVersion == 0 {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(rsperas.ErrInvalidInstallRequest.Error())}}, nil
	}
	if !info.MaterializeMVCC && !info.HasPayload {
		return planPerasInstallSegmentIndexRoutes(db, info, staging)
	}
	segment, digest, err := rsperas.DecodeInstallSegmentPayload(req)
	if err != nil {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
	}
	var catalogRoutingKeys [][]byte
	if !info.MaterializeMVCC {
		routingKeys, err := rsperas.CatalogInstallRoutingKeys(info)
		if err != nil {
			return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
		}
		catalogRoutingKeys = routingKeys
		if ok, err := staging.catalogInstallPlanned(segment.Root, routingKeys, info.CanonicalObjectKey); err != nil {
			return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
		} else if ok {
			return perasInstallSegmentApplyPlan{response: perasInstallSegmentAppliedResponse(segment.Root, segment.Stats(), 1)}, nil
		}
		if ok, err := loadPerasSegmentCatalogInstallForObjectKeys(db, segment, routingKeys); err != nil {
			return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
		} else if ok {
			return perasInstallSegmentApplyPlan{response: perasInstallSegmentAppliedResponse(segment.Root, segment.Stats(), 1)}, nil
		}
	}
	var entries []*entrykv.Entry
	if info.MaterializeMVCC {
		entries, err = buildMVCCSegmentInstallEntriesWithVerifiedPayload(segment, info.InstallVersion, info.Payload, digest)
	} else {
		entries, err = buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKeys(segment, info.InstallVersion, info.Payload, digest, catalogRoutingKeys)
	}
	if err != nil {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
	}
	install, keyErr := perasPreparedMVCCInstallRequest(info, entries)
	releaseEntries(entries)
	if keyErr != nil {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: keyErr}}, nil
	}
	if !info.MaterializeMVCC {
		if err := staging.markCatalogInstall(segment.Root, catalogRoutingKeys, info.CanonicalObjectKey); err != nil {
			return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
		}
	}
	return perasInstallSegmentApplyPlan{response: perasInstallSegmentAppliedResponse(segment.Root, segment.Stats(), uint64(len(install.GetEntries()))), install: install}, nil
}

func planPerasInstallSegmentIndexRoutes(db txnstore.Store, info rsperas.InstallRequestInfo, staging *perasInstallBatchStaging) (perasInstallSegmentApplyPlan, error) {
	if info.SegmentEpochID == 0 || info.SegmentOperationCount == 0 || info.SegmentEntryCount == 0 || info.SegmentPayloadSize == 0 || len(info.CanonicalObjectKey) == 0 {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort("missing segment catalog index metadata")}}, nil
	}
	routingKeys, err := rsperas.CatalogInstallRoutingKeys(info)
	if err != nil {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
	}
	for _, routingKey := range routingKeys {
		if bytes.Equal(routingKey, info.CanonicalObjectKey) {
			return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort("canonical segment route requires payload")}}, nil
		}
	}
	if ok, err := staging.catalogInstallPlanned(info.Root, routingKeys, info.CanonicalObjectKey); err != nil {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
	} else if ok {
		return perasInstallSegmentApplyPlan{response: perasInstallHeaderAppliedResponse(info, 1)}, nil
	}
	if ok, err := loadPerasSegmentCatalogIndexInstallForObjectKeys(db, info.Root, routingKeys, info.CanonicalObjectKey); err != nil {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
	} else if ok {
		return perasInstallSegmentApplyPlan{response: perasInstallHeaderAppliedResponse(info, 1)}, nil
	}
	entries, err := buildMVCCSegmentCatalogIndexInstallEntriesForObjectKeys(info.Root, info.PayloadDigest, info.SegmentEpochID, info.InstallVersion, info.SegmentPayloadSize, routingKeys, info.CanonicalObjectKey)
	if err != nil {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
	}
	install, keyErr := perasPreparedMVCCInstallRequest(info, entries)
	releaseEntries(entries)
	if keyErr != nil {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: keyErr}}, nil
	}
	if err := staging.markCatalogInstall(info.Root, routingKeys, info.CanonicalObjectKey); err != nil {
		return perasInstallSegmentApplyPlan{response: &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}}, nil
	}
	return perasInstallSegmentApplyPlan{response: perasInstallHeaderAppliedResponse(info, uint64(len(install.GetEntries()))), install: install}, nil
}

func perasPreparedMVCCInstallRequest(info rsperas.InstallRequestInfo, entries []*entrykv.Entry) (*kvrpcpb.InstallPreparedMVCCEntriesRequest, *kvrpcpb.KeyError) {
	idempotencyKey := make([]byte, 0, len(info.Root)+len(info.RoutingKey))
	idempotencyKey = append(idempotencyKey, info.Root[:]...)
	idempotencyKey = append(idempotencyKey, info.RoutingKey...)
	return buildInstallPreparedMVCCEntriesRequest(
		info.RoutingKey,
		info.InstallVersion,
		entries,
		info.DependencyKeys,
		idempotencyKey,
		"peras_segment_install",
	)
}

func perasInstallSegmentAppliedResponse(root [32]byte, stats fsperas.SegmentStats, applied uint64) *kvrpcpb.PerasInstallSegmentResponse {
	return &kvrpcpb.PerasInstallSegmentResponse{
		SegmentRoot:    append([]byte(nil), root[:]...),
		OperationCount: stats.OperationCount,
		EntryCount:     stats.EntryCount,
		AppliedEntries: applied,
	}
}

func perasInstallHeaderAppliedResponse(info rsperas.InstallRequestInfo, applied uint64) *kvrpcpb.PerasInstallSegmentResponse {
	return &kvrpcpb.PerasInstallSegmentResponse{
		SegmentRoot:    append([]byte(nil), info.Root[:]...),
		OperationCount: info.SegmentOperationCount,
		EntryCount:     info.SegmentEntryCount,
		AppliedEntries: applied,
	}
}

func (s *perasInstallBatchStaging) catalogInstallPlanned(root [32]byte, routingKeys [][]byte, canonicalObjectKey []byte) (bool, error) {
	if s == nil || len(s.catalogIndex) == 0 {
		return false, nil
	}
	for _, routingKey := range routingKeys {
		indexKey, err := perasCatalogInstallIndexKey(root, routingKey)
		if err != nil {
			return false, err
		}
		planned, ok := s.catalogIndex[string(indexKey)]
		if !ok || !bytes.Equal(planned, canonicalObjectKey) {
			return false, nil
		}
	}
	return len(routingKeys) > 0, nil
}

func (s *perasInstallBatchStaging) markCatalogInstall(root [32]byte, routingKeys [][]byte, canonicalObjectKey []byte) error {
	if s == nil {
		return nil
	}
	if s.catalogIndex == nil {
		s.catalogIndex = make(map[string][]byte)
	}
	for _, routingKey := range routingKeys {
		indexKey, err := perasCatalogInstallIndexKey(root, routingKey)
		if err != nil {
			return err
		}
		s.catalogIndex[string(indexKey)] = append([]byte(nil), canonicalObjectKey...)
	}
	return nil
}

func perasCatalogInstallIndexKey(root [32]byte, routingKey []byte) ([]byte, error) {
	route, err := inspectPerasSegmentObjectKey(root, routingKey)
	if err != nil {
		return nil, err
	}
	return fsmeta.EncodePerasSegmentCatalogIndexKey(route.mount, route.bucket, root)
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
