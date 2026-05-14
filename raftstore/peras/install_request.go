// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"

	"github.com/feichai0017/NoKV/fsmeta"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

type InstallRequestInfo struct {
	RoutingKey         []byte
	CanonicalObjectKey []byte
	RoutingKeys        [][]byte
	DependencyKeys     [][]byte
	CatalogKeys        [][]byte
	MaterializedKeys   [][]byte
	Root               [32]byte
	PayloadDigest      [32]byte
	Payload            []byte
	InstallVersion     uint64
	MaterializeMVCC    bool

	SegmentEpochID        uint64
	SegmentOperationCount uint64
	SegmentEntryCount     uint64
	SegmentPayloadSize    uint64

	HasPayload bool
}

func InspectInstallRequest(req *kvrpcpb.PerasInstallSegmentRequest) (InstallRequestInfo, error) {
	if req == nil {
		return InstallRequestInfo{}, ErrInvalidInstallRequest
	}
	var root [32]byte
	if len(req.GetSegmentRoot()) != len(root) {
		return InstallRequestInfo{}, ErrInvalidInstallRequest
	}
	copy(root[:], req.GetSegmentRoot())
	var digest [32]byte
	if len(req.GetSegmentPayloadDigest()) != len(digest) {
		return InstallRequestInfo{}, ErrInvalidInstallRequest
	}
	copy(digest[:], req.GetSegmentPayloadDigest())
	info := InstallRequestInfo{
		RoutingKey:            req.GetRoutingKey(),
		CanonicalObjectKey:    req.GetCanonicalObjectKey(),
		RoutingKeys:           req.GetRoutingKeys(),
		DependencyKeys:        req.GetDependencyKeys(),
		CatalogKeys:           req.GetCatalogKeys(),
		MaterializedKeys:      req.GetMaterializedKeys(),
		Root:                  root,
		PayloadDigest:         digest,
		Payload:               req.GetSegmentPayload(),
		InstallVersion:        req.GetInstallVersion(),
		MaterializeMVCC:       req.GetMaterializeMvcc(),
		SegmentEpochID:        req.GetSegmentEpochId(),
		SegmentOperationCount: req.GetSegmentOperationCount(),
		SegmentEntryCount:     req.GetSegmentEntryCount(),
		SegmentPayloadSize:    req.GetSegmentPayloadSize(),
		HasPayload:            len(req.GetSegmentPayload()) > 0,
	}
	return info, nil
}

func DecodeInstallSegmentPayload(req *kvrpcpb.PerasInstallSegmentRequest) (fsperas.PerasSegment, [32]byte, error) {
	info, err := InspectInstallRequest(req)
	if err != nil {
		return fsperas.PerasSegment{}, [32]byte{}, err
	}
	if !info.HasPayload {
		return fsperas.PerasSegment{}, [32]byte{}, ErrInvalidInstallRequest
	}
	segment, err := fsperas.VerifyPerasSegmentPayload(info.Payload, info.Root, info.PayloadDigest)
	if err != nil {
		return fsperas.PerasSegment{}, [32]byte{}, err
	}
	return segment, info.PayloadDigest, nil
}

func CatalogRouteKeys(req *kvrpcpb.PerasInstallSegmentRequest) ([][]byte, error) {
	info, err := InspectInstallRequest(req)
	if err != nil {
		return nil, err
	}
	return CatalogRouteInstallKeys(info.Root, info.RoutingKey)
}

func CatalogRouteInstallKeys(root [32]byte, routingKey []byte) ([][]byte, error) {
	return fsperas.PerasSegmentCatalogRouteInstallKeys(root, routingKey)
}

func InstallKeys(req *kvrpcpb.PerasInstallSegmentRequest) ([][]byte, error) {
	info, err := InspectInstallRequest(req)
	if err != nil {
		return nil, err
	}
	if len(info.RoutingKey) == 0 {
		return nil, ErrInvalidInstallRequest
	}
	if len(info.DependencyKeys) > 0 {
		return validateInstallDependencyHeader(info)
	}
	if !info.MaterializeMVCC {
		return fsperas.PerasSegmentCatalogRouteInstallKeys(info.Root, info.RoutingKey)
	}
	segment, _, err := DecodeInstallSegmentPayload(req)
	if err != nil {
		return nil, err
	}
	return fsperas.PerasSegmentInstallKeys(segment, info.RoutingKey, true)
}

func WatchKeys(req *kvrpcpb.PerasInstallSegmentRequest) [][]byte {
	if req != nil && req.GetMaterializeMvcc() && len(req.GetMaterializedKeys()) > 0 {
		return dentryKeysFromHeader(req.GetMaterializedKeys())
	}
	segment, _, err := DecodeInstallSegmentPayload(req)
	if err != nil {
		return nil
	}
	dentries := segment.Dentries
	out := make([][]byte, 0, len(dentries))
	for _, entry := range dentries {
		out = append(out, append([]byte(nil), entry.Key...))
	}
	return out
}

func dentryKeysFromHeader(keys [][]byte) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		parts, ok := fsmeta.InspectKey(key)
		if !ok || parts.Kind != fsmeta.KeyKindDentry {
			continue
		}
		out = append(out, append([]byte(nil), key...))
	}
	return out
}

func validateInstallDependencyHeader(info InstallRequestInfo) ([][]byte, error) {
	if len(info.DependencyKeys) == 0 {
		return nil, ErrInvalidInstallRequest
	}
	if err := validateInstallKeys(info.DependencyKeys); err != nil {
		return nil, err
	}
	if !info.MaterializeMVCC {
		routeKeys, err := fsperas.PerasSegmentCatalogRouteInstallKeys(info.Root, info.RoutingKey)
		if err != nil {
			return nil, err
		}
		if !installKeysContainAll(info.DependencyKeys, routeKeys) {
			return nil, ErrInvalidInstallRequest
		}
		if len(info.CatalogKeys) > 0 && !installKeysContainAll(info.DependencyKeys, info.CatalogKeys) {
			return nil, ErrInvalidInstallRequest
		}
		return info.DependencyKeys, nil
	}
	if info.SegmentEntryCount == 0 || len(info.MaterializedKeys) == 0 {
		return nil, ErrInvalidInstallRequest
	}
	if info.SegmentEntryCount != uint64(len(info.MaterializedKeys)) {
		return nil, ErrInvalidInstallRequest
	}
	if err := validateInstallKeys(info.MaterializedKeys); err != nil {
		return nil, err
	}
	if !installKeysHavePrefix(info.DependencyKeys, info.MaterializedKeys) {
		return nil, ErrInvalidInstallRequest
	}
	if len(info.CatalogKeys) > 0 {
		if err := validateInstallKeys(info.CatalogKeys); err != nil {
			return nil, err
		}
		if !installKeysContainAll(info.DependencyKeys[len(info.MaterializedKeys):], info.CatalogKeys) {
			return nil, ErrInvalidInstallRequest
		}
	}
	return info.DependencyKeys, nil
}

func validateInstallKeys(keys [][]byte) error {
	if len(keys) == 0 {
		return ErrInvalidInstallRequest
	}
	for _, key := range keys {
		if len(key) == 0 {
			return ErrInvalidInstallRequest
		}
	}
	return nil
}

func installKeysHavePrefix(have, prefix [][]byte) bool {
	if len(prefix) > len(have) {
		return false
	}
	for i, key := range prefix {
		if !bytes.Equal(have[i], key) {
			return false
		}
	}
	return true
}

func installKeysContainAll(have, want [][]byte) bool {
	if len(want) == 0 {
		return true
	}
	for _, wanted := range want {
		if len(wanted) == 0 {
			return false
		}
		found := false
		for _, key := range have {
			if bytes.Equal(key, wanted) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
