// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
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

	ReadFirstKey       []byte
	ReadLastKey        []byte
	ReadDentryCount    uint64
	ReadInodeCount     uint64
	ReadSessionCount   uint64
	ReadTombstoneCount uint64
	ReadDirectoryCount uint64

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
		ReadFirstKey:          req.GetReadFirstKey(),
		ReadLastKey:           req.GetReadLastKey(),
		ReadDentryCount:       req.GetReadDentryCount(),
		ReadInodeCount:        req.GetReadInodeCount(),
		ReadSessionCount:      req.GetReadSessionCount(),
		ReadTombstoneCount:    req.GetReadTombstoneCount(),
		ReadDirectoryCount:    req.GetReadDirectoryCount(),
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
	if err := ValidateInstallReadHeader(info, segment); err != nil {
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

func CatalogInstallRoutingKeys(info InstallRequestInfo) ([][]byte, error) {
	if info.MaterializeMVCC || len(info.RoutingKey) == 0 {
		return nil, ErrInvalidInstallRequest
	}
	source := info.RoutingKeys
	if len(source) == 0 {
		source = [][]byte{info.RoutingKey}
	}
	if !installKeysContainAll(source, [][]byte{info.RoutingKey}) {
		return nil, ErrInvalidInstallRequest
	}
	out := make([][]byte, 0, len(source))
	for _, key := range source {
		if _, err := fsperas.PerasSegmentCatalogRouteInstallKeys(info.Root, key); err != nil {
			return nil, err
		}
		out = appendUniqueInstallKey(out, key)
	}
	if len(out) == 0 {
		return nil, ErrInvalidInstallRequest
	}
	return out, nil
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
	if err := ValidateInstallReadHeaderShape(info); err != nil {
		return nil, err
	}
	if len(info.DependencyKeys) > 0 {
		return validateInstallDependencyHeader(info)
	}
	if !info.MaterializeMVCC {
		routingKeys, err := CatalogInstallRoutingKeys(info)
		if err != nil {
			return nil, err
		}
		keys := make([][]byte, 0, len(routingKeys)*2)
		for _, routingKey := range routingKeys {
			routeKeys, err := fsperas.PerasSegmentCatalogRouteInstallKeys(info.Root, routingKey)
			if err != nil {
				return nil, err
			}
			keys = appendInstallKeys(keys, routeKeys...)
		}
		return keys, nil
	}
	segment, _, err := DecodeInstallSegmentPayload(req)
	if err != nil {
		return nil, err
	}
	return fsperas.PerasSegmentInstallKeys(segment, info.RoutingKey, true)
}

func ValidateInstallReadHeader(info InstallRequestInfo, segment fsperas.PerasSegment) error {
	if err := ValidateInstallReadHeaderShape(info); err != nil {
		return err
	}
	stats := segment.Stats()
	if info.SegmentEpochID != segment.EpochID ||
		info.SegmentOperationCount != stats.OperationCount ||
		info.SegmentEntryCount != stats.EntryCount ||
		info.SegmentPayloadSize != uint64(len(info.Payload)) {
		return ErrInvalidInstallRequest
	}
	header := segment.ReadHeaderView()
	if header.EntryCount != info.SegmentEntryCount ||
		header.DentryCount != info.ReadDentryCount ||
		header.InodeCount != info.ReadInodeCount ||
		header.SessionCount != info.ReadSessionCount ||
		header.TombstoneCount != info.ReadTombstoneCount ||
		header.DirectoryCount != info.ReadDirectoryCount ||
		!bytes.Equal(header.FirstKey, info.ReadFirstKey) ||
		!bytes.Equal(header.LastKey, info.ReadLastKey) {
		return ErrInvalidInstallRequest
	}
	return nil
}

func ValidateInstallReadHeaderShape(info InstallRequestInfo) error {
	if info.SegmentEpochID == 0 || info.SegmentOperationCount == 0 || info.SegmentEntryCount == 0 || info.SegmentPayloadSize == 0 {
		return ErrInvalidInstallRequest
	}
	if len(info.ReadFirstKey) == 0 || len(info.ReadLastKey) == 0 || bytes.Compare(info.ReadFirstKey, info.ReadLastKey) > 0 {
		return ErrInvalidInstallRequest
	}
	indexed := info.ReadDentryCount + info.ReadInodeCount + info.ReadSessionCount
	if indexed > info.SegmentEntryCount || info.ReadTombstoneCount > info.SegmentEntryCount || info.ReadDirectoryCount > info.ReadDentryCount {
		return ErrInvalidInstallRequest
	}
	return nil
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
		routingKeys, err := CatalogInstallRoutingKeys(info)
		if err != nil {
			return nil, err
		}
		for _, routingKey := range routingKeys {
			routeKeys, err := fsperas.PerasSegmentCatalogRouteInstallKeys(info.Root, routingKey)
			if err != nil {
				return nil, err
			}
			if !installKeysContainAll(info.DependencyKeys, routeKeys) {
				return nil, ErrInvalidInstallRequest
			}
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

func appendInstallKeys(dst [][]byte, keys ...[]byte) [][]byte {
	for _, key := range keys {
		dst = appendUniqueInstallKey(dst, key)
	}
	return dst
}

func appendUniqueInstallKey(dst [][]byte, key []byte) [][]byte {
	if len(key) == 0 {
		return dst
	}
	for _, existing := range dst {
		if bytes.Equal(existing, key) {
			return dst
		}
	}
	return append(dst, append([]byte(nil), key...))
}
