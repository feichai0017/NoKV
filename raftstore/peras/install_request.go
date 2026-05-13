package peras

import (
	"errors"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

var ErrInvalidInstallRequest = errors.New("raftstore/peras: invalid install request")

type InstallRequestInfo struct {
	RoutingKey         []byte
	CanonicalObjectKey []byte
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
