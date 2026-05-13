package raftstore

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/utils"
)

type perasSegmentInstallClient interface {
	InstallPerasSegment(context.Context, []byte, *kvrpcpb.PerasInstallSegmentRequest) (*kvrpcpb.PerasInstallSegmentResponse, error)
}

type raftstoreSegmentInstaller struct {
	runner             *Runner
	router             *fsmetawatch.Router
	nextInstallVersion atomic.Uint64
}

func newRaftstoreSegmentInstaller(runner *Runner, router *fsmetawatch.Router) *raftstoreSegmentInstaller {
	return &raftstoreSegmentInstaller{runner: runner, router: router}
}

func raftstoreSegmentInstallParallelism() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		return 1
	}
	return n
}

func (i *raftstoreSegmentInstaller) InstallSegment(ctx context.Context, req runtimeperas.SegmentInstallRequest) (runtimeperas.InstallCursor, error) {
	if i == nil || i.runner == nil || i.runner.kv == nil {
		return runtimeperas.InstallCursor{}, runtimeperas.ErrRuntimeInvalid
	}
	kv, ok := i.runner.kv.(perasSegmentInstallClient)
	if !ok {
		return runtimeperas.InstallCursor{}, runtimeperas.ErrRuntimeInvalid
	}
	installVersion, err := i.reserveInstallVersion(ctx)
	if err != nil {
		return runtimeperas.InstallCursor{}, err
	}
	routingKeys, err := runtimeperas.SegmentInstallRoutingKeys(req.Segment, req.MaterializeMVCC)
	if err != nil {
		return runtimeperas.InstallCursor{}, err
	}
	if len(routingKeys) == 0 {
		return runtimeperas.InstallCursor{}, runtimeperas.ErrRuntimeInvalid
	}
	var canonicalObjectKey []byte
	if !req.MaterializeMVCC {
		canonicalObjectKey, err = fsperas.PerasSegmentObjectKey(req.Segment)
		if err != nil {
			return runtimeperas.InstallCursor{}, err
		}
	}
	results := make([]perasRouteInstallResult, len(routingKeys))
	parallelism := len(routingKeys)
	if limit := raftstoreSegmentInstallParallelism(); limit > 0 && parallelism > limit {
		parallelism = limit
	}
	throttle := utils.NewThrottle(parallelism)
	for idx, routingKey := range routingKeys {
		idx := idx
		routingKey := routingKey
		err := throttle.Go(func() error {
			result, err := i.installSegmentRoute(ctx, kv, routingKey, canonicalObjectKey, req.Segment, req.Payload, req.PayloadDigest, installVersion, req.MaterializeMVCC)
			if err != nil {
				return fmt.Errorf("peras install segment route %d: %w", idx, err)
			}
			results[idx] = result
			return nil
		})
		if err != nil {
			return runtimeperas.InstallCursor{}, err
		}
	}
	if err := throttle.Finish(); err != nil {
		return runtimeperas.InstallCursor{}, err
	}
	result := choosePerasInstallResult(routingKeys, results, canonicalObjectKey, req.MaterializeMVCC)
	if !result.cursor.Valid() {
		return runtimeperas.InstallCursor{}, runtimeperas.ErrRuntimeInvalid
	}
	if !req.MaterializeMVCC && result.resp != nil {
		i.publishInstalledSegment(req.Segment, result.resp)
	}
	return result.cursor, nil
}

type perasRouteInstallResult struct {
	cursor runtimeperas.InstallCursor
	resp   *kvrpcpb.PerasInstallSegmentResponse
}

func choosePerasInstallResult(routingKeys [][]byte, results []perasRouteInstallResult, canonicalObjectKey []byte, materialize bool) perasRouteInstallResult {
	if !materialize && len(canonicalObjectKey) > 0 {
		for idx := range results {
			if bytes.Equal(routingKeys[idx], canonicalObjectKey) && results[idx].cursor.Valid() {
				return results[idx]
			}
		}
	}
	for idx := range results {
		if results[idx].cursor.Valid() {
			return results[idx]
		}
	}
	return perasRouteInstallResult{}
}

func (i *raftstoreSegmentInstaller) installSegmentRoute(
	ctx context.Context,
	kv perasSegmentInstallClient,
	routingKey []byte,
	canonicalObjectKey []byte,
	segment fsperas.PerasSegment,
	payload []byte,
	digest [32]byte,
	installVersion uint64,
	materialize bool,
) (perasRouteInstallResult, error) {
	routePayload := payload
	if !materialize && len(canonicalObjectKey) > 0 && !bytes.Equal(routingKey, canonicalObjectKey) {
		routePayload = nil
	}
	stats := segment.Stats()
	resp, err := kv.InstallPerasSegment(ctx, routingKey, &kvrpcpb.PerasInstallSegmentRequest{
		RoutingKey:            runtimeCloneBytes(routingKey),
		SegmentRoot:           append([]byte(nil), segment.Root[:]...),
		SegmentPayloadDigest:  append([]byte(nil), digest[:]...),
		SegmentPayload:        routePayload,
		InstallVersion:        installVersion,
		MaterializeMvcc:       materialize,
		SegmentEpochId:        segment.EpochID,
		SegmentOperationCount: stats.OperationCount,
		SegmentEntryCount:     stats.EntryCount,
		SegmentPayloadSize:    uint64(len(payload)),
		CanonicalObjectKey:    runtimeCloneBytes(canonicalObjectKey),
	})
	if err != nil {
		return perasRouteInstallResult{}, err
	}
	if resp == nil {
		return perasRouteInstallResult{}, runtimeperas.ErrRuntimeInvalid
	}
	if keyErr := resp.GetError(); keyErr != nil {
		return perasRouteInstallResult{}, runnerKeyError("peras install segment", keyErr)
	}
	if err := validatePerasSegmentInstallResponse(segment, resp); err != nil {
		return perasRouteInstallResult{}, err
	}
	cursor := perasInstallCursorFromResponse(resp)
	if !cursor.Valid() {
		return perasRouteInstallResult{}, runtimeperas.ErrRuntimeInvalid
	}
	return perasRouteInstallResult{cursor: cursor, resp: resp}, nil
}

func (i *raftstoreSegmentInstaller) reserveInstallVersion(ctx context.Context) (uint64, error) {
	if i == nil {
		return 0, runtimeperas.ErrRuntimeInvalid
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}
	return i.nextInstallVersion.Add(1), nil
}

func validatePerasSegmentInstallResponse(segment fsperas.PerasSegment, resp *kvrpcpb.PerasInstallSegmentResponse) error {
	if resp == nil {
		return runtimeperas.ErrRuntimeInvalid
	}
	if !bytes.Equal(resp.GetSegmentRoot(), segment.Root[:]) {
		return runtimeperas.ErrRuntimeInvalid
	}
	stats := segment.Stats()
	if resp.GetOperationCount() != stats.OperationCount ||
		resp.GetEntryCount() != stats.EntryCount ||
		(stats.EntryCount > 0 && resp.GetAppliedEntries() == 0) {
		return runtimeperas.ErrRuntimeInvalid
	}
	return nil
}

func perasInstallCursorFromResponse(resp *kvrpcpb.PerasInstallSegmentResponse) runtimeperas.InstallCursor {
	if resp == nil {
		return runtimeperas.InstallCursor{}
	}
	return runtimeperas.InstallCursor{
		RegionID:       resp.GetRegionId(),
		Term:           resp.GetTerm(),
		Index:          resp.GetIndex(),
		InstallVersion: resp.GetCommitVersion(),
	}
}

func (i *raftstoreSegmentInstaller) publishInstalledSegment(segment fsperas.PerasSegment, resp *kvrpcpb.PerasInstallSegmentResponse) {
	if i == nil || i.router == nil || resp == nil || resp.GetRegionId() == 0 || resp.GetIndex() == 0 {
		return
	}
	commitVersion := resp.GetCommitVersion()
	if commitVersion == 0 {
		return
	}
	cursor := fsmeta.WatchCursor{
		RegionID: resp.GetRegionId(),
		Term:     resp.GetTerm(),
		Index:    resp.GetIndex(),
	}
	for _, entry := range segment.Dentries {
		if len(entry.Key) == 0 {
			continue
		}
		i.router.Publish(fsmeta.WatchEvent{
			Cursor:        cursor,
			CommitVersion: commitVersion,
			Source:        fsmeta.WatchEventSourceCommit,
			Key:           entry.Key,
		})
	}
}

func runtimeCloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
