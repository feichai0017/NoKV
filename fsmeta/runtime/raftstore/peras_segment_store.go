// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/utils"
)

type perasSegmentInstallClient interface {
	InstallPerasSegment(context.Context, []byte, *kvrpcpb.PerasInstallSegmentRequest) (*kvrpcpb.PerasInstallSegmentResponse, error)
}

type perasSegmentRouteGrouper interface {
	GroupKeysByRoute(context.Context, [][]byte) ([]client.RouteKeyGroup, error)
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
	install, err := normalizedSegmentInstallPlan(req.Segment, req.Install, req.MaterializeMVCC)
	if err != nil {
		return runtimeperas.InstallCursor{}, err
	}
	routingKeys := install.RoutingKeys
	if len(routingKeys) == 0 {
		return runtimeperas.InstallCursor{}, runtimeperas.ErrRuntimeInvalid
	}
	canonicalObjectKey := runtimeCloneBytes(install.CanonicalObjectKey)
	if !req.MaterializeMVCC {
		if len(canonicalObjectKey) == 0 {
			return runtimeperas.InstallCursor{}, runtimeperas.ErrRuntimeInvalid
		}
	}
	routeGroups, err := i.installRouteGroups(ctx, kv, routingKeys)
	if err != nil {
		return runtimeperas.InstallCursor{}, err
	}
	results := make([]perasRouteInstallResult, len(routeGroups))
	parallelism := len(routeGroups)
	if limit := raftstoreSegmentInstallParallelism(); limit > 0 && parallelism > limit {
		parallelism = limit
	}
	throttle := utils.NewThrottle(parallelism)
	for idx, group := range routeGroups {
		idx, group := idx, cloneRouteKeyGroup(group)
		err := throttle.Go(func() error {
			result, err := i.installSegmentRouteGroup(ctx, kv, group.Keys, install, canonicalObjectKey, req.Segment, req.Payload, req.PayloadDigest, installVersion, req.MaterializeMVCC)
			if err != nil {
				return fmt.Errorf("peras install segment route group %d: %w", idx, err)
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
	result := choosePerasInstallResult(results, canonicalObjectKey, req.MaterializeMVCC)
	if !result.cursor.Valid() {
		return runtimeperas.InstallCursor{}, runtimeperas.ErrRuntimeInvalid
	}
	if !req.MaterializeMVCC && result.resp != nil {
		i.publishInstalledSegment(req.Segment, result.resp)
	}
	return result.cursor, nil
}

type perasRouteInstallResult struct {
	routingKeys [][]byte
	cursor      runtimeperas.InstallCursor
	resp        *kvrpcpb.PerasInstallSegmentResponse
}

func choosePerasInstallResult(results []perasRouteInstallResult, canonicalObjectKey []byte, materialize bool) perasRouteInstallResult {
	if !materialize && len(canonicalObjectKey) > 0 {
		for idx := range results {
			if keySetContains(results[idx].routingKeys, canonicalObjectKey) && results[idx].cursor.Valid() {
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

func (i *raftstoreSegmentInstaller) installRouteGroups(ctx context.Context, kv perasSegmentInstallClient, routingKeys [][]byte) ([]client.RouteKeyGroup, error) {
	if grouper, ok := kv.(perasSegmentRouteGrouper); ok {
		groups, err := grouper.GroupKeysByRoute(ctx, routingKeys)
		if err != nil {
			return nil, err
		}
		if len(groups) == 0 {
			return nil, runtimeperas.ErrRuntimeInvalid
		}
		groups = cloneRouteKeyGroups(groups)
		if len(groups) == 0 {
			return nil, runtimeperas.ErrRuntimeInvalid
		}
		return groups, nil
	}
	groups := make([]client.RouteKeyGroup, 0, len(routingKeys))
	for idx, key := range routingKeys {
		if len(key) == 0 {
			return nil, runtimeperas.ErrRuntimeInvalid
		}
		groups = append(groups, client.RouteKeyGroup{
			RegionID: uint64(idx + 1),
			Keys:     [][]byte{runtimeCloneBytes(key)},
		})
	}
	return groups, nil
}

func (i *raftstoreSegmentInstaller) installSegmentRouteGroup(
	ctx context.Context,
	kv perasSegmentInstallClient,
	routingKeys [][]byte,
	install compile.InstallPlan,
	canonicalObjectKey []byte,
	segment fsperas.PerasSegment,
	payload []byte,
	digest [32]byte,
	installVersion uint64,
	materialize bool,
) (perasRouteInstallResult, error) {
	if len(routingKeys) == 0 || len(routingKeys[0]) == 0 {
		return perasRouteInstallResult{}, runtimeperas.ErrRuntimeInvalid
	}
	routingKey := routingKeys[0]
	routePayload := payload
	if !materialize && len(canonicalObjectKey) > 0 && !keySetContains(routingKeys, canonicalObjectKey) {
		routePayload = nil
	}
	dependencyKeys, catalogKeys, materializedKeys, err := routeInstallHeaderKeys(install, segment.Root, routingKeys, materialize)
	if err != nil {
		return perasRouteInstallResult{}, err
	}
	stats := segment.Stats()
	readHeader := segment.ReadHeaderView()
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
		RoutingKeys:           cloneRuntimeKeySet(routingKeys),
		DependencyKeys:        dependencyKeys,
		CatalogKeys:           catalogKeys,
		MaterializedKeys:      materializedKeys,
		ReadFirstKey:          readHeader.FirstKey,
		ReadLastKey:           readHeader.LastKey,
		ReadDentryCount:       readHeader.DentryCount,
		ReadInodeCount:        readHeader.InodeCount,
		ReadSessionCount:      readHeader.SessionCount,
		ReadTombstoneCount:    readHeader.TombstoneCount,
		ReadDirectoryCount:    readHeader.DirectoryCount,
	})
	if err != nil {
		if client.IsRetryExhausted(err) {
			return perasRouteInstallResult{}, perasInstallRouteRetryExhaustedError{cause: err}
		}
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
	return perasRouteInstallResult{routingKeys: cloneRuntimeKeySet(routingKeys), cursor: cursor, resp: resp}, nil
}

func normalizedSegmentInstallPlan(segment fsperas.PerasSegment, plan compile.InstallPlan, materialize bool) (compile.InstallPlan, error) {
	if len(plan.RoutingKeys) == 0 || plan.Mode == compile.SegmentInstallNone {
		return fsperas.PerasSegmentInstallPlan(segment, materialize)
	}
	if plan.Materialize != materialize {
		return compile.InstallPlan{}, runtimeperas.ErrRuntimeInvalid
	}
	return compile.InstallPlan{
		Mode:               plan.Mode,
		Materialize:        plan.Materialize,
		RoutingKeys:        cloneRuntimeKeySet(plan.RoutingKeys),
		DependencyKeys:     cloneRuntimeKeySet(plan.DependencyKeys),
		CatalogKeys:        cloneRuntimeKeySet(plan.CatalogKeys),
		MaterializedKeys:   cloneRuntimeKeySet(plan.MaterializedKeys),
		CanonicalObjectKey: runtimeCloneBytes(plan.CanonicalObjectKey),
	}, nil
}

func routeInstallHeaderKeys(plan compile.InstallPlan, root [32]byte, routingKeys [][]byte, materialize bool) ([][]byte, [][]byte, [][]byte, error) {
	if materialize {
		if len(plan.DependencyKeys) == 0 || len(plan.MaterializedKeys) == 0 {
			return nil, nil, nil, runtimeperas.ErrRuntimeInvalid
		}
		return cloneRuntimeKeySet(plan.DependencyKeys), cloneRuntimeKeySet(plan.CatalogKeys), cloneRuntimeKeySet(plan.MaterializedKeys), nil
	}
	routeKeys := make([][]byte, 0, len(routingKeys)*2)
	for _, routingKey := range routingKeys {
		keys, err := fsperas.PerasSegmentCatalogRouteInstallKeys(root, routingKey)
		if err != nil {
			return nil, nil, nil, err
		}
		routeKeys = appendRuntimeUniqueKeys(routeKeys, keys...)
	}
	return routeKeys, routeKeys, nil, nil
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

func cloneRuntimeKeySet(keys [][]byte) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		out = append(out, runtimeCloneBytes(key))
	}
	return out
}

func appendRuntimeUniqueKeys(dst [][]byte, keys ...[]byte) [][]byte {
	for _, key := range keys {
		if len(key) == 0 || keySetContains(dst, key) {
			continue
		}
		dst = append(dst, runtimeCloneBytes(key))
	}
	return dst
}

func keySetContains(keys [][]byte, target []byte) bool {
	if len(target) == 0 {
		return false
	}
	for _, key := range keys {
		if bytes.Equal(key, target) {
			return true
		}
	}
	return false
}

func cloneRouteKeyGroup(group client.RouteKeyGroup) client.RouteKeyGroup {
	return client.RouteKeyGroup{
		RegionID:      group.RegionID,
		LeaderStoreID: group.LeaderStoreID,
		Keys:          cloneRuntimeKeySet(group.Keys),
	}
}

func cloneRouteKeyGroups(groups []client.RouteKeyGroup) []client.RouteKeyGroup {
	out := make([]client.RouteKeyGroup, 0, len(groups))
	for _, group := range groups {
		if len(group.Keys) == 0 {
			continue
		}
		out = append(out, cloneRouteKeyGroup(group))
	}
	return out
}

type raftstoreSegmentCatalogScanner struct {
	runner *Runner
}

func (s raftstoreSegmentCatalogScanner) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]runtimeperas.KV, error) {
	if s.runner == nil {
		return nil, runtimeperas.ErrRuntimeInvalid
	}
	rows, err := s.runner.Scan(ctx, startKey, limit, version)
	if err != nil {
		return nil, err
	}
	out := make([]runtimeperas.KV, 0, len(rows))
	for _, row := range rows {
		out = append(out, runtimeperas.KV{
			Key:   runtimeCloneBytes(row.Key),
			Value: runtimeCloneBytes(row.Value),
		})
	}
	return out, nil
}
