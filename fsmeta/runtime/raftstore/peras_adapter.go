package raftstore

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	rsperas "github.com/feichai0017/NoKV/raftstore/peras"
	"github.com/feichai0017/NoKV/utils"
	"google.golang.org/grpc"
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

type remotePerasWitness struct {
	id     string
	client kvrpcpb.StoreKVClient
}

func newRemotePerasWitness(id string, client kvrpcpb.StoreKVClient) (*remotePerasWitness, error) {
	if id == "" || client == nil {
		return nil, runtimeperas.ErrRuntimeInvalid
	}
	return &remotePerasWitness{id: id, client: client}, nil
}

func (w *remotePerasWitness) ID() string {
	if w == nil {
		return ""
	}
	return w.id
}

func (w *remotePerasWitness) AppendSegment(ctx context.Context, scope compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	if w == nil || w.client == nil {
		return runtimeperas.ErrRuntimeInvalid
	}
	_, err := w.client.PerasWitnessSegment(ctx, &kvrpcpb.PerasWitnessSegmentRequest{
		Scope:  rsperas.ScopeToProto(scope),
		Record: rsperas.SegmentWitnessRecordToProto(record),
	})
	return err
}

func (w *remotePerasWitness) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if w == nil || w.client == nil {
		return fsperas.WitnessSnapshot{}, runtimeperas.ErrRuntimeInvalid
	}
	resp, err := w.client.PerasWitnessProbe(ctx, &kvrpcpb.PerasWitnessProbeRequest{EpochId: epochID})
	if err != nil {
		return fsperas.WitnessSnapshot{}, err
	}
	return rsperas.SnapshotFromProto(resp)
}

const (
	perasWitnessDiscoveryTimeout = 45 * time.Second
	perasWitnessDiscoveryBackoff = 100 * time.Millisecond
)

type witnessStoreLister interface {
	ListStores(context.Context, *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error)
}

type witnessConnections struct {
	witnesses []fsperas.WitnessReplica
	conns     []*grpc.ClientConn
}

func buildWitnessConnections(ctx context.Context, lister witnessStoreLister, dialOpts []grpc.DialOption, storeIDs []uint64) (*witnessConnections, error) {
	if lister == nil {
		return nil, errStoreListerRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, perasWitnessDiscoveryTimeout)
	defer cancel()

	allowed := make(map[uint64]struct{}, len(storeIDs))
	for _, id := range storeIDs {
		if id != 0 {
			allowed[id] = struct{}{}
		}
	}
	for {
		out, complete, err := tryBuildWitnessConnections(ctx, lister, dialOpts, allowed)
		if err != nil {
			return nil, err
		}
		if complete {
			return out, nil
		}
		if out != nil {
			_ = out.Close()
		}
		timer := time.NewTimer(perasWitnessDiscoveryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, runtimeperas.ErrRuntimeInvalid
		case <-timer.C:
		}
	}
}

func tryBuildWitnessConnections(ctx context.Context, lister witnessStoreLister, dialOpts []grpc.DialOption, allowed map[uint64]struct{}) (*witnessConnections, bool, error) {
	resp, err := lister.ListStores(ctx, &coordpb.ListStoresRequest{})
	if err != nil {
		return nil, false, err
	}
	out := &witnessConnections{}
	seen := make(map[uint64]struct{}, len(allowed))
	for _, store := range resp.GetStores() {
		if !witnessStoreSelected(store, allowed) {
			continue
		}
		if len(allowed) > 0 {
			seen[store.GetStoreId()] = struct{}{}
		}
		conn, err := grpc.NewClient(store.GetClientAddr(), dialOpts...)
		if err != nil {
			_ = out.Close()
			return nil, false, fmt.Errorf("dial peras witness store %d: %w", store.GetStoreId(), err)
		}
		witness, err := newRemotePerasWitness(
			fmt.Sprintf("store-%d", store.GetStoreId()),
			kvrpcpb.NewStoreKVClient(conn),
		)
		if err != nil {
			_ = conn.Close()
			_ = out.Close()
			return nil, false, err
		}
		out.conns = append(out.conns, conn)
		out.witnesses = append(out.witnesses, witness)
	}
	complete := len(out.witnesses) > 0
	if len(allowed) > 0 {
		complete = len(seen) == len(allowed)
	}
	if !complete {
		return out, false, nil
	}
	slices.SortFunc(out.witnesses, func(left, right fsperas.WitnessReplica) int {
		if left.ID() < right.ID() {
			return -1
		}
		if left.ID() > right.ID() {
			return 1
		}
		return 0
	})
	return out, true, nil
}

func witnessStoreSelected(store *coordpb.StoreInfo, allowed map[uint64]struct{}) bool {
	if store == nil || store.GetState() != coordpb.StoreState_STORE_STATE_UP || store.GetClientAddr() == "" {
		return false
	}
	if len(allowed) == 0 {
		return true
	}
	_, ok := allowed[store.GetStoreId()]
	return ok
}

func (c *witnessConnections) Close() error {
	if c == nil {
		return nil
	}
	var first error
	for _, conn := range c.conns {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.conns = nil
	c.witnesses = nil
	return first
}
