// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"fmt"
	"sync"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	metadatapb "github.com/feichai0017/NoKV/pb/metadata"
)

const maxRouteAttempts = 3

// TimestampSource supplies metadata MVCC timestamps. Production wiring should
// bind this to coordinator TSO; tests may use MonotonicTimestampSource.
type TimestampSource interface {
	ReserveTimestamp(context.Context, uint64) (uint64, error)
}

// MetadataRoute is one resolved data-plane serving route.
type MetadataRoute struct {
	Context *metadatapb.MetadataContext
	Client  metadatapb.MetadataPlaneClient
}

// RouteProvider returns the current serving route for a metadata key. The
// provider is a rebuildable serving view; root remains the source of topology
// truth.
type RouteProvider interface {
	RouteForKey(context.Context, []byte) (MetadataRoute, error)
}

type routeErrorObserver interface {
	ObserveRegionError(context.Context, []byte, MetadataRoute, *errorpb.RegionError)
}

// StaticRouteProvider is a single-region route provider used by tests and
// local experiments before coordinator-backed routing is wired in.
type StaticRouteProvider struct {
	Context *metadatapb.MetadataContext
	Client  metadatapb.MetadataPlaneClient
}

func (p StaticRouteProvider) RouteForKey(context.Context, []byte) (MetadataRoute, error) {
	if p.Context == nil || p.Context.GetRegionId() == 0 {
		return MetadataRoute{}, errRouteProviderRequired
	}
	if p.Client == nil {
		return MetadataRoute{}, errClientRequired
	}
	return MetadataRoute{
		Context: cloneMetadataContext(p.Context),
		Client:  p.Client,
	}, nil
}

// MonotonicTimestampSource is a process-local timestamp source for tests.
type MonotonicTimestampSource struct {
	mu   sync.Mutex
	next uint64
}

func NewMonotonicTimestampSource(first uint64) *MonotonicTimestampSource {
	if first == 0 {
		first = 1
	}
	return &MonotonicTimestampSource{next: first}
}

func (s *MonotonicTimestampSource) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errInvalidMetadataCommand
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	first := s.next
	s.next += count
	return first, nil
}

// Runner implements backend.Store over MetadataPlane.
type Runner struct {
	routes RouteProvider
	tso    TimestampSource
}

func NewRunner(routes RouteProvider, tso TimestampSource) (*Runner, error) {
	if routes == nil {
		return nil, errRouteProviderRequired
	}
	if tso == nil {
		return nil, errTimestampSourceRequired
	}
	return &Runner{routes: routes, tso: tso}, nil
}

// forwarding-ok: backend.Store requires timestamp reservation on Runner; timestamp authority stays behind TimestampSource.
func (r *Runner) ReserveTimestamp(ctx context.Context, count uint64) (uint64, error) {
	return r.tso.ReserveTimestamp(ctx, count)
}

func (r *Runner) Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error) {
	var lastErr error
	for attempt := 0; attempt < maxRouteAttempts; attempt++ {
		route, err := r.routes.RouteForKey(ctx, key)
		if err != nil {
			return nil, false, err
		}
		resp, err := route.Client.Get(ctx, &metadatapb.MetadataGetRequest{
			Context: route.Context,
			Key:     cloneBytes(key),
			Version: version,
		})
		if err != nil {
			return nil, false, err
		}
		if routeErr := resp.GetRegionError(); routeErr != nil {
			lastErr = regionError(routeErr)
			if !canRetryRouteError(lastErr) || attempt == maxRouteAttempts-1 {
				return nil, false, lastErr
			}
			r.observeRegionError(ctx, key, route, routeErr)
			continue
		}
		if err := keyError(resp.GetError()); err != nil {
			return nil, false, err
		}
		if resp.GetNotFound() || resp.GetKv() == nil {
			return nil, false, nil
		}
		return cloneBytes(resp.GetKv().GetValue()), true, nil
	}
	return nil, false, lastErr
}

func (r *Runner) BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	reqs := make([]*metadatapb.MetadataGetRequest, 0, len(keys))
	for _, key := range keys {
		reqs = append(reqs, &metadatapb.MetadataGetRequest{
			Key:     cloneBytes(key),
			Version: version,
		})
	}
	var lastErr error
	for attempt := 0; attempt < maxRouteAttempts; attempt++ {
		route, err := r.routes.RouteForKey(ctx, keys[0])
		if err != nil {
			return nil, err
		}
		resp, err := route.Client.BatchGet(ctx, &metadatapb.MetadataBatchGetRequest{
			Context:  route.Context,
			Requests: reqs,
		})
		if err != nil {
			return nil, err
		}
		if routeErr := resp.GetRegionError(); routeErr != nil {
			lastErr = regionError(routeErr)
			if !canRetryRouteError(lastErr) || attempt == maxRouteAttempts-1 {
				return nil, lastErr
			}
			r.observeRegionError(ctx, keys[0], route, routeErr)
			continue
		}
		for i, item := range resp.GetResponses() {
			if routeErr := item.GetRegionError(); routeErr != nil {
				return nil, regionError(routeErr)
			}
			if err := keyError(item.GetError()); err != nil {
				return nil, err
			}
			if item.GetNotFound() || item.GetKv() == nil || i >= len(keys) {
				continue
			}
			out[string(keys[i])] = cloneBytes(item.GetKv().GetValue())
		}
		return out, nil
	}
	return nil, lastErr
}

func (r *Runner) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]backend.KV, error) {
	if limit == 0 {
		return nil, nil
	}
	var lastErr error
	for attempt := 0; attempt < maxRouteAttempts; attempt++ {
		route, err := r.routes.RouteForKey(ctx, startKey)
		if err != nil {
			return nil, err
		}
		resp, err := route.Client.Scan(ctx, &metadatapb.MetadataScanRequest{
			Context:  route.Context,
			StartKey: cloneBytes(startKey),
			Limit:    limit,
			Version:  version,
		})
		if err != nil {
			return nil, err
		}
		if routeErr := resp.GetRegionError(); routeErr != nil {
			lastErr = regionError(routeErr)
			if !canRetryRouteError(lastErr) || attempt == maxRouteAttempts-1 {
				return nil, lastErr
			}
			r.observeRegionError(ctx, startKey, route, routeErr)
			continue
		}
		if err := keyError(resp.GetError()); err != nil {
			return nil, err
		}
		kvs := make([]backend.KV, 0, len(resp.GetKvs()))
		for _, kv := range resp.GetKvs() {
			kvs = append(kvs, backend.KV{Key: cloneBytes(kv.GetKey()), Value: cloneBytes(kv.GetValue())})
		}
		return kvs, nil
	}
	return nil, lastErr
}

func (r *Runner) CommitMetadata(ctx context.Context, command backend.MetadataCommand) (backend.MetadataCommitResult, error) {
	if command.ReadVersion == 0 {
		return backend.MetadataCommitResult{}, errInvalidMetadataCommand
	}
	routeKey := command.PrimaryKey
	if len(routeKey) == 0 && len(command.Mutations) != 0 {
		routeKey = command.Mutations[0].Key
	}
	var lastErr error
	for attempt := 0; attempt < maxRouteAttempts; attempt++ {
		route, err := r.routes.RouteForKey(ctx, routeKey)
		if err != nil {
			return backend.MetadataCommitResult{}, err
		}
		resp, err := route.Client.CommitMetadata(ctx, &metadatapb.MetadataCommitRequest{
			Context: route.Context,
			Command: metadataCommandToProto(command),
		})
		if err != nil {
			return backend.MetadataCommitResult{}, err
		}
		if routeErr := resp.GetRegionError(); routeErr != nil {
			lastErr = regionError(routeErr)
			if !canRetryRouteError(lastErr) || attempt == maxRouteAttempts-1 {
				return backend.MetadataCommitResult{}, lastErr
			}
			r.observeRegionError(ctx, routeKey, route, routeErr)
			continue
		}
		if err := keyError(resp.GetError()); err != nil {
			return backend.MetadataCommitResult{}, err
		}
		result := resp.GetResult()
		if result == nil {
			return backend.MetadataCommitResult{}, nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: missing metadata commit result")
		}
		return backend.MetadataCommitResult{
			CommitVersion:    result.GetCommitVersion(),
			RegionID:         result.GetRegionId(),
			Term:             result.GetTerm(),
			Index:            result.GetIndex(),
			AppliedMutations: result.GetAppliedMutations(),
		}, nil
	}
	return backend.MetadataCommitResult{}, lastErr
}

func (r *Runner) observeRegionError(ctx context.Context, key []byte, route MetadataRoute, err *errorpb.RegionError) {
	observer, ok := r.routes.(routeErrorObserver)
	if !ok {
		return
	}
	observer.ObserveRegionError(ctx, key, route, err)
}

func canRetryRouteError(err error) bool {
	return nokverrors.Retryable(err)
}

func metadataCommandToProto(command backend.MetadataCommand) *metadatapb.MetadataCommand {
	return &metadatapb.MetadataCommand{
		RequestId:     cloneBytes(command.RequestID),
		Mount:         command.Mount,
		MountKeyId:    command.MountKeyID,
		PrimaryKey:    cloneBytes(command.PrimaryKey),
		ReadVersion:   command.ReadVersion,
		CommitVersion: command.CommitVersion,
		Predicates:    metadataPredicatesToProto(command.Predicates),
		Mutations:     metadataMutationsToProto(command.Mutations),
		WatchKeys:     cloneByteSlices(command.WatchKeys),
	}
}

func metadataPredicatesToProto(predicates []*backend.Predicate) []*metadatapb.MetadataPredicate {
	out := make([]*metadatapb.MetadataPredicate, 0, len(predicates))
	for _, pred := range predicates {
		if pred == nil {
			continue
		}
		out = append(out, &metadatapb.MetadataPredicate{
			Key:           cloneBytes(pred.Key),
			Kind:          metadataPredicateKindToProto(pred.Kind),
			ReadVersion:   pred.ReadVersion,
			ExpectedValue: cloneBytes(pred.ExpectedValue),
		})
	}
	return out
}

func metadataMutationsToProto(mutations []*backend.Mutation) []*metadatapb.MetadataMutation {
	out := make([]*metadatapb.MetadataMutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		out = append(out, &metadatapb.MetadataMutation{
			Op:                metadataMutationOpToProto(mut.Op),
			Key:               cloneBytes(mut.Key),
			Value:             cloneBytes(mut.Value),
			AssertionNotExist: mut.AssertionNotExist,
			ExpiresAt:         mut.ExpiresAt,
		})
	}
	return out
}

func metadataPredicateKindToProto(kind backend.PredicateKind) metadatapb.MetadataPredicateKind {
	switch kind {
	case backend.PredicateNotExists:
		return metadatapb.MetadataPredicateKind_METADATA_PREDICATE_KIND_NOT_EXISTS
	case backend.PredicateExists:
		return metadatapb.MetadataPredicateKind_METADATA_PREDICATE_KIND_EXISTS
	case backend.PredicateValueEquals:
		return metadatapb.MetadataPredicateKind_METADATA_PREDICATE_KIND_VALUE_EQUALS
	default:
		return metadatapb.MetadataPredicateKind_METADATA_PREDICATE_KIND_NOT_EXISTS
	}
}

func metadataMutationOpToProto(op backend.MutationOp) metadatapb.MetadataMutation_Op {
	switch op {
	case backend.MutationPut:
		return metadatapb.MetadataMutation_PUT
	case backend.MutationDelete:
		return metadatapb.MetadataMutation_DELETE
	default:
		return metadatapb.MetadataMutation_PUT
	}
}

func regionError(err *errorpb.RegionError) error {
	switch {
	case err == nil:
		return nil
	case err.GetNotLeader() != nil:
		notLeader := err.GetNotLeader()
		return nokverrors.New(nokverrors.KindNotLeader, fmt.Sprintf("fsmeta/runtime/raftstore: region %d not leader", notLeader.GetRegionId()))
	case err.GetEpochNotMatch() != nil:
		return nokverrors.New(nokverrors.KindStaleEpoch, "fsmeta/runtime/raftstore: region epoch not match")
	case err.GetStaleCommand() != nil:
		return nokverrors.New(nokverrors.KindStaleEpoch, "fsmeta/runtime/raftstore: stale command")
	case err.GetEntryTooLarge() != nil:
		tooLarge := err.GetEntryTooLarge()
		return nokverrors.New(nokverrors.KindResourceExhausted, fmt.Sprintf("fsmeta/runtime/raftstore: region %d raft entry too large", tooLarge.GetRegionId()))
	case err.GetStoreNotMatch() != nil:
		mismatch := err.GetStoreNotMatch()
		return nokverrors.New(nokverrors.KindRegionRouting, fmt.Sprintf("fsmeta/runtime/raftstore: store mismatch request=%d actual=%d", mismatch.GetRequestStoreId(), mismatch.GetActualStoreId()))
	case err.GetRegionNotFound() != nil:
		notFound := err.GetRegionNotFound()
		return nokverrors.New(nokverrors.KindRegionRouting, fmt.Sprintf("fsmeta/runtime/raftstore: region %d not found", notFound.GetRegionId()))
	case err.GetKeyNotInRegion() != nil:
		keyErr := err.GetKeyNotInRegion()
		return nokverrors.New(nokverrors.KindRegionRouting, fmt.Sprintf("fsmeta/runtime/raftstore: key not in region %d", keyErr.GetRegionId()))
	default:
		return nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: empty region error")
	}
}

func keyError(err *metadatapb.MetadataKeyError) error {
	if err == nil {
		return nil
	}
	if retryable := err.GetRetryable(); retryable != "" {
		return nokverrors.New(nokverrors.KindRetryable, retryable)
	}
	if abort := err.GetAbort(); abort != "" {
		return nokverrors.New(nokverrors.KindAborted, abort)
	}
	if exists := err.GetAlreadyExists(); exists != nil {
		msg := fmt.Sprintf("fsmeta/runtime/raftstore: metadata key already exists: %q", exists.GetKey())
		return nokverrors.New(nokverrors.KindAlreadyExists, msg)
	}
	if conflict := err.GetWriteConflict(); conflict != nil {
		msg := fmt.Sprintf(
			"fsmeta/runtime/raftstore: metadata write conflict key=%q primary=%q start=%d conflict=%d commit=%d",
			conflict.GetKey(),
			conflict.GetPrimary(),
			conflict.GetStartTs(),
			conflict.GetConflictTs(),
			conflict.GetCommitTs(),
		)
		return nokverrors.New(nokverrors.KindWriteConflict, msg)
	}
	if locked := err.GetLocked(); locked != nil {
		msg := fmt.Sprintf(
			"fsmeta/runtime/raftstore: metadata lock conflict key=%q primary=%q version=%d ttl=%d",
			locked.GetKey(),
			locked.GetPrimaryLock(),
			locked.GetLockVersion(),
			locked.GetLockTtl(),
		)
		return nokverrors.New(nokverrors.KindLockConflict, msg)
	}
	return nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: empty metadata key error")
}

func cloneByteSlices(src [][]byte) [][]byte {
	out := make([][]byte, 0, len(src))
	for _, item := range src {
		out = append(out, cloneBytes(item))
	}
	return out
}

func cloneMetadataContext(src *metadatapb.MetadataContext) *metadatapb.MetadataContext {
	if src == nil {
		return nil
	}
	return &metadatapb.MetadataContext{
		RegionId:        src.GetRegionId(),
		RegionEpoch:     src.GetRegionEpoch(),
		Peer:            src.GetPeer(),
		ReadConsistency: src.GetReadConsistency(),
		ReadPreference:  src.GetReadPreference(),
	}
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	return append([]byte(nil), src...)
}
