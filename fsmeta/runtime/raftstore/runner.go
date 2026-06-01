// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	metadatapb "github.com/feichai0017/NoKV/pb/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxRouteAttempts = 3

// TimestampSource supplies metadata MVCC timestamps.
type TimestampSource interface {
	ReserveTimestamp(context.Context, uint64) (uint64, error)
}

// MetadataRoute is one resolved data-plane serving route.
type MetadataRoute struct {
	Context   *metadatapb.MetadataContext
	StoreAddr string
	Client    metadatapb.MetadataPlaneClient
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

type routeFailureObserver interface {
	ObserveRouteFailure(context.Context, []byte, MetadataRoute, error)
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
			Context:   route.Context,
			Key:       cloneBytes(key),
			Version:   version,
			KeyFamily: metadataFamilyToProto(metadataFamilyForKey(key)),
		})
		if err != nil {
			if shouldRetryRouteCall(ctx, err, attempt) {
				lastErr = err
				r.observeRouteFailure(ctx, key, route, err)
				continue
			}
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
			Key:       cloneBytes(key),
			Version:   version,
			KeyFamily: metadataFamilyToProto(metadataFamilyForKey(key)),
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
			if shouldRetryRouteCall(ctx, err, attempt) {
				lastErr = err
				r.observeRouteFailure(ctx, keys[0], route, err)
				continue
			}
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
			Context:   route.Context,
			StartKey:  cloneBytes(startKey),
			Limit:     limit,
			Version:   version,
			KeyFamily: metadataFamilyToProto(metadataFamilyForKey(startKey)),
		})
		if err != nil {
			if shouldRetryRouteCall(ctx, err, attempt) {
				lastErr = err
				r.observeRouteFailure(ctx, startKey, route, err)
				continue
			}
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
			kvs = append(kvs, backend.KV{
				Family: metadataFamilyFromProto(kv.GetKeyFamily()),
				Key:    cloneBytes(kv.GetKey()),
				Value:  cloneBytes(kv.GetValue()),
			})
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
			if shouldRetryRouteCall(ctx, err, attempt) {
				lastErr = err
				r.observeRouteFailure(ctx, routeKey, route, err)
				continue
			}
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

func (r *Runner) observeRouteFailure(ctx context.Context, key []byte, route MetadataRoute, err error) {
	observer, ok := r.routes.(routeFailureObserver)
	if !ok {
		return
	}
	observer.ObserveRouteFailure(ctx, key, route, err)
}

func shouldRetryRouteCall(ctx context.Context, err error, attempt int) bool {
	if err == nil || attempt >= maxRouteAttempts-1 || ctx.Err() != nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func canRetryRouteError(err error) bool {
	return nokverrors.Retryable(err)
}

func metadataCommandToProto(command backend.MetadataCommand) *metadatapb.MetadataCommand {
	return &metadatapb.MetadataCommand{
		RequestId:        cloneBytes(command.RequestID),
		Mount:            command.Mount,
		MountKeyId:       command.MountKeyID,
		PrimaryKey:       cloneBytes(command.PrimaryKey),
		PrimaryKeyFamily: metadataFamilyToProto(metadataFamilyOrKey(command.PrimaryFamily, command.PrimaryKey)),
		ReadVersion:      command.ReadVersion,
		CommitVersion:    command.CommitVersion,
		Predicates:       metadataPredicatesToProto(command.Predicates),
		Mutations:        metadataMutationsToProto(command.Mutations),
		WatchKeys:        cloneByteSlices(command.WatchKeys),
		WatchKeyRefs:     metadataWatchRefsToProto(command.WatchRefs, command.WatchKeys),
		WatchEvents:      metadataWatchEventsToProto(command.WatchEvents),
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
			KeyFamily:     metadataFamilyToProto(metadataFamilyOrKey(pred.Family, pred.Key)),
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
			Op:                  metadataMutationOpToProto(mut.Op),
			Key:                 cloneBytes(mut.Key),
			KeyFamily:           metadataFamilyToProto(metadataFamilyOrKey(mut.Family, mut.Key)),
			Value:               cloneBytes(mut.Value),
			AssertionNotExist:   mut.AssertionNotExist,
			ExpiresAt:           mut.ExpiresAt,
			RetentionPinVersion: mut.RetentionPinVersion,
		})
	}
	return out
}

func metadataWatchRefsToProto(refs []backend.KeyRef, fallback [][]byte) []*metadatapb.MetadataWatchKey {
	out := make([]*metadatapb.MetadataWatchKey, 0, len(refs)+len(fallback))
	if len(refs) != 0 {
		for _, ref := range refs {
			if len(ref.Key) == 0 {
				continue
			}
			out = append(out, &metadatapb.MetadataWatchKey{
				Key:       cloneBytes(ref.Key),
				KeyFamily: metadataFamilyToProto(metadataFamilyOrKey(ref.Family, ref.Key)),
			})
		}
		return out
	}
	for _, key := range fallback {
		if len(key) == 0 {
			continue
		}
		out = append(out, &metadatapb.MetadataWatchKey{
			Key:       cloneBytes(key),
			KeyFamily: metadataFamilyToProto(metadataFamilyForKey(key)),
		})
	}
	return out
}

func metadataWatchEventsToProto(events []backend.WatchEvent) []*metadatapb.MetadataWatchEvent {
	out := make([]*metadatapb.MetadataWatchEvent, 0, len(events))
	for _, event := range events {
		if len(event.Key) == 0 {
			continue
		}
		out = append(out, &metadatapb.MetadataWatchEvent{
			KeyFamily: metadataFamilyToProto(metadataFamilyOrKey(event.Family, event.Key)),
			Key:       cloneBytes(event.Key),
			Operation: metadataWatchOperationToProto(event.Operation),
			Parent:    event.Parent,
			Name:      event.Name,
			Inode:     event.Inode,
			OldParent: event.OldParent,
			OldName:   event.OldName,
			NewParent: event.NewParent,
			NewName:   event.NewName,
		})
	}
	return out
}

func metadataWatchOperationToProto(op backend.WatchOperation) metadatapb.MetadataWatchOperation {
	switch op {
	case backend.WatchOperationCreate:
		return metadatapb.MetadataWatchOperation_METADATA_WATCH_OPERATION_CREATE
	case backend.WatchOperationUpdate:
		return metadatapb.MetadataWatchOperation_METADATA_WATCH_OPERATION_UPDATE
	case backend.WatchOperationDelete:
		return metadatapb.MetadataWatchOperation_METADATA_WATCH_OPERATION_DELETE
	case backend.WatchOperationRename:
		return metadatapb.MetadataWatchOperation_METADATA_WATCH_OPERATION_RENAME
	case backend.WatchOperationReplace:
		return metadatapb.MetadataWatchOperation_METADATA_WATCH_OPERATION_REPLACE
	case backend.WatchOperationLink:
		return metadatapb.MetadataWatchOperation_METADATA_WATCH_OPERATION_LINK
	default:
		return metadatapb.MetadataWatchOperation_METADATA_WATCH_OPERATION_UNSPECIFIED
	}
}

func metadataPredicateKindToProto(kind backend.PredicateKind) metadatapb.MetadataPredicateKind {
	switch kind {
	case backend.PredicateNotExists:
		return metadatapb.MetadataPredicateKind_METADATA_PREDICATE_KIND_NOT_EXISTS
	case backend.PredicateExists:
		return metadatapb.MetadataPredicateKind_METADATA_PREDICATE_KIND_EXISTS
	case backend.PredicateValueEquals:
		return metadatapb.MetadataPredicateKind_METADATA_PREDICATE_KIND_VALUE_EQUALS
	case backend.PredicatePrefixEmpty:
		return metadatapb.MetadataPredicateKind_METADATA_PREDICATE_KIND_PREFIX_EMPTY
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

func metadataFamilyForKey(key []byte) backend.MetadataFamily {
	kind, err := layout.KeyKindOf(key)
	if err != nil {
		return backend.MetadataFamilyUnspecified
	}
	switch kind {
	case layout.KeyKindMount:
		return backend.MetadataFamilyMount
	case layout.KeyKindInode:
		return backend.MetadataFamilyInode
	case layout.KeyKindDentry:
		return backend.MetadataFamilyDentry
	case layout.KeyKindParent:
		return backend.MetadataFamilyParent
	case layout.KeyKindChunk:
		return backend.MetadataFamilyChunk
	case layout.KeyKindSession:
		return backend.MetadataFamilySession
	case layout.KeyKindUsage:
		return backend.MetadataFamilyQuota
	case layout.KeyKindSnapshot:
		return backend.MetadataFamilySnapshot
	case layout.KeyKindPath:
		return backend.MetadataFamilyPathIndex
	case layout.KeyKindSegment:
		return backend.MetadataFamilySegment
	default:
		return backend.MetadataFamilyUnspecified
	}
}

func metadataFamilyOrKey(family backend.MetadataFamily, key []byte) backend.MetadataFamily {
	if family != backend.MetadataFamilyUnspecified {
		return family
	}
	return metadataFamilyForKey(key)
}

func metadataFamilyToProto(family backend.MetadataFamily) metadatapb.MetadataFamily {
	switch family {
	case backend.MetadataFamilyMount:
		return metadatapb.MetadataFamily_METADATA_FAMILY_MOUNT
	case backend.MetadataFamilyInode:
		return metadatapb.MetadataFamily_METADATA_FAMILY_INODE
	case backend.MetadataFamilyDentry:
		return metadatapb.MetadataFamily_METADATA_FAMILY_DENTRY
	case backend.MetadataFamilyParent:
		return metadatapb.MetadataFamily_METADATA_FAMILY_PARENT
	case backend.MetadataFamilyChunk:
		return metadatapb.MetadataFamily_METADATA_FAMILY_CHUNK
	case backend.MetadataFamilySession:
		return metadatapb.MetadataFamily_METADATA_FAMILY_SESSION
	case backend.MetadataFamilyQuota:
		return metadatapb.MetadataFamily_METADATA_FAMILY_QUOTA
	case backend.MetadataFamilySnapshot:
		return metadatapb.MetadataFamily_METADATA_FAMILY_SNAPSHOT
	case backend.MetadataFamilyPathIndex:
		return metadatapb.MetadataFamily_METADATA_FAMILY_PATH_INDEX
	case backend.MetadataFamilyWatch:
		return metadatapb.MetadataFamily_METADATA_FAMILY_WATCH
	case backend.MetadataFamilyCommandDedupe:
		return metadatapb.MetadataFamily_METADATA_FAMILY_COMMAND_DEDUPE
	case backend.MetadataFamilySegment:
		return metadatapb.MetadataFamily_METADATA_FAMILY_SEGMENT
	default:
		return metadatapb.MetadataFamily_METADATA_FAMILY_UNSPECIFIED
	}
}

func metadataFamilyFromProto(family metadatapb.MetadataFamily) backend.MetadataFamily {
	switch family {
	case metadatapb.MetadataFamily_METADATA_FAMILY_MOUNT:
		return backend.MetadataFamilyMount
	case metadatapb.MetadataFamily_METADATA_FAMILY_INODE:
		return backend.MetadataFamilyInode
	case metadatapb.MetadataFamily_METADATA_FAMILY_DENTRY:
		return backend.MetadataFamilyDentry
	case metadatapb.MetadataFamily_METADATA_FAMILY_PARENT:
		return backend.MetadataFamilyParent
	case metadatapb.MetadataFamily_METADATA_FAMILY_CHUNK:
		return backend.MetadataFamilyChunk
	case metadatapb.MetadataFamily_METADATA_FAMILY_SESSION:
		return backend.MetadataFamilySession
	case metadatapb.MetadataFamily_METADATA_FAMILY_QUOTA:
		return backend.MetadataFamilyQuota
	case metadatapb.MetadataFamily_METADATA_FAMILY_SNAPSHOT:
		return backend.MetadataFamilySnapshot
	case metadatapb.MetadataFamily_METADATA_FAMILY_PATH_INDEX:
		return backend.MetadataFamilyPathIndex
	case metadatapb.MetadataFamily_METADATA_FAMILY_WATCH:
		return backend.MetadataFamilyWatch
	case metadatapb.MetadataFamily_METADATA_FAMILY_COMMAND_DEDUPE:
		return backend.MetadataFamilyCommandDedupe
	case metadatapb.MetadataFamily_METADATA_FAMILY_SEGMENT:
		return backend.MetadataFamilySegment
	default:
		return backend.MetadataFamilyUnspecified
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
		return nokverrors.NewMetadataKeyError(nokverrors.MetadataKeyIssue{Kind: nokverrors.KindRetryable, Message: retryable})
	}
	if abort := err.GetAbort(); abort != "" {
		return nokverrors.NewMetadataKeyError(nokverrors.MetadataKeyIssue{Kind: nokverrors.KindAborted, Message: abort})
	}
	if exists := err.GetAlreadyExists(); exists != nil {
		msg := fmt.Sprintf("fsmeta/runtime/raftstore: metadata key already exists: %q", exists.GetKey())
		return nokverrors.NewMetadataKeyError(nokverrors.MetadataKeyIssue{
			Kind:    nokverrors.KindAlreadyExists,
			Key:     cloneBytes(exists.GetKey()),
			Message: msg,
		})
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
