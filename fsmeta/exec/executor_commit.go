// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

func (e *Executor) commitWithMetadataPredicates(ctx context.Context, kind model.OperationKind, mount model.MountIdentity, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, startVersion, commitVersion uint64) error {
	return e.commitWithMetadataPredicatesAndWatch(ctx, kind, mount, primary, predicates, mutations, nil, startVersion, commitVersion)
}

func (e *Executor) commitWithMetadataPredicatesAndWatch(ctx context.Context, kind model.OperationKind, mount model.MountIdentity, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, watchEvents []backend.WatchEvent, startVersion, commitVersion uint64) error {
	stats := e.metadataPredicateCounters(kind)
	if stats != nil {
		stats.attemptTotal.Add(1)
	}
	err := e.commitMetadataCommandWithWatch(ctx, mount, primary, predicates, mutations, watchEvents, startVersion, commitVersion)
	if err == nil && stats != nil {
		stats.successTotal.Add(1)
	}
	return err
}

func (e *Executor) commitMetadataCommand(ctx context.Context, mount model.MountIdentity, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, startVersion, commitVersion uint64) error {
	return e.commitMetadataCommandWithWatch(ctx, mount, primary, predicates, mutations, nil, startVersion, commitVersion)
}

func (e *Executor) commitMetadataCommandWithWatch(ctx context.Context, mount model.MountIdentity, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, watchEvents []backend.WatchEvent, startVersion, commitVersion uint64) error {
	_, err := e.commitMetadataCommandWithVersionAndWatch(ctx, mount, primary, predicates, mutations, watchEvents, startVersion, 0, commitVersion)
	return err
}

func (e *Executor) commitMetadataCommandAtWithWatch(ctx context.Context, mount model.MountIdentity, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, watchEvents []backend.WatchEvent, startVersion, commitVersion uint64) (backend.MetadataCommitResult, error) {
	return e.commitMetadataCommandWithVersionAndWatch(ctx, mount, primary, predicates, mutations, watchEvents, startVersion, commitVersion, commitVersion)
}

func (e *Executor) commitMetadataCommandWithVersionAndWatch(ctx context.Context, mount model.MountIdentity, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, watchEvents []backend.WatchEvent, startVersion, commandCommitVersion, requestIDCommitVersion uint64) (backend.MetadataCommitResult, error) {
	clonedPredicates := cloneMetadataPredicates(predicates)
	clonedMutations := cloneMutations(mutations)
	clonedWatchEvents := cloneWatchEvents(watchEvents)
	return e.runner.CommitMetadata(ctx, backend.MetadataCommand{
		RequestID:     metadataCommandRequestID(mount, primary, startVersion, requestIDCommitVersion),
		Mount:         string(mount.MountID),
		MountKeyID:    uint64(mount.MountKeyID),
		PrimaryFamily: metadataFamilyForKey(primary),
		PrimaryKey:    cloneBytes(primary),
		ReadVersion:   startVersion,
		CommitVersion: commandCommitVersion,
		Predicates:    clonedPredicates,
		Mutations:     clonedMutations,
		WatchKeys:     metadataCommandWatchKeys(clonedMutations),
		WatchRefs:     metadataCommandWatchRefs(clonedMutations),
		WatchEvents:   clonedWatchEvents,
	})
}

func metadataCommandRequestID(mount model.MountIdentity, primary []byte, startVersion, commitVersion uint64) []byte {
	out := make([]byte, 0, len(mount.MountID)+len(primary)+32)
	out = append(out, string(mount.MountID)...)
	out = append(out, 0)
	var fixed [24]byte
	binary.BigEndian.PutUint64(fixed[0:8], uint64(mount.MountKeyID))
	binary.BigEndian.PutUint64(fixed[8:16], startVersion)
	binary.BigEndian.PutUint64(fixed[16:24], commitVersion)
	out = append(out, fixed[:]...)
	out = append(out, primary...)
	return out
}

func metadataCommandWatchKeys(mutations []*backend.Mutation) [][]byte {
	keys := make([][]byte, 0, len(mutations))
	seen := make(map[string]struct{}, len(mutations))
	for _, mut := range mutations {
		if mut == nil || len(mut.Key) == 0 {
			continue
		}
		if _, ok := seen[string(mut.Key)]; ok {
			continue
		}
		seen[string(mut.Key)] = struct{}{}
		keys = append(keys, cloneBytes(mut.Key))
	}
	return keys
}

func metadataCommandWatchRefs(mutations []*backend.Mutation) []backend.KeyRef {
	refs := make([]backend.KeyRef, 0, len(mutations))
	seen := make(map[string]struct{}, len(mutations))
	for _, mut := range mutations {
		if mut == nil || len(mut.Key) == 0 {
			continue
		}
		key := string(mut.Key)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		refs = append(refs, backend.KeyRef{
			Family: metadataFamilyForKey(mut.Key),
			Key:    cloneBytes(mut.Key),
		})
	}
	return refs
}

func cloneWatchEvents(events []backend.WatchEvent) []backend.WatchEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]backend.WatchEvent, 0, len(events))
	for _, event := range events {
		event.Key = cloneBytes(event.Key)
		out = append(out, event)
	}
	return out
}

func metadataCommandPrimary(mutations []*backend.Mutation) []byte {
	for _, mut := range mutations {
		if mut != nil && len(mut.Key) != 0 {
			return mut.Key
		}
	}
	return nil
}

func (e *Executor) commitWithoutMetadataPredicatesAndWatch(ctx context.Context, kind model.OperationKind, mount model.MountIdentity, primary []byte, mutations []*backend.Mutation, watchEvents []backend.WatchEvent, startVersion, commitVersion uint64) error {
	if stats := e.metadataPredicateCounters(kind); stats != nil {
		stats.skipTotal.Add(1)
	}
	return e.commitMetadataCommandWithWatch(ctx, mount, primary, nil, mutations, watchEvents, startVersion, commitVersion)
}

func (e *Executor) metadataPredicateCounters(kind model.OperationKind) *metadataPredicateCounters {
	if e == nil {
		return nil
	}
	return e.metadataPredicates[kind]
}

// GetReadVersion returns an ephemeral MVCC read version. It is intentionally
// cheaper than SnapshotSubtree: no root event is published and no GC-retention
// promise is made.
func (e *Executor) GetReadVersion(ctx context.Context, req model.ReadVersionRequest) (uint64, error) {
	if req.Mount == "" {
		return 0, model.ErrInvalidMountID
	}
	if err := e.requireActiveMount(ctx, req.Mount); err != nil {
		return 0, err
	}
	return e.reserveReadVersion(ctx)
}

func (e *Executor) reserveReadVersion(ctx context.Context) (uint64, error) {
	return e.reserveTimestampWithRetry(ctx, 1, maxReadContentionRetries, &e.readRetriesTotal, &e.readRetryExhaustedTotal)
}

func (e *Executor) readVersion(ctx context.Context, snapshotVersion uint64) (uint64, error) {
	if snapshotVersion != 0 {
		return snapshotVersion, nil
	}
	return e.reserveReadVersion(ctx)
}

// reserveCommitVersions reserves a read version plus a speculative commit version
// in one TSO hop. Metadata backends may use the speculative commit version or
// push the actual commit version after observing newer concurrent reads.
//
// When a path does use the speculative commit version, server-side safety nets
// keep pre-allocation from silently violating snapshot isolation:
//
//  1. Local metadata commits can push the chosen commit version past timestamps
//     reserved while the command was in flight.
//  2. Backends reject stale fixed commit versions with KindCommitTsExpired.
//
// Together these force a retry-with-fresh-ts under contention: incorrect
// speculative commit versions are detected at commit time, never silently
// accepted. CommitTsExpired is retried transparently by withCommitRetry below.
func (e *Executor) reserveCommitVersions(ctx context.Context) (uint64, uint64, error) {
	startVersion, err := e.reserveTimestampWithRetry(ctx, 2, maxCommitContentionRetries, &e.commitRetriesTotal, &e.commitRetryExhaustedTotal)
	if err != nil {
		return 0, 0, err
	}
	return startVersion, startVersion + 1, nil
}

func (e *Executor) reserveTimestampWithRetry(ctx context.Context, count uint64, maxRetries int, retryTotal, exhaustedTotal *atomic.Uint64) (uint64, error) {
	var last error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		version, err := e.runner.ReserveTimestamp(ctx, count)
		if err == nil {
			return version, nil
		}
		if !nokverrors.Retryable(err) {
			return 0, err
		}
		last = err
		if attempt == maxRetries {
			if exhaustedTotal != nil {
				exhaustedTotal.Add(1)
			}
			break
		}
		if retryTotal != nil {
			retryTotal.Add(1)
		}
		// Coordinator duty handoff can reject a timestamp request with stale
		// evidence before any fsmeta mutation has started. Retrying here keeps
		// transient authority churn below the namespace API boundary.
		if err := waitCommitContentionRetryDelay(ctx, commitContentionRetryDelay(attempt)); err != nil {
			return 0, err
		}
	}
	return 0, last
}

func (e *Executor) withCommitRetry(ctx context.Context, run func(startVersion, commitVersion uint64) error, scopes ...compile.AuthorityScope) error {
	return e.withCommitRetryLoop(ctx, run)
}

func (e *Executor) withCommitRetryLoop(ctx context.Context, run func(startVersion, commitVersion uint64) error) error {
	var last error
	started := time.Now()
	for attempt := 0; ; attempt++ {
		startVersion, commitVersion, err := e.reserveCommitVersions(ctx)
		if err != nil {
			return err
		}
		err = run(startVersion, commitVersion)
		if err == nil {
			return nil
		}
		if !isRetryableCommitAttempt(err) {
			return translateMutateError(err)
		}
		last = err
		if !canRetryCommitAttempt(attempt, started, err, e.lockTTL) {
			e.commitRetryExhaustedTotal.Add(1)
			break
		}
		// A live backend lock or a coordinator/region route refresh can race
		// with the same semantic fsmeta operation. Retrying at this boundary
		// keeps transient MVCC and route churn below the API contract.
		e.commitRetriesTotal.Add(1)
		delay := commitContentionRetryDelay(attempt)
		if budget := commitRetryBudget(err, e.lockTTL); budget > 0 {
			remaining := budget - time.Since(started)
			if remaining <= 0 {
				e.commitRetryExhaustedTotal.Add(1)
				break
			}
			delay = min(delay, remaining)
		}
		if err := waitCommitContentionRetryDelay(ctx, delay); err != nil {
			return err
		}
	}
	return translateMutateError(last)
}

func (e *Executor) withReadRetry(ctx context.Context, snapshotVersion uint64, run func(version uint64) error) error {
	var last error
	for attempt := 0; attempt <= maxReadContentionRetries; attempt++ {
		version, err := e.readVersion(ctx, snapshotVersion)
		if err != nil {
			return err
		}
		err = run(version)
		if err == nil {
			return nil
		}
		if !isRetryableReadAttempt(err) {
			return err
		}
		last = err
		if attempt == maxReadContentionRetries {
			e.readRetryExhaustedTotal.Add(1)
			break
		}
		// ReadDir and ReadDirPlus may race with live backend locks or region
		// route refresh. Retrying keeps the external API at the fsmeta level
		// instead of leaking transient storage details to callers.
		e.readRetriesTotal.Add(1)
		if err := waitCommitContentionRetryDelay(ctx, commitContentionRetryDelay(attempt)); err != nil {
			return err
		}
	}
	return last
}

func canRetryCommitAttempt(attempt int, started time.Time, err error, fallbackLockTTL uint64) bool {
	if budget := commitRetryBudget(err, fallbackLockTTL); budget > 0 {
		return time.Since(started) < budget
	}
	return attempt < maxCommitContentionRetries
}

func commitRetryBudget(err error, fallbackLockTTL uint64) time.Duration {
	switch {
	case nokverrors.IsKind(err, nokverrors.KindLockConflict):
	case nokverrors.IsKind(err, nokverrors.KindRetryable):
		// Replicated backends use Retryable for metadata-native revision
		// conflicts, for example when another command has committed at or above
		// the read version. The fsmeta semantic operation can safely re-read and
		// re-plan, but under raft/store congestion it needs the same bounded
		// liveness window as a visible live-lock wait.
	default:
		return 0
	}
	ttlMillis := commitLockTTLMillis(err)
	if ttlMillis == 0 {
		ttlMillis = fallbackLockTTL
	}
	if ttlMillis == 0 {
		return commitContentionRetryMaxBackoff
	}
	budget := time.Duration(ttlMillis) * time.Millisecond
	if budget <= 0 || budget > maxCommitLockRetryBudget {
		return maxCommitLockRetryBudget
	}
	return budget + commitContentionRetryMaxBackoff
}

func commitLockTTLMillis(err error) uint64 {
	var carrier nokverrors.KeyErrorCarrier
	if !errors.As(err, &carrier) {
		return 0
	}
	var maxTTL uint64
	for _, issue := range carrier.KeyErrors() {
		if ttl := issue.LockTTL; ttl > maxTTL {
			maxTTL = ttl
		}
	}
	return maxTTL
}

func commitContentionRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return commitContentionRetryBaseBackoff
	}
	if attempt >= 7 {
		return commitContentionRetryMaxBackoff
	}
	return min(commitContentionRetryBaseBackoff<<attempt, commitContentionRetryMaxBackoff)
}

func waitCommitContentionRetryDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}

func cloneMutations(in []*backend.Mutation) []*backend.Mutation {
	out := make([]*backend.Mutation, 0, len(in))
	for _, mut := range in {
		if mut == nil {
			out = append(out, nil)
			continue
		}
		out = append(out, &backend.Mutation{
			Family:            metadataFamilyForKey(mut.Key),
			Op:                mut.Op,
			Key:               cloneBytes(mut.Key),
			Value:             cloneBytes(mut.Value),
			AssertionNotExist: mut.AssertionNotExist,
			ExpiresAt:         mut.ExpiresAt,
		})
	}
	return out
}

func cloneMetadataPredicates(in []*backend.Predicate) []*backend.Predicate {
	out := make([]*backend.Predicate, 0, len(in))
	for _, pred := range in {
		if pred == nil {
			out = append(out, nil)
			continue
		}
		out = append(out, &backend.Predicate{
			Family:        metadataFamilyForKey(pred.Key),
			Key:           cloneBytes(pred.Key),
			Kind:          pred.Kind,
			ReadVersion:   pred.ReadVersion,
			ExpectedValue: cloneBytes(pred.ExpectedValue),
		})
	}
	return out
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

func translateMutateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, model.ErrExists) {
		return err
	}
	if nokverrors.IsKind(err, nokverrors.KindAlreadyExists) {
		return fmt.Errorf("%w: %v", model.ErrExists, err)
	}
	return err
}

func isRetryableCommitContention(err error) bool {
	return nokverrors.IsMetadataContention(err)
}

func isRetryableCommitAttempt(err error) bool {
	return isRetryableCommitContention(err) || isRetryableRouteRefresh(err)
}

func isRetryableReadAttempt(err error) bool {
	return isRetryableCommitContention(err) || isRetryableRouteRefresh(err)
}

func isRetryableRouteRefresh(err error) bool {
	switch nokverrors.KindOf(err) {
	case nokverrors.KindRouteUnavailable,
		nokverrors.KindRegionRouting,
		nokverrors.KindStaleEpoch:
		return true
	default:
		return false
	}
}

func metadataNotExistsPredicate(key []byte) *backend.Predicate {
	return &backend.Predicate{Key: cloneBytes(key), Kind: backend.PredicateNotExists}
}

func metadataPrefixEmptyPredicate(prefix []byte) *backend.Predicate {
	return &backend.Predicate{Key: cloneBytes(prefix), Kind: backend.PredicatePrefixEmpty}
}

func metadataValueEqualsPredicate(key, value []byte) *backend.Predicate {
	return &backend.Predicate{
		Key:           cloneBytes(key),
		Kind:          backend.PredicateValueEquals,
		ExpectedValue: cloneBytes(value),
	}
}
