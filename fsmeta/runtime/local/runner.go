// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	cpebble "github.com/cockroachdb/pebble"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

const (
	localWriteKeyPrefix byte = 'w'
	localMaxVersion          = ^uint64(0)

	localWritePut    byte = 1
	localWriteDelete byte = 2
)

// Runner adapts a Pebble DB to backend.Store with one-node MVCC commits.
type Runner struct {
	db        *cpebble.DB
	writeOpts *cpebble.WriteOptions

	mu               sync.Mutex
	nextTS           uint64
	latestObservedTS uint64
	observer         mutationObserver

	atomicMutateTotal            atomic.Uint64
	atomicPredicateRejectedTotal atomic.Uint64
	mutateTotal                  atomic.Uint64
}

type mutationObserver interface {
	ObserveMutation(commitVersion uint64, mutations []*backend.Mutation)
}

type localWrite struct {
	Kind         byte
	StartVersion uint64
	ExpiresAt    uint64
	Value        []byte
}

// NewRunner constructs a local fsmeta backend over a Pebble DB.
func NewRunner(db *cpebble.DB) (*Runner, error) {
	if db == nil {
		return nil, errDBRequired
	}
	r := &Runner{db: db, writeOpts: cpebble.Sync}
	maxVersion, err := r.maxObservedVersion()
	if err != nil {
		return nil, err
	}
	r.nextTS = maxVersion + 1
	r.latestObservedTS = maxVersion
	return r, nil
}

// SetMutationObserver attaches a local runtime observer that is called after a
// mutation group is durably applied.
func (r *Runner) SetMutationObserver(observer mutationObserver) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.observer = observer
	r.mu.Unlock()
}

// ReserveTimestamp reserves count consecutive local MVCC timestamps.
func (r *Runner) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errTimestampCount
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	first := r.nextTS
	r.nextTS += count
	last := first + count - 1
	if last > r.latestObservedTS {
		r.latestObservedTS = last
	}
	return first, nil
}

// Get returns the value visible at version.
func (r *Runner) Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	value, ok, err := r.readValue(key, version)
	if err != nil {
		return nil, false, err
	}
	return value, ok, nil
}

// BatchGet returns found values visible at version, keyed by string(key).
func (r *Runner) BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if err := ctxErr(ctx); err != nil {
			return nil, err
		}
		value, ok, err := r.readValue(key, version)
		if err != nil {
			return nil, err
		}
		if ok {
			out[string(key)] = value
		}
	}
	return out, nil
}

// Scan returns up to limit visible key/value pairs starting at startKey.
func (r *Runner) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]backend.KV, error) {
	if limit == 0 {
		return nil, nil
	}
	out := make([]backend.KV, 0, limit)
	err := r.scanUserKeys(startKey, func(userKey []byte) (bool, error) {
		if err := ctxErr(ctx); err != nil {
			return false, err
		}
		value, ok, err := r.readValue(userKey, version)
		if err != nil {
			return false, err
		}
		if ok {
			out = append(out, backend.KV{Key: cloneBytes(userKey), Value: value})
		}
		return uint32(len(out)) < limit, nil
	})
	return out, err
}

// Mutate commits all mutations atomically at the effective local commit
// timestamp. The effective timestamp may move forward when another timestamp
// was observed after the caller reserved its speculative commit timestamp.
func (r *Runner) Mutate(ctx context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion, _ uint64) (uint64, error) {
	return r.mutate(ctx, primary, mutations, startVersion, commitVersion, true)
}

// MutateAtCommit commits all mutations exactly at commitVersion.
func (r *Runner) MutateAtCommit(ctx context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion, _ uint64) (uint64, error) {
	return r.mutate(ctx, primary, mutations, startVersion, commitVersion, false)
}

// InstallMutationsAtCommit applies a segment-install mutation group at
// commitVersion. Local demo mode stores every metadata value in the write
// record, so installs are one Pebble batch with no separate intent table.
func (r *Runner) InstallMutationsAtCommit(ctx context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion uint64) (uint64, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	if commitVersion <= startVersion {
		return 0, txnAbort(errCommitVersion)
	}
	effectiveCommitVersion, observer, err := r.applyMutationGroup(primary, mutations, startVersion, commitVersion, false, true)
	if err != nil {
		return 0, err
	}
	if observer != nil {
		observer.ObserveMutation(effectiveCommitVersion, mutations)
	}
	return effectiveCommitVersion, nil
}

// AtomicMutatePreservesReadOrder reports that TryAtomicMutate validates
// predicates and applies writes under the same local mutex.
func (r *Runner) AtomicMutatePreservesReadOrder() bool {
	return r != nil
}

// TryAtomicMutate applies predicate-checked mutations through one Pebble batch.
func (r *Runner) TryAtomicMutate(ctx context.Context, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, startVersion, commitVersion uint64) (bool, error) {
	if err := ctxErr(ctx); err != nil {
		return true, err
	}
	if commitVersion <= startVersion {
		return true, txnAbort(errCommitVersion)
	}
	effectiveCommitVersion, observer, handled, err := r.applyAtomicMutationGroup(primary, predicates, mutations, startVersion, commitVersion)
	if err != nil || !handled {
		return handled, err
	}
	if observer != nil {
		observer.ObserveMutation(effectiveCommitVersion, mutations)
	}
	return true, nil
}

// Stats returns local runner diagnostics.
func (r *Runner) Stats() map[string]any {
	if r == nil {
		return map[string]any{
			"next_timestamp":                  uint64(0),
			"latest_observed":                 uint64(0),
			"mutate_total":                    uint64(0),
			"atomic_mutate_total":             uint64(0),
			"atomic_predicate_rejected_total": uint64(0),
			"storage":                         "pebble",
		}
	}
	r.mu.Lock()
	next := r.nextTS
	observed := r.latestObservedTS
	r.mu.Unlock()
	return map[string]any{
		"next_timestamp":                  next,
		"latest_observed":                 observed,
		"mutate_total":                    r.mutateTotal.Load(),
		"atomic_mutate_total":             r.atomicMutateTotal.Load(),
		"atomic_predicate_rejected_total": r.atomicPredicateRejectedTotal.Load(),
		"storage":                         "pebble",
	}
}

func (r *Runner) mutate(ctx context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion uint64, allowCommitPush bool) (uint64, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	if commitVersion <= startVersion {
		return 0, txnAbort(errCommitVersion)
	}
	effectiveCommitVersion, observer, err := r.applyMutationGroup(primary, mutations, startVersion, commitVersion, allowCommitPush, false)
	if err != nil {
		return 0, err
	}
	if observer != nil {
		observer.ObserveMutation(effectiveCommitVersion, mutations)
	}
	return effectiveCommitVersion, nil
}

func (r *Runner) applyMutationGroup(primary []byte, mutations []*backend.Mutation, startVersion, commitVersion uint64, allowCommitPush bool, allowMissingDeletePrimary bool) (uint64, mutationObserver, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	effectiveCommitVersion := r.reserveMutationCommitVersionLocked(commitVersion, allowCommitPush)
	if err := r.validateMutationsLocked(primary, mutations, startVersion, commitVersion, allowMissingDeletePrimary); err != nil {
		return 0, nil, err
	}
	if err := r.applyMutationBatchLocked(mutations, startVersion, effectiveCommitVersion); err != nil {
		return 0, nil, err
	}
	return effectiveCommitVersion, r.completeMutationApplyLocked(effectiveCommitVersion, false), nil
}

func (r *Runner) applyAtomicMutationGroup(primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, startVersion, commitVersion uint64) (uint64, mutationObserver, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if applied, err := r.atomicMutationAlreadyAppliedLocked(mutations, startVersion, commitVersion); err != nil {
		return 0, nil, true, txnRetryable(err)
	} else if applied {
		r.recordAtomicMutationMetric()
		return commitVersion, r.observer, true, nil
	}
	if err := r.validateAtomicPredicatesLocked(predicates, startVersion); err != nil {
		r.atomicPredicateRejectedTotal.Add(1)
		return 0, nil, true, err
	}
	if err := r.validateMutationsLocked(primary, mutations, startVersion, commitVersion, false); err != nil {
		return 0, nil, true, err
	}
	if err := r.applyMutationBatchLocked(mutations, startVersion, commitVersion); err != nil {
		return 0, nil, true, err
	}
	return commitVersion, r.completeMutationApplyLocked(commitVersion, true), true, nil
}

func (r *Runner) reserveMutationCommitVersionLocked(commitVersion uint64, allowCommitPush bool) uint64 {
	if !allowCommitPush {
		return commitVersion
	}
	effective := commitVersion
	if r.latestObservedTS >= effective {
		effective = r.latestObservedTS + 1
	}
	if r.nextTS > effective {
		effective = r.nextTS
	}
	if effective > r.latestObservedTS {
		r.latestObservedTS = effective
	}
	if r.nextTS <= effective {
		r.nextTS = effective + 1
	}
	return effective
}

func (r *Runner) completeMutationApplyLocked(commitVersion uint64, atomic bool) mutationObserver {
	r.recordMutationMetrics(atomic)
	if commitVersion > r.latestObservedTS {
		r.latestObservedTS = commitVersion
	}
	if r.nextTS <= commitVersion {
		r.nextTS = commitVersion + 1
	}
	return r.observer
}

func (r *Runner) validateAtomicPredicatesLocked(predicates []*backend.Predicate, startVersion uint64) error {
	for _, pred := range predicates {
		if pred == nil || len(pred.Key) == 0 {
			return txnAbort(errInvalidAtomicMutate)
		}
		readVersion := pred.ReadVersion
		if readVersion == 0 {
			readVersion = startVersion
		}
		value, exists, err := r.readValueLocked(pred.Key, readVersion)
		if err != nil {
			return txnRetryable(err)
		}
		switch pred.Kind {
		case backend.PredicateNotExists:
			if exists {
				return txnAlreadyExists(pred.Key)
			}
		case backend.PredicateExists:
			if !exists {
				return txnAbort(errInvalidAtomicMutate)
			}
		case backend.PredicateValueEquals:
			if !exists || !bytes.Equal(value, pred.ExpectedValue) {
				return txnRetryable(errAtomicPredicate)
			}
		default:
			return txnAbort(errInvalidAtomicMutate)
		}
	}
	return nil
}

func (r *Runner) atomicMutationAlreadyAppliedLocked(mutations []*backend.Mutation, startVersion, commitVersion uint64) (bool, error) {
	anyPresent := false
	allPresent := true
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		write, foundCommit, found, err := r.writeByStartVersionLocked(mut.Key, startVersion)
		if err != nil {
			return false, err
		}
		if !found {
			allPresent = false
			continue
		}
		anyPresent = true
		expectedKind, err := backendMutationKind(mut.Op)
		if err != nil {
			return false, err
		}
		if foundCommit != commitVersion || write.Kind != expectedKind {
			return false, nil
		}
		if mut.Op == backend.MutationPut && (!bytes.Equal(write.Value, mut.Value) || write.ExpiresAt != mut.ExpiresAt) {
			return false, nil
		}
	}
	return anyPresent && allPresent, nil
}

func (r *Runner) validateMutationsLocked(primary []byte, mutations []*backend.Mutation, startVersion, commitVersion uint64, allowMissingDeletePrimary bool) error {
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		key := mut.Key
		if len(key) == 0 {
			return txnAbort(errEmptyMutationKey)
		}
		latest, ok, err := r.latestWriteVersionLocked(key)
		if err != nil {
			return txnRetryable(err)
		}
		if ok && latest > startVersion {
			return txnCommitExpired(key, commitVersion, latest+1)
		}
		if mut.AssertionNotExist {
			if _, ok, err := r.readValueLocked(key, startVersion); err != nil {
				return txnRetryable(err)
			} else if ok {
				return txnAlreadyExists(key)
			}
			if _, ok, err := r.readValueLocked(key, localMaxVersion); err != nil {
				return txnRetryable(err)
			} else if ok {
				return txnAlreadyExists(key)
			}
		}
		if bytes.Equal(key, primary) && mut.Op == backend.MutationDelete && !allowMissingDeletePrimary {
			if _, ok, err := r.readValueLocked(key, localMaxVersion); err != nil {
				return txnRetryable(err)
			} else if !ok {
				return txnKeyError(&kvrpcpb.KeyError{Retryable: errKeyNotFound.Error()})
			}
		}
		switch mut.Op {
		case backend.MutationPut, backend.MutationDelete:
		default:
			return txnUnsupportedMutation(mut.Op)
		}
	}
	return nil
}

func (r *Runner) applyMutationBatchLocked(mutations []*backend.Mutation, startVersion, commitVersion uint64) error {
	batch := r.db.NewBatch()
	defer func() { _ = batch.Close() }()
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		write, err := writeForMutation(mut, startVersion)
		if err != nil {
			return err
		}
		if err := batch.Set(encodeLocalWriteKey(mut.Key, commitVersion), encodeLocalWrite(write), nil); err != nil {
			return err
		}
	}
	return batch.Commit(r.writeOpts)
}

func (r *Runner) readValue(key []byte, readVersion uint64) ([]byte, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readValueLocked(key, readVersion)
}

func (r *Runner) readValueLocked(key []byte, readVersion uint64) ([]byte, bool, error) {
	write, ok, err := r.writeForReadLocked(key, readVersion)
	if err != nil || !ok {
		return nil, false, err
	}
	if write.Kind == localWriteDelete {
		return nil, false, nil
	}
	if write.ExpiresAt > 0 && write.ExpiresAt <= uint64(time.Now().Unix()) {
		return nil, false, nil
	}
	return cloneBytes(write.Value), true, nil
}

func (r *Runner) writeForReadLocked(key []byte, readVersion uint64) (localWrite, bool, error) {
	iter, err := r.db.NewIter(nil)
	if err != nil {
		return localWrite{}, false, err
	}
	defer func() { _ = iter.Close() }()
	seek := encodeLocalWriteKey(key, readVersion)
	for valid := iter.SeekGE(seek); valid; valid = iter.Next() {
		prefix, userKey, commitVersion, ok := decodeLocalVersionedKey(iter.Key())
		if !ok || prefix != localWriteKeyPrefix || !bytes.Equal(userKey, key) {
			break
		}
		if commitVersion > readVersion {
			continue
		}
		raw, err := iter.ValueAndErr()
		if err != nil {
			return localWrite{}, false, err
		}
		write, err := decodeLocalWrite(raw)
		if err != nil {
			return localWrite{}, false, err
		}
		return write, true, nil
	}
	return localWrite{}, false, iter.Error()
}

func (r *Runner) latestWriteVersionLocked(key []byte) (uint64, bool, error) {
	var latest uint64
	var found bool
	err := r.scanWritesLocked(key, func(_ localWrite, commitVersion uint64) bool {
		if !found || commitVersion > latest {
			latest = commitVersion
			found = true
		}
		return false
	})
	return latest, found, err
}

func (r *Runner) writeByStartVersionLocked(key []byte, startVersion uint64) (localWrite, uint64, bool, error) {
	var foundWrite localWrite
	var foundCommit uint64
	var found bool
	err := r.scanWritesLocked(key, func(write localWrite, commitVersion uint64) bool {
		if write.StartVersion != startVersion {
			return true
		}
		foundWrite = write
		foundCommit = commitVersion
		found = true
		return false
	})
	return foundWrite, foundCommit, found, err
}

func (r *Runner) scanWritesLocked(key []byte, fn func(localWrite, uint64) bool) error {
	iter, err := r.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()
	for valid := iter.SeekGE(encodeLocalWriteKey(key, localMaxVersion)); valid; valid = iter.Next() {
		prefix, userKey, commitVersion, ok := decodeLocalVersionedKey(iter.Key())
		if !ok || prefix != localWriteKeyPrefix || !bytes.Equal(userKey, key) {
			break
		}
		raw, err := iter.ValueAndErr()
		if err != nil {
			return err
		}
		write, err := decodeLocalWrite(raw)
		if err != nil {
			return err
		}
		if !fn(write, commitVersion) {
			break
		}
	}
	return iter.Error()
}

func (r *Runner) scanUserKeys(startKey []byte, fn func([]byte) (bool, error)) error {
	r.mu.Lock()
	iter, err := r.db.NewIter(nil)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	var keys [][]byte
	var lastUserKey []byte
	for valid := iter.SeekGE(encodeLocalWriteKey(startKey, localMaxVersion)); valid; valid = iter.Next() {
		prefix, userKey, _, ok := decodeLocalVersionedKey(iter.Key())
		if !ok || prefix != localWriteKeyPrefix {
			break
		}
		if bytes.Equal(userKey, lastUserKey) {
			continue
		}
		lastUserKey = cloneBytes(userKey)
		keys = append(keys, cloneBytes(userKey))
	}
	err = iter.Error()
	if closeErr := iter.Close(); err == nil {
		err = closeErr
	}
	r.mu.Unlock()
	if err != nil {
		return err
	}
	for _, key := range keys {
		cont, err := fn(key)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (r *Runner) maxObservedVersion() (uint64, error) {
	iter, err := r.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()
	var maxVersion uint64
	for valid := iter.First(); valid; valid = iter.Next() {
		prefix, _, version, ok := decodeLocalVersionedKey(iter.Key())
		if !ok || prefix != localWriteKeyPrefix {
			continue
		}
		if version != localMaxVersion && version > maxVersion {
			maxVersion = version
		}
	}
	return maxVersion, iter.Error()
}

func writeForMutation(mut *backend.Mutation, startVersion uint64) (localWrite, error) {
	switch mut.Op {
	case backend.MutationPut:
		return localWrite{
			Kind:         localWritePut,
			StartVersion: startVersion,
			ExpiresAt:    mut.ExpiresAt,
			Value:        cloneBytes(mut.Value),
		}, nil
	case backend.MutationDelete:
		return localWrite{Kind: localWriteDelete, StartVersion: startVersion}, nil
	default:
		return localWrite{}, txnUnsupportedMutation(mut.Op)
	}
}

func backendMutationKind(op backend.MutationOp) (byte, error) {
	switch op {
	case backend.MutationPut:
		return localWritePut, nil
	case backend.MutationDelete:
		return localWriteDelete, nil
	default:
		return localWritePut, txnUnsupportedMutation(op)
	}
}

func encodeLocalWrite(write localWrite) []byte {
	out := make([]byte, 1+8+8+binary.MaxVarintLen64+len(write.Value))
	out[0] = write.Kind
	binary.BigEndian.PutUint64(out[1:9], write.StartVersion)
	binary.BigEndian.PutUint64(out[9:17], write.ExpiresAt)
	n := binary.PutUvarint(out[17:], uint64(len(write.Value)))
	out = out[:17+n]
	out = append(out, write.Value...)
	return out
}

func decodeLocalWrite(src []byte) (localWrite, error) {
	if len(src) < 17 {
		return localWrite{}, errInvalidInternalEntry
	}
	valueLen, n := binary.Uvarint(src[17:])
	if n <= 0 {
		return localWrite{}, errInvalidInternalEntry
	}
	valueStart := 17 + n
	if uint64(len(src)-valueStart) != valueLen {
		return localWrite{}, errInvalidInternalEntry
	}
	return localWrite{
		Kind:         src[0],
		StartVersion: binary.BigEndian.Uint64(src[1:9]),
		ExpiresAt:    binary.BigEndian.Uint64(src[9:17]),
		Value:        cloneBytes(src[valueStart:]),
	}, nil
}

func encodeLocalWriteKey(userKey []byte, version uint64) []byte {
	out := make([]byte, 0, 1+len(userKey)+2+8)
	out = append(out, localWriteKeyPrefix)
	out = appendEscapedKey(out, userKey)
	out = append(out, 0, 0)
	var suffix [8]byte
	binary.BigEndian.PutUint64(suffix[:], ^version)
	out = append(out, suffix[:]...)
	return out
}

func appendEscapedKey(out []byte, key []byte) []byte {
	for _, b := range key {
		if b == 0 {
			out = append(out, 0, 1)
			continue
		}
		out = append(out, b)
	}
	return out
}

func decodeLocalVersionedKey(key []byte) (byte, []byte, uint64, bool) {
	if len(key) < 1+2+8 {
		return 0, nil, 0, false
	}
	prefix := key[0]
	body := key[1 : len(key)-8]
	userKey := make([]byte, 0, len(body))
	for i := 0; i < len(body); i++ {
		b := body[i]
		if b != 0 {
			userKey = append(userKey, b)
			continue
		}
		if i+1 >= len(body) {
			return 0, nil, 0, false
		}
		next := body[i+1]
		switch next {
		case 0:
			if i+2 != len(body) {
				return 0, nil, 0, false
			}
			version := ^binary.BigEndian.Uint64(key[len(key)-8:])
			return prefix, userKey, version, true
		case 1:
			userKey = append(userKey, 0)
			i++
		default:
			return 0, nil, 0, false
		}
	}
	return 0, nil, 0, false
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	return append([]byte(nil), src...)
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
