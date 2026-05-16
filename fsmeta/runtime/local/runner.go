// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	localdb "github.com/feichai0017/NoKV/local"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/txn/mvcc"
	"github.com/feichai0017/NoKV/utils"
)

// Runner adapts local.DB to fsmetaexec.TxnRunner with single-node MVCC commits.
type Runner struct {
	db *localdb.DB

	mu               sync.Mutex
	nextTS           uint64
	latestObservedTS uint64
	observer         mutationObserver

	atomicMutateTotal            atomic.Uint64
	atomicPredicateRejectedTotal atomic.Uint64
	atomicRejectedTotal          atomic.Uint64
	mutateTotal                  atomic.Uint64
}

type mutationObserver interface {
	ObserveMutation(commitVersion uint64, mutations []*kvrpcpb.Mutation)
}

// NewRunner constructs a local fsmeta TxnRunner.
func NewRunner(db *localdb.DB) (*Runner, error) {
	if db == nil {
		return nil, errDBRequired
	}
	maxVersion, err := maxObservedVersion(db)
	if err != nil {
		return nil, err
	}
	return &Runner{
		db:               db,
		nextTS:           maxVersion + 1,
		latestObservedTS: maxVersion,
	}, nil
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
func (r *Runner) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]fsmetaexec.KV, error) {
	if limit == 0 {
		return nil, nil
	}
	iter := r.db.NewInternalIterator(&index.Options{IsAsc: true})
	if iter == nil {
		return nil, nil
	}
	defer func() { _ = iter.Close() }()
	out := make([]fsmetaexec.KV, 0, limit)
	var lastUserKey []byte
	iter.Seek(kv.InternalKey(kv.CFWrite, startKey, kv.MaxVersion))
	for iter.Valid() && uint32(len(out)) < limit {
		if err := ctxErr(ctx); err != nil {
			return nil, err
		}
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		cf, userKey, _, ok := kv.SplitInternalKey(item.Entry().Key)
		if !ok {
			return nil, errInvalidInternalEntry
		}
		if cf != kv.CFWrite {
			break
		}
		if bytes.Compare(userKey, startKey) < 0 || bytes.Equal(userKey, lastUserKey) {
			iter.Next()
			continue
		}
		lastUserKey = cloneBytes(userKey)
		value, ok, err := r.readValue(userKey, version)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, fsmetaexec.KV{Key: cloneBytes(userKey), Value: value})
		}
		iter.Next()
	}
	return out, nil
}

// Mutate commits all mutations atomically at the effective local commit
// timestamp. The effective timestamp may move forward when another timestamp
// was observed after the caller reserved its speculative commit timestamp.
func (r *Runner) Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, _ uint64) (uint64, error) {
	return r.mutate(ctx, primary, mutations, startVersion, commitVersion, true)
}

// MutateAtCommit commits all mutations exactly at commitVersion.
func (r *Runner) MutateAtCommit(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, _ uint64) (uint64, error) {
	return r.mutate(ctx, primary, mutations, startVersion, commitVersion, false)
}

// TryAtomicMutate applies predicate-checked mutations through the same local
// atomic apply group used by Mutate. handled=false means the caller-owned DB is
// sharded in a way that cannot preserve group atomicity for this key set.
func (r *Runner) TryAtomicMutate(ctx context.Context, primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (bool, error) {
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
			"next_timestamp":                    uint64(0),
			"latest_observed":                   uint64(0),
			"mutate_total":                      uint64(0),
			"atomic_mutate_total":               uint64(0),
			"atomic_predicate_rejected_total":   uint64(0),
			"atomic_apply_group_rejected_total": uint64(0),
		}
	}
	r.mu.Lock()
	next := r.nextTS
	observed := r.latestObservedTS
	r.mu.Unlock()
	return map[string]any{
		"next_timestamp":                    next,
		"latest_observed":                   observed,
		"mutate_total":                      r.mutateTotal.Load(),
		"atomic_mutate_total":               r.atomicMutateTotal.Load(),
		"atomic_predicate_rejected_total":   r.atomicPredicateRejectedTotal.Load(),
		"atomic_apply_group_rejected_total": r.atomicRejectedTotal.Load(),
	}
}

func (r *Runner) mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64, allowCommitPush bool) (uint64, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	if commitVersion <= startVersion {
		return 0, txnAbort(errCommitVersion)
	}
	effectiveCommitVersion, observer, err := r.applyMutationGroup(primary, mutations, startVersion, commitVersion, allowCommitPush)
	if err != nil {
		return 0, err
	}
	if observer != nil {
		observer.ObserveMutation(effectiveCommitVersion, mutations)
	}
	return effectiveCommitVersion, nil
}

func (r *Runner) applyMutationGroup(primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64, allowCommitPush bool) (uint64, mutationObserver, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	effectiveCommitVersion := commitVersion
	if allowCommitPush && r.latestObservedTS >= effectiveCommitVersion {
		effectiveCommitVersion = r.latestObservedTS + 1
		if r.nextTS <= effectiveCommitVersion {
			r.nextTS = effectiveCommitVersion + 1
		}
	}
	if err := r.validateMutations(primary, mutations, startVersion, commitVersion); err != nil {
		return 0, nil, err
	}
	entries, err := buildMutationEntries(mutations, startVersion, effectiveCommitVersion)
	if err != nil {
		return 0, nil, err
	}
	defer releaseEntries(entries)
	if !r.db.CanApplyInternalEntriesAtomically(entries) {
		r.atomicRejectedTotal.Add(1)
		return 0, nil, errNonAtomicApplyGroup
	}
	if err := r.db.ApplyInternalEntries(entries); err != nil {
		return 0, nil, err
	}
	r.mutateTotal.Add(1)
	if effectiveCommitVersion > r.latestObservedTS {
		r.latestObservedTS = effectiveCommitVersion
	}
	if r.nextTS <= effectiveCommitVersion {
		r.nextTS = effectiveCommitVersion + 1
	}
	return effectiveCommitVersion, r.observer, nil
}

func (r *Runner) applyAtomicMutationGroup(primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (uint64, mutationObserver, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if applied, err := r.atomicMutationAlreadyApplied(mutations, startVersion, commitVersion); err != nil {
		return 0, nil, true, txnRetryable(err)
	} else if applied {
		r.atomicMutateTotal.Add(1)
		return commitVersion, r.observer, true, nil
	}
	if err := r.validateAtomicPredicates(predicates, startVersion); err != nil {
		r.atomicPredicateRejectedTotal.Add(1)
		return 0, nil, true, err
	}
	if err := r.validateMutations(primary, mutations, startVersion, commitVersion); err != nil {
		return 0, nil, true, err
	}
	entries, err := buildMutationEntries(mutations, startVersion, commitVersion)
	if err != nil {
		return 0, nil, true, err
	}
	defer releaseEntries(entries)
	if !r.db.CanApplyInternalEntriesAtomically(entries) {
		r.atomicRejectedTotal.Add(1)
		return 0, nil, false, nil
	}
	if err := r.db.ApplyInternalEntries(entries); err != nil {
		return 0, nil, true, err
	}
	r.atomicMutateTotal.Add(1)
	r.mutateTotal.Add(1)
	if commitVersion > r.latestObservedTS {
		r.latestObservedTS = commitVersion
	}
	if r.nextTS <= commitVersion {
		r.nextTS = commitVersion + 1
	}
	return commitVersion, r.observer, true, nil
}

func (r *Runner) validateAtomicPredicates(predicates []*kvrpcpb.AtomicPredicate, startVersion uint64) error {
	for _, pred := range predicates {
		if pred == nil || len(pred.GetKey()) == 0 {
			return txnAbort(errInvalidAtomicMutate)
		}
		readVersion := pred.GetReadVersion()
		if readVersion == 0 {
			readVersion = startVersion
		}
		value, exists, err := r.readValue(pred.GetKey(), readVersion)
		if err != nil {
			return txnRetryable(err)
		}
		switch pred.GetKind() {
		case kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS:
			if exists {
				return txnAlreadyExists(pred.GetKey())
			}
		case kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_EXISTS:
			if !exists {
				return txnAbort(errInvalidAtomicMutate)
			}
		case kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS:
			if !exists || !bytes.Equal(value, pred.GetExpectedValue()) {
				return txnRetryable(errAtomicPredicate)
			}
		default:
			return txnAbort(errInvalidAtomicMutate)
		}
	}
	return nil
}

func (r *Runner) atomicMutationAlreadyApplied(mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (bool, error) {
	anyPresent := false
	allPresent := true
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		write, foundCommit, found, err := r.writeByStartVersion(mut.GetKey(), startVersion)
		if err != nil {
			return false, err
		}
		if !found {
			allPresent = false
			continue
		}
		anyPresent = true
		if foundCommit != commitVersion || write.Kind != mut.GetOp() {
			return false, nil
		}
		if mut.GetOp() == kvrpcpb.Mutation_Put {
			matches, err := r.committedPutMatches(write, mut, startVersion)
			if err != nil || !matches {
				return false, err
			}
		}
	}
	return anyPresent && allPresent, nil
}

func (r *Runner) validateMutations(primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) error {
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		key := mut.GetKey()
		if len(key) == 0 {
			return txnAbort(errEmptyMutationKey)
		}
		latest, ok, err := r.latestWriteVersion(key)
		if err != nil {
			return txnRetryable(err)
		}
		if ok && latest > startVersion {
			return txnCommitExpired(key, commitVersion, latest+1)
		}
		if mut.GetAssertionNotExist() {
			if _, ok, err := r.readValue(key, startVersion); err != nil {
				return txnRetryable(err)
			} else if ok {
				return txnAlreadyExists(key)
			}
			if _, ok, err := r.readValue(key, kv.MaxVersion); err != nil {
				return txnRetryable(err)
			} else if ok {
				return txnAlreadyExists(key)
			}
		}
		if bytes.Equal(key, primary) && mut.GetOp() == kvrpcpb.Mutation_Delete {
			if _, ok, err := r.readValue(key, kv.MaxVersion); err != nil {
				return txnRetryable(err)
			} else if !ok {
				return txnKeyError(&kvrpcpb.KeyError{Retryable: utils.ErrKeyNotFound.Error()})
			}
		}
		switch mut.GetOp() {
		case kvrpcpb.Mutation_Put, kvrpcpb.Mutation_Delete:
		default:
			return txnUnsupportedMutation(mut.GetOp())
		}
	}
	return nil
}

func (r *Runner) readValue(key []byte, readVersion uint64) ([]byte, bool, error) {
	write, ok, err := r.writeForRead(key, readVersion)
	if err != nil || !ok {
		return nil, false, err
	}
	switch write.Kind {
	case kvrpcpb.Mutation_Delete, kvrpcpb.Mutation_Rollback:
		return nil, false, nil
	}
	if len(write.ShortValue) > 0 {
		if write.ExpiresAt > 0 && write.ExpiresAt <= uint64(time.Now().Unix()) {
			return nil, false, nil
		}
		return cloneBytes(write.ShortValue), true, nil
	}
	entry, err := r.db.GetInternalEntry(kv.CFDefault, key, write.StartTs)
	if err != nil {
		if errors.Is(err, utils.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer entry.DecrRef()
	if entry.IsDeletedOrExpired() {
		return nil, false, nil
	}
	return cloneBytes(entry.Value), true, nil
}

func (r *Runner) writeForRead(key []byte, readVersion uint64) (mvcc.Write, bool, error) {
	var (
		best      mvcc.Write
		bestTS    uint64
		bestFound bool
	)
	err := r.scanWrites(key, func(write mvcc.Write, commitTS uint64) bool {
		if commitTS <= readVersion && (write.Kind == kvrpcpb.Mutation_Lock || write.Kind == kvrpcpb.Mutation_Rollback) {
			return true
		}
		if commitTS <= readVersion && (!bestFound || commitTS > bestTS) {
			best = write
			bestTS = commitTS
			bestFound = true
		}
		return true
	})
	return best, bestFound, err
}

func (r *Runner) latestWriteVersion(key []byte) (uint64, bool, error) {
	var (
		latest uint64
		found  bool
	)
	err := r.scanWrites(key, func(_ mvcc.Write, commitTS uint64) bool {
		if !found || commitTS > latest {
			latest = commitTS
			found = true
		}
		return true
	})
	return latest, found, err
}

func (r *Runner) writeByStartVersion(key []byte, startVersion uint64) (mvcc.Write, uint64, bool, error) {
	var (
		foundWrite  mvcc.Write
		foundCommit uint64
		found       bool
	)
	err := r.scanWrites(key, func(write mvcc.Write, commitTS uint64) bool {
		if write.StartTs != startVersion {
			return true
		}
		foundWrite = write
		foundCommit = commitTS
		found = true
		return false
	})
	return foundWrite, foundCommit, found, err
}

func (r *Runner) committedPutMatches(write mvcc.Write, mut *kvrpcpb.Mutation, startVersion uint64) (bool, error) {
	if len(write.ShortValue) > 0 {
		return bytes.Equal(write.ShortValue, mut.GetValue()) && write.ExpiresAt == mut.GetExpiresAt(), nil
	}
	entry, err := r.db.GetInternalEntry(kv.CFDefault, mut.GetKey(), startVersion)
	if err != nil {
		if errors.Is(err, utils.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	defer entry.DecrRef()
	if entry.IsDeletedOrExpired() {
		return false, nil
	}
	return bytes.Equal(entry.Value, mut.GetValue()) && entry.ExpiresAt == mut.GetExpiresAt(), nil
}

func (r *Runner) scanWrites(key []byte, fn func(mvcc.Write, uint64) bool) error {
	iter := r.db.NewInternalIterator(&index.Options{IsAsc: true})
	if iter == nil {
		return nil
	}
	defer func() { _ = iter.Close() }()
	iter.Seek(kv.InternalKey(kv.CFWrite, key, kv.MaxVersion))
	for iter.Valid() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		cf, userKey, ts, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			return errInvalidInternalEntry
		}
		if cf != kv.CFWrite || !bytes.Equal(userKey, key) {
			break
		}
		if entry.Meta&kv.BitDelete == 0 {
			write, err := mvcc.DecodeWrite(entry.Value)
			if err != nil {
				return err
			}
			if !fn(write, ts) {
				break
			}
		}
		iter.Next()
	}
	return nil
}

type versionedOp struct {
	cf      kv.ColumnFamily
	key     []byte
	version uint64
	value   []byte
	meta    byte
	expires uint64
}

func buildMutationEntries(mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) ([]*kv.Entry, error) {
	ops := make([]versionedOp, 0, len(mutations)*3)
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		switch mut.GetOp() {
		case kvrpcpb.Mutation_Put:
			write := mvcc.Write{Kind: mut.GetOp(), StartTs: startVersion}
			if mvcc.CanInlineShortValue(mut.GetOp(), mut.GetValue()) {
				write.ShortValue = cloneBytes(mut.GetValue())
				write.ExpiresAt = mut.GetExpiresAt()
				ops = append(ops, versionedOp{
					cf:      kv.CFWrite,
					key:     mut.GetKey(),
					version: commitVersion,
					value:   mvcc.EncodeWrite(write),
				})
				continue
			}
			ops = append(ops,
				versionedOp{cf: kv.CFDefault, key: mut.GetKey(), version: startVersion, meta: kv.BitDelete},
				versionedOp{cf: kv.CFDefault, key: mut.GetKey(), version: startVersion, value: mut.GetValue(), expires: mut.GetExpiresAt()},
				versionedOp{cf: kv.CFWrite, key: mut.GetKey(), version: commitVersion, value: mvcc.EncodeWrite(write)},
			)
		case kvrpcpb.Mutation_Delete:
			ops = append(ops,
				versionedOp{cf: kv.CFDefault, key: mut.GetKey(), version: startVersion, meta: kv.BitDelete},
				versionedOp{
					cf:      kv.CFWrite,
					key:     mut.GetKey(),
					version: commitVersion,
					value:   mvcc.EncodeWrite(mvcc.Write{Kind: mut.GetOp(), StartTs: startVersion}),
				},
			)
		default:
			return nil, txnUnsupportedMutation(mut.GetOp())
		}
	}
	entries := make([]*kv.Entry, 0, len(ops))
	for _, op := range ops {
		entries = append(entries, kv.NewInternalEntry(op.cf, op.key, op.version, op.value, op.meta, op.expires))
	}
	return entries, nil
}

func maxObservedVersion(db *localdb.DB) (uint64, error) {
	if db == nil {
		return 0, nil
	}
	iter := db.NewInternalIterator(&index.Options{IsAsc: true})
	if iter == nil {
		return 0, nil
	}
	defer func() { _ = iter.Close() }()
	var maxVersion uint64
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			continue
		}
		_, _, version, ok := kv.SplitInternalKey(item.Entry().Key)
		if !ok {
			return 0, errInvalidInternalEntry
		}
		if version == kv.MaxVersion {
			continue
		}
		if version > maxVersion {
			maxVersion = version
		}
	}
	return maxVersion, nil
}

func releaseEntries(entries []*kv.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
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
