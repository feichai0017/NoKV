package exec

import (
	"bytes"
	"context"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"slices"
	"sort"
	"time"
)

func (e *Executor) tryPerasVisibleOpenWriteSession(ctx context.Context, program compile.OpenWriteSessionProgram, mount fsmeta.MountIdentity, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, bool, error) {
	delta := program.Compiled.Delta
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return fsmeta.SessionRecord{}, false, nil
	}
	plan := delta.Plan
	view := e.newPerasReadView(ctx)
	inode, ok, err := view.readInode(mount, req.Inode)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	if !ok {
		return fsmeta.SessionRecord{}, false, nil
	}
	if inode.Type != fsmeta.InodeTypeFile {
		return fsmeta.SessionRecord{}, false, nil
	}
	nowTime := e.clock()
	expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
	if !ok {
		return fsmeta.SessionRecord{}, false, nil
	}
	now := nowTime.UnixNano()
	if existing, ok, err := view.readSession(plan.ReadKeys[1]); err != nil {
		return fsmeta.SessionRecord{}, false, err
	} else if ok {
		if sessionLive(existing, now) {
			return fsmeta.SessionRecord{}, false, nil
		}
		// Stale cleanup is value-sensitive and may touch an old session-id key
		// outside this request's concrete write-set. Keep it on the transaction
		// runner.
		return fsmeta.SessionRecord{}, false, nil
	}
	if index := e.perasPredicateIndex(); !e.perasNotExistsKnown(delta.Authority, plan.ReadKeys[2], index) {
		if owner, ok, err := view.readSession(plan.ReadKeys[2]); err != nil {
			return fsmeta.SessionRecord{}, false, err
		} else if ok {
			if sessionLive(owner, now) {
				return fsmeta.SessionRecord{}, false, nil
			}
			return fsmeta.SessionRecord{}, false, nil
		}
	}
	record := fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
	value, err := fsmeta.EncodeSessionValue(record)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	evidence := view.predicateEvidenceForDelta(delta)
	concrete, err := compile.MaterializeOpenWriteSession(program, compile.OpenWriteSessionValues{
		SessionValue:    value,
		PredicateProofs: evidence.Proofs,
		KnownAbsent:     evidence.KnownAbsent,
	})
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	committed, err := e.tryPerasVisibleCommit(ctx, concrete)
	if err != nil {
		return fsmeta.SessionRecord{}, committed, err
	}
	if !committed {
		return fsmeta.SessionRecord{}, false, nil
	}
	return record, true, nil
}

func (e *Executor) tryPerasVisibleHeartbeatWriteSession(ctx context.Context, program compile.HeartbeatWriteSessionProgram, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, bool, error) {
	delta := program.Compiled.Delta
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return fsmeta.SessionRecord{}, false, nil
	}
	plan := delta.Plan
	view := e.newPerasReadView(ctx)
	nowTime := e.clock()
	expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
	if !ok {
		return fsmeta.SessionRecord{}, false, nil
	}
	now := nowTime.UnixNano()
	session, ok, err := view.readSession(plan.ReadKeys[0])
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	if !ok || !sessionLive(session, now) || session.Inode != req.Inode {
		return fsmeta.SessionRecord{}, false, nil
	}
	owner, ok, err := view.readSession(plan.ReadKeys[1])
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	if !ok || !sessionLive(owner, now) || owner.Session != req.Session || owner.Inode != req.Inode {
		return fsmeta.SessionRecord{}, false, nil
	}
	record := fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
	value, err := fsmeta.EncodeSessionValue(record)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	evidence := view.predicateEvidenceForDelta(delta)
	concrete, err := compile.MaterializeHeartbeatWriteSession(program, compile.HeartbeatWriteSessionValues{
		SessionValue:    value,
		PredicateProofs: evidence.Proofs,
		KnownAbsent:     evidence.KnownAbsent,
	})
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	committed, err := e.tryPerasVisibleCommit(ctx, concrete)
	if err != nil {
		return fsmeta.SessionRecord{}, committed, err
	}
	if !committed {
		return fsmeta.SessionRecord{}, false, nil
	}
	return record, true, nil
}

func (e *Executor) tryPerasVisibleCloseWriteSession(ctx context.Context, program compile.CloseWriteSessionProgram, mount fsmeta.MountIdentity, req fsmeta.CloseWriteSessionRequest) (bool, error) {
	delta := program.Compiled.Delta
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	plan := delta.Plan
	view := e.newPerasReadView(ctx)
	session, ok, err := view.readSession(plan.ReadKeys[0])
	if err != nil {
		return false, err
	}
	if !ok || session.Inode != req.Inode {
		return false, nil
	}
	deleteOwner := false
	ownerKey, err := fsmeta.EncodeInodeSessionKey(mount, session.Inode)
	if err != nil {
		return false, err
	}
	if owner, ok, err := view.readSession(ownerKey); err != nil {
		return false, err
	} else if ok && owner.Session == req.Session && owner.Inode == session.Inode {
		deleteOwner = true
	}
	evidence := view.predicateEvidenceForDelta(delta)
	concrete, err := compile.MaterializeCloseWriteSession(program, compile.CloseWriteSessionValues{
		DeleteOwner:     deleteOwner,
		PredicateProofs: evidence.Proofs,
		KnownAbsent:     evidence.KnownAbsent,
	})
	if err != nil {
		return false, err
	}
	return e.tryPerasVisibleCommit(ctx, concrete)
}

func (e *Executor) tryPerasVisibleExpireWriteSession(ctx context.Context, mount fsmeta.MountIdentity, record fsmeta.SessionRecord) (bool, error) {
	program, err := compile.CompileCloseWriteSessionProgram(fsmeta.CloseWriteSessionRequest{
		Mount:   mount.MountID,
		Inode:   record.Inode,
		Session: record.Session,
	}, mount)
	if err != nil {
		return false, err
	}
	return e.tryPerasVisibleCloseWriteSession(ctx, program, mount, fsmeta.CloseWriteSessionRequest{
		Mount:   mount.MountID,
		Inode:   record.Inode,
		Session: record.Session,
	})
}

func sessionDrainScopeForInodes(mount fsmeta.MountIdentity, inodes map[fsmeta.InodeID]struct{}) compile.AuthorityScope {
	if mount.MountKeyID == 0 || len(inodes) == 0 {
		return compile.AuthorityScope{}
	}
	out := compile.AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
		Inodes:     make([]fsmeta.InodeID, 0, len(inodes)),
	}
	seenBuckets := make(map[fsmeta.AffinityBucket]struct{}, len(inodes))
	for inode := range inodes {
		out.Inodes = append(out.Inodes, inode)
		bucket := fsmeta.BucketForInodeID(inode)
		if _, ok := seenBuckets[bucket]; !ok {
			seenBuckets[bucket] = struct{}{}
			out.Buckets = append(out.Buckets, bucket)
		}
	}
	slices.Sort(out.Inodes)
	slices.Sort(out.Buckets)
	return out
}

// OpenWriteSession records one exclusive writer lease for an inode. It writes
// both a session-id key and an inode-owner key so concurrent opens for the same
// inode conflict on one Percolator key.
func (e *Executor) OpenWriteSession(ctx context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileOpenWriteSessionProgram(req, mount)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.SessionRecord{}, err
	}
	plan := delta.Plan
	if req.TTL <= 0 {
		return fsmeta.SessionRecord{}, fsmeta.ErrInvalidRequest
	}
	if record, committed, err := e.tryPerasVisibleOpenWriteSession(ctx, program, mount, req); committed || err != nil {
		if err != nil {
			return fsmeta.SessionRecord{}, err
		}
		return record, nil
	}
	var record fsmeta.SessionRecord
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		inode, ok, err := e.readInode(ctx, mount, req.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return fsmeta.ErrNotFound
		}
		if inode.Type != fsmeta.InodeTypeFile {
			return fsmeta.ErrInvalidRequest
		}
		inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
		if err != nil {
			return err
		}
		inodeValue, err := fsmeta.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		nowTime := e.clock()
		expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
		if !ok {
			return fsmeta.ErrInvalidRequest
		}
		candidate := fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
		now := nowTime.UnixNano()
		predicates := make([]*kvrpcpb.AtomicPredicate, 0, 4)
		if existing, ok, err := e.readSessionByKey(ctx, plan.ReadKeys[1], startVersion); err != nil {
			return err
		} else if ok && sessionLive(existing, now) {
			return fsmeta.ErrExists
		} else if ok {
			existingValue, err := fsmeta.EncodeSessionValue(existing)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(plan.ReadKeys[1], existingValue))
		} else {
			predicates = append(predicates, atomicNotExists(plan.ReadKeys[1]))
		}
		mutations := make([]*kvrpcpb.Mutation, 0, 3)
		if owner, ok, err := e.readSessionByKey(ctx, plan.ReadKeys[2], startVersion); err != nil {
			return err
		} else if ok {
			if sessionLive(owner, now) {
				return fsmeta.ErrExists
			}
			ownerValue, err := fsmeta.EncodeSessionValue(owner)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(plan.ReadKeys[2], ownerValue))
			staleSessionKey, err := fsmeta.EncodeSessionKey(mount, owner.Inode, owner.Session)
			if err != nil {
				return err
			}
			if string(staleSessionKey) != string(plan.ReadKeys[1]) {
				if value, ok, err := e.runner.Get(ctx, staleSessionKey, startVersion); err != nil {
					return err
				} else if ok && bytes.Equal(value, ownerValue) {
					predicates = append(predicates, atomicValueEquals(staleSessionKey, ownerValue))
					mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: staleSessionKey})
				}
			}
		} else {
			predicates = append(predicates, atomicNotExists(plan.ReadKeys[2]))
		}
		value, err := fsmeta.EncodeSessionValue(candidate)
		if err != nil {
			return err
		}
		mutations = append(mutations,
			&kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[0]), Value: value},
			&kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[1]), Value: value},
		)
		predicates = append(predicates, atomicValueEquals(inodeKey, inodeValue))
		// Open is a value-sensitive admission path: the session-id key, owner
		// key, inode key, and any stale cleanup key must still match the values
		// read above. Value predicates make the 1PC attempt a real CAS instead
		// of an existence-only overwrite.
		if err := e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion); err != nil {
			return err
		}
		record = candidate
		return nil
	}, delta.Authority); err != nil {
		return fsmeta.SessionRecord{}, err
	}
	return record, nil
}

// HeartbeatWriteSession extends a live writer lease. Both session records must
// agree, otherwise the session is considered lost and the caller must reopen.
func (e *Executor) HeartbeatWriteSession(ctx context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileHeartbeatWriteSessionProgram(req, mount)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.SessionRecord{}, err
	}
	plan := delta.Plan
	if req.TTL <= 0 {
		return fsmeta.SessionRecord{}, fsmeta.ErrInvalidRequest
	}
	if record, committed, err := e.tryPerasVisibleHeartbeatWriteSession(ctx, program, req); committed || err != nil {
		if err != nil {
			return fsmeta.SessionRecord{}, err
		}
		return record, nil
	}
	var record fsmeta.SessionRecord
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		nowTime := e.clock()
		expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
		if !ok {
			return fsmeta.ErrInvalidRequest
		}
		candidate := fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
		now := nowTime.UnixNano()
		session, ok, err := e.readSessionByKey(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		if !ok || !sessionLive(session, now) || session.Inode != req.Inode {
			return fsmeta.ErrNotFound
		}
		sessionValue, err := fsmeta.EncodeSessionValue(session)
		if err != nil {
			return err
		}
		owner, ok, err := e.readSessionByKey(ctx, plan.ReadKeys[1], startVersion)
		if err != nil {
			return err
		}
		if !ok || !sessionLive(owner, now) || owner.Session != req.Session || owner.Inode != req.Inode {
			return fsmeta.ErrNotFound
		}
		ownerValue, err := fsmeta.EncodeSessionValue(owner)
		if err != nil {
			return err
		}
		value, err := fsmeta.EncodeSessionValue(candidate)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[0]), Value: value},
			{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[1]), Value: value},
		}
		predicates := []*kvrpcpb.AtomicPredicate{
			atomicValueEquals(plan.ReadKeys[0], sessionValue),
			atomicValueEquals(plan.ReadKeys[1], ownerValue),
		}
		if err := e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion); err != nil {
			return err
		}
		record = candidate
		return nil
	}, delta.Authority); err != nil {
		return fsmeta.SessionRecord{}, err
	}
	return record, nil
}

// CloseWriteSession releases one writer lease. It deletes the owner key only
// when it still points at the closing session.
func (e *Executor) CloseWriteSession(ctx context.Context, req fsmeta.CloseWriteSessionRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileCloseWriteSessionProgram(req, mount)
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	if committed, err := e.tryPerasVisibleCloseWriteSession(ctx, program, mount, req); committed || err != nil {
		return err
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		session, ok, err := e.readSessionByKey(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return fsmeta.ErrNotFound
		}
		if session.Inode != req.Inode {
			return fsmeta.ErrNotFound
		}
		sessionValue, err := fsmeta.EncodeSessionValue(session)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Delete, Key: cloneBytes(plan.MutateKeys[0])}}
		predicates := []*kvrpcpb.AtomicPredicate{atomicValueEquals(plan.ReadKeys[0], sessionValue)}
		ownerKey, err := fsmeta.EncodeInodeSessionKey(mount, session.Inode)
		if err != nil {
			return err
		}
		if owner, ok, err := e.readSessionByKey(ctx, ownerKey, startVersion); err != nil {
			return err
		} else if ok && owner.Session == req.Session && owner.Inode == session.Inode {
			ownerValue, err := fsmeta.EncodeSessionValue(owner)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(ownerKey, ownerValue))
			mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: ownerKey})
		}
		return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	return nil
}

// ExpireWriteSessions removes stale session-id and inode-owner records for one
// mount. It is a bounded maintenance primitive; callers should repeat until
// Expired is zero when draining a large backlog.
func (e *Executor) ExpireWriteSessions(ctx context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileExpireWriteSessionsProgram(req, mount)
	if err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, err
	}
	plan := delta.Plan
	now := e.clock().UnixNano()
	var expired uint64
	scanPrefixes := plan.ReadPrefixes
	if err := e.withTxnRetryNoPerasFlush(ctx, func(startVersion, commitVersion uint64) error {
		deletes := make(map[string][]byte)
		type expiredSessionKey struct {
			inode   fsmeta.InodeID
			session fsmeta.SessionID
		}
		expiredSessions := make(map[expiredSessionKey]struct{})
		fallbackInodes := make(map[fsmeta.InodeID]struct{})
		remaining := plan.Limit
		for _, scanPrefix := range scanPrefixes {
			if remaining == 0 {
				break
			}
			kvs, err := e.runner.Scan(ctx, scanPrefix, remaining, startVersion)
			if err != nil {
				return err
			}
			kvs = e.mergePerasOverlayScan(kvs, scanPrefix, remaining)
			var matched uint32
			for _, kv := range kvs {
				if !bytes.HasPrefix(kv.Key, scanPrefix) {
					break
				}
				matched++
				kind, err := fsmeta.KeyKindOf(kv.Key)
				if err != nil {
					return err
				}
				if kind != fsmeta.KeyKindSession {
					continue
				}
				record, err := fsmeta.DecodeSessionValue(kv.Value)
				if err != nil {
					return err
				}
				if sessionLive(record, now) {
					continue
				}
				expiredKey := expiredSessionKey{inode: record.Inode, session: record.Session}
				if _, seen := expiredSessions[expiredKey]; seen {
					continue
				}
				if committed, err := e.tryPerasVisibleExpireWriteSession(ctx, mount, record); err != nil {
					return err
				} else if committed {
					expiredSessions[expiredKey] = struct{}{}
					continue
				}
				deletes[string(kv.Key)] = cloneBytes(kv.Key)
				sessionKey, err := fsmeta.EncodeSessionKey(mount, record.Inode, record.Session)
				if err != nil {
					return err
				}
				ownerKey, err := fsmeta.EncodeInodeSessionKey(mount, record.Inode)
				if err != nil {
					return err
				}
				if value, ok, err := e.runner.Get(ctx, sessionKey, startVersion); err != nil {
					return err
				} else if ok && bytes.Equal(value, kv.Value) {
					deletes[string(sessionKey)] = sessionKey
					expiredSessions[expiredSessionKey{inode: record.Inode, session: record.Session}] = struct{}{}
				}
				if value, ok, err := e.runner.Get(ctx, ownerKey, startVersion); err != nil {
					return err
				} else if ok && bytes.Equal(value, kv.Value) {
					deletes[string(ownerKey)] = ownerKey
				}
				fallbackInodes[record.Inode] = struct{}{}
			}
			remaining -= matched
		}
		if len(deletes) == 0 {
			expired = uint64(len(expiredSessions))
			return nil
		}
		drainScope := sessionDrainScopeForInodes(mount, fallbackInodes)
		if authorityScopeEmpty(drainScope) {
			drainScope = delta.Authority
		}
		// Fallback expiration mutates base LSM session keys. Drain only after
		// concrete expired keys are known so ordinary visible session updates do
		// not wait behind speculative maintenance scans.
		if err := e.drainPerasAuthority(ctx, drainScope); err != nil {
			return err
		}
		keys := make([]string, 0, len(deletes))
		for key := range deletes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		mutations := make([]*kvrpcpb.Mutation, 0, len(deletes))
		for _, key := range keys {
			mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: deletes[key]})
		}
		primary := deletes[keys[0]]
		if _, err := e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL); err != nil {
			return err
		}
		expired = uint64(len(expiredSessions))
		return nil
	}); err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, err
	}
	return fsmeta.ExpireWriteSessionsResult{Expired: expired}, nil
}

func sessionExpiryUnixNs(now time.Time, ttl time.Duration) (int64, bool) {
	if ttl <= 0 {
		return 0, false
	}
	const maxInt64 = int64(1<<63 - 1)
	nowUnixNs := now.UnixNano()
	ttlUnixNs := int64(ttl)
	if nowUnixNs > maxInt64-ttlUnixNs {
		return 0, false
	}
	return nowUnixNs + ttlUnixNs, true
}

func (e *Executor) clock() time.Time {
	if e != nil && e.now != nil {
		return e.now()
	}
	return time.Now()
}

func sessionLive(record fsmeta.SessionRecord, nowUnixNs int64) bool {
	return record.ExpiresUnixNs > nowUnixNs
}
