package exec

import (
	"context"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

func (e *Executor) tryPerasVisibleUnlink(ctx context.Context, program compile.UnlinkProgram, mount fsmeta.MountIdentity, req fsmeta.UnlinkRequest) (bool, error) {
	compiled := program.Compiled
	delta := compiled.Delta
	plan := delta.Plan
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	view := e.newPerasReadView(ctx)
	record, err := view.readDentry(plan.PrimaryKey)
	if err != nil {
		return false, err
	}
	inode, ok, err := view.readInode(mount, record.Inode)
	if err != nil {
		return false, err
	}
	if !ok || inode.Type == fsmeta.InodeTypeDirectory {
		return false, nil
	}
	quotaOK, err := e.perasQuotaAllowsVisibleCommit(ctx, []QuotaChange{{
		Mount:      req.Mount,
		MountKeyID: mount.MountKeyID,
		Scope:      req.Parent,
		Bytes:      -inodeSizeDelta(inode.Size),
		Inodes:     -1,
	}})
	if err != nil {
		return false, err
	}
	if !quotaOK {
		return false, nil
	}
	inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
	if err != nil {
		return false, err
	}
	effects := []compile.WriteEffect{perasDeleteEffect(plan.MutateKeys[0])}
	if inode.LinkCount <= 1 {
		effects = append(effects, perasDeleteEffect(inodeKey))
	} else {
		inode.LinkCount--
		inodeValue, err := fsmeta.EncodeInodeValue(inode)
		if err != nil {
			return false, err
		}
		effects = append(effects, perasPutEffect(inodeKey, inodeValue))
	}
	concrete, err := view.materializePerasCompiledOp(compiled, effects)
	if err != nil {
		return false, err
	}
	return e.tryPerasVisibleCommit(ctx, concrete)
}

// Unlink removes one dentry, decrements its inode link count, and deletes the
// inode record when the last dentry goes away.
func (e *Executor) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileUnlinkProgram(req, mount, compile.WithQuotaMode(e.perasQuotaMode()))
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	if committed, err := e.tryPerasVisibleUnlink(ctx, program, mount, req); committed || err != nil {
		if err != nil {
			return err
		}
		e.invalidateNegative(plan.MutateKeys[0])
		e.invalidateDirPages(req.Mount, req.Parent)
		return nil
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		record, err := e.readDentry(ctx, plan.PrimaryKey, startVersion)
		if err != nil {
			return err
		}
		dentryValue, err := fsmeta.EncodeDentryValue(record)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{{
			Op:  kvrpcpb.Mutation_Delete,
			Key: cloneBytes(plan.MutateKeys[0]),
		}}
		predicates := []*kvrpcpb.AtomicPredicate{atomicValueEquals(plan.PrimaryKey, dentryValue)}
		if inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion); err != nil {
			return err
		} else if ok {
			inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
			if err != nil {
				return err
			}
			oldInodeValue, err := fsmeta.EncodeInodeValue(inode)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(inodeKey, oldInodeValue))
			if inode.LinkCount <= 1 {
				mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: inodeKey})
			} else {
				inode.LinkCount--
				inodeValue, err := fsmeta.EncodeInodeValue(inode)
				if err != nil {
					return err
				}
				mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: inodeKey, Value: inodeValue})
			}
			quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
				Mount:      req.Mount,
				MountKeyID: mount.MountKeyID,
				Scope:      req.Parent,
				Bytes:      -inodeSizeDelta(inode.Size),
				Inodes:     -1,
			}}, startVersion)
			if err != nil {
				return err
			}
			mutations = append(mutations, quotaMutations...)
		}
		if len(mutations) == len(predicates) {
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	// Unlink removed the dentry; the next Lookup must observe ErrNotFound
	// from the runner instead of any prior positive memo (we do not cache
	// hits today, but Invalidate is also the right thing for any future
	// hit-cache layering). Bump the parent's dirpage epoch so a cached
	// ReadDirPlus does not still surface the dentry.
	e.invalidateNegative(plan.MutateKeys[0])
	e.invalidateDirPages(req.Mount, req.Parent)
	return nil
}
