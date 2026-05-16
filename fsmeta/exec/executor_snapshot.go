// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

// SnapshotSubtree reserves a durable MVCC read version for one direct subtree
// root. The service boundary publishes the returned token into rooted truth so
// GC can treat it as a retained snapshot until RetireSnapshotSubtree.
func (e *Executor) SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	program, err := compile.CompileSnapshotSubtreeProgram(req, mountRecord.Identity())
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	if err := e.flushPerasAuthority(ctx, delta.Authority); err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	if capturer, ok := e.perasCommitter.(PerasSnapshotCapturer); ok {
		if err := capturer.CapturePerasSnapshot(version); err != nil {
			return fsmeta.SnapshotSubtreeToken{}, err
		}
	}
	return fsmeta.SnapshotSubtreeToken{
		Mount:       req.Mount,
		MountKeyID:  mountRecord.MountKeyID,
		RootInode:   req.RootInode,
		ReadVersion: version,
	}, nil
}

func (e *Executor) ResolveSnapshotSubtreeToken(ctx context.Context, token fsmeta.SnapshotSubtreeToken) (fsmeta.SnapshotSubtreeToken, error) {
	record, err := e.resolveKnownMount(ctx, token.Mount)
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	if token.RootInode == 0 || token.ReadVersion == 0 {
		return fsmeta.SnapshotSubtreeToken{}, fsmeta.ErrInvalidRequest
	}
	token.MountKeyID = record.MountKeyID
	return token, nil
}
