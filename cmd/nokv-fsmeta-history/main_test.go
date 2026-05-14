// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
)

func TestExternalHistoryOpsScopesRootOperationsAndFiltersInternalSessions(t *testing.T) {
	const (
		mount      = fsmeta.MountID("prod")
		scopeName  = "history-scope"
		scopeInode = fsmeta.InodeID(9001)
	)
	scopeOp := scopeCreateOperation(mount, scopeName, scopeInode)
	requireOp(t, scopeOp, fsmetacontract.OpCreate, mount, fsmeta.RootInode, scopeName, scopeInode)

	ops := externalHistoryOps([]fsmetacontract.Operation{
		{Kind: fsmetacontract.OpOpenWriteSession, Mount: "vol", Parent: fsmeta.RootInode, Inode: 10},
		{Kind: fsmetacontract.OpCreate, Mount: "vol", Parent: fsmeta.RootInode, Name: "alpha", Inode: 11},
		{Kind: fsmetacontract.OpRenameSubtree, Mount: "vol", FromParent: fsmeta.RootInode, FromName: "alpha", ToParent: fsmeta.RootInode, ToName: "beta"},
		{Kind: fsmetacontract.OpLink, Mount: "vol", Parent: fsmeta.RootInode, Name: "link", Inode: 11},
		{Kind: fsmetacontract.OpAdvanceTime, Mount: "vol", AdvanceNs: 1},
	}, mount, scopeInode, scopeInode)

	if len(ops) != 3 {
		t.Fatalf("filtered op count=%d, want 3: %#v", len(ops), ops)
	}
	requireOp(t, ops[0], fsmetacontract.OpCreate, mount, scopeInode, "alpha", scopeInode+11)
	if ops[1].Mount != mount || ops[1].FromParent != scopeInode || ops[1].ToParent != scopeInode {
		t.Fatalf("rename was not scoped into generated root: %#v", ops[1])
	}
	requireOp(t, ops[2], fsmetacontract.OpLink, mount, scopeInode, "link", scopeInode+11)
	if got := scopeGeneratedInode(scopeInode, 0); got != 0 {
		t.Fatalf("zero inode remapped to %d", got)
	}
}

func requireOp(t *testing.T, op fsmetacontract.Operation, kind fsmetacontract.OperationKind, mount fsmeta.MountID, parent fsmeta.InodeID, name string, inode fsmeta.InodeID) {
	t.Helper()
	if op.Kind != kind || op.Mount != mount || op.Parent != parent || op.Name != name || op.Inode != inode {
		t.Fatalf("op=%#v, want kind=%s mount=%s parent=%d name=%q inode=%d", op, kind, mount, parent, name, inode)
	}
}
