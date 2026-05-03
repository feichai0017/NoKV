package main

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
)

func TestSoakHistoryOpsScopesExternalNamespaceOperations(t *testing.T) {
	const (
		mount      = fsmeta.MountID("prod")
		scopeName  = "soak-scope"
		scopeInode = fsmeta.InodeID(8001)
	)
	ops := soakHistoryOps([]fsmetacontract.Operation{
		{Kind: fsmetacontract.OpHeartbeatSession, Mount: "vol", Session: "writer-a"},
		{Kind: fsmetacontract.OpLookup, Mount: "vol", Parent: fsmeta.RootInode, Name: "alpha"},
		{Kind: fsmetacontract.OpReadDirPlus, Mount: "vol", Parent: fsmeta.RootInode, StartAfter: "a", Limit: 10},
		{Kind: fsmetacontract.OpUnlink, Mount: "vol", Parent: fsmeta.RootInode, Name: "alpha"},
		{Kind: fsmetacontract.OpExpireSessions, Mount: "vol", Limit: 1},
	}, mount, scopeName, scopeInode)

	if len(ops) != 4 {
		t.Fatalf("filtered op count=%d, want 4: %#v", len(ops), ops)
	}
	if ops[0].Kind != fsmetacontract.OpCreate || ops[0].Mount != mount || ops[0].Parent != fsmeta.RootInode || ops[0].Name != scopeName || ops[0].Inode != scopeInode {
		t.Fatalf("scope create mismatch: %#v", ops[0])
	}
	for _, op := range ops[1:] {
		if op.Mount != mount || op.Parent != scopeInode {
			t.Fatalf("op was not scoped into generated root: %#v", op)
		}
	}
	if ops[2].StartAfter != "a" || ops[2].Limit != 10 {
		t.Fatalf("ReadDirPlus pagination fields changed: %#v", ops[2])
	}
}
