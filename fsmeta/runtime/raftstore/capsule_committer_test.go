package raftstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	fscapsule "github.com/feichai0017/NoKV/fsmeta/exec/capsule"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	capsuleauth "github.com/feichai0017/NoKV/fsmeta/runtime/capsuleauth"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

func TestRemoteCapsuleCommitterCommitsAndServesOverlay(t *testing.T) {
	provider := &fakeRuntimeCapsuleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRemoteCapsuleCommitter(RemoteCapsuleCommitterConfig{
		Authority: provider,
		Witnesses: testRuntimeCapsuleWitnesses(t, 3),
	})
	require.NoError(t, err)

	delta := testRuntimeCapsuleDelta([]byte("dentry/a"), []byte("inode/a"))
	_, err = committer.CommitCapsule(context.Background(), fscapsule.OperationID{ClientID: "client", Seq: 1}, delta)
	require.NoError(t, err)

	value, deleted, ok := committer.GetCapsuleOverlay([]byte("dentry/a"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func BenchmarkRemoteCapsuleCommitterCreate(b *testing.B) {
	provider := &fakeRuntimeCapsuleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRemoteCapsuleCommitter(RemoteCapsuleCommitterConfig{
		Authority: provider,
		Witnesses: testRuntimeCapsuleWitnesses(b, 3),
	})
	require.NoError(b, err)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dentryKey := appendUvarintKey("dentry/", uint64(i))
		inodeKey := appendUvarintKey("inode/", uint64(i))
		_, err := committer.CommitCapsule(ctx, fscapsule.OperationID{ClientID: "bench", Seq: uint64(i + 1)}, testRuntimeCapsuleDelta(dentryKey, inodeKey))
		if err != nil {
			b.Fatal(err)
		}
	}
}

type fakeRuntimeCapsuleGrantProvider struct {
	holderID string
	grant    capsuleauth.AuthorityGrant
	owned    bool
	err      error
}

func (p *fakeRuntimeCapsuleGrantProvider) HolderID() string {
	return p.holderID
}

func (p *fakeRuntimeCapsuleGrantProvider) Acquire(context.Context, compile.AuthorityScope) (capsuleauth.AuthorityGrant, bool, error) {
	owned := p.owned
	if !owned {
		owned = true
	}
	return p.grant, owned, p.err
}

func testRuntimeCapsuleWitnesses(tb testing.TB, n int) []fscapsule.WitnessReplica {
	tb.Helper()
	witnesses := make([]fscapsule.WitnessReplica, 0, n)
	for i := 0; i < n; i++ {
		manager, err := wal.Open(wal.Config{Dir: tb.TempDir()})
		require.NoError(tb, err)
		tb.Cleanup(func() { _ = manager.Close() })
		log, err := fscapsule.NewWALWitnessLog(manager, wal.DurabilityBuffered)
		require.NoError(tb, err)
		witness, err := fscapsule.NewLocalWitnessReplica(fmt.Sprintf("witness-%d", i), log)
		require.NoError(tb, err)
		witnesses = append(witnesses, witness)
	}
	return witnesses
}

func testRuntimeCommitterGrant() capsuleauth.AuthorityGrant {
	return capsuleauth.AuthorityGrant{
		GrantID:         "grant-1",
		EpochID:         1,
		HolderID:        "holder-a",
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
		Scope: rootproto.CapsuleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 1,
			Parents:    []uint64{1},
			Inodes:     []uint64{2},
		},
	}
}

func testRuntimeCapsuleDelta(dentryKey, inodeKey []byte) compile.SemanticDelta {
	return compile.SemanticDelta{
		Kind: fsmeta.OperationCreate,
		Authority: compile.AuthorityScope{
			Mount:      "vol",
			MountKeyID: 1,
			Parents:    []fsmeta.InodeID{1},
			Inodes:     []fsmeta.InodeID{2},
		},
		ReadPredicates: []compile.Predicate{
			{Kind: compile.PredicateNotExists, Key: dentryKey},
			{Kind: compile.PredicateNotExists, Key: inodeKey},
		},
		WriteEffects: []compile.WriteEffect{
			{Kind: compile.EffectPut, Key: dentryKey, Value: []byte("dentry-value")},
			{Kind: compile.EffectPut, Key: inodeKey, Value: []byte("inode-value")},
		},
		Eligibility: compile.EligibilityFastPath,
	}
}

func appendUvarintKey(prefix string, v uint64) []byte {
	out := append([]byte(prefix), 0)
	return binary.AppendUvarint(out, v)
}
