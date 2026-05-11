package raftstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	perasauth "github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

func TestRemotePerasCommitterCommitsAndServesOverlay(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority: provider,
		Witnesses: testRuntimePerasWitnesses(t, 3),
	})
	require.NoError(t, err)

	delta := testRuntimePerasDelta([]byte("dentry/a"), []byte("inode/a"))
	_, err = committer.CommitPeras(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta)
	require.NoError(t, err)

	value, deleted, ok := committer.GetPerasOverlay([]byte("dentry/a"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func BenchmarkRemotePerasCommitterCreate(b *testing.B) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority: provider,
		Witnesses: testRuntimePerasWitnesses(b, 3),
	})
	require.NoError(b, err)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dentryKey := appendUvarintKey("dentry/", uint64(i))
		inodeKey := appendUvarintKey("inode/", uint64(i))
		_, err := committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "bench", Seq: uint64(i + 1)}, testRuntimePerasDelta(dentryKey, inodeKey))
		if err != nil {
			b.Fatal(err)
		}
	}
}

type fakeRuntimePerasGrantProvider struct {
	holderID string
	grant    perasauth.AuthorityGrant
	owned    bool
	err      error
}

func (p *fakeRuntimePerasGrantProvider) HolderID() string {
	return p.holderID
}

func (p *fakeRuntimePerasGrantProvider) Acquire(context.Context, compile.AuthorityScope) (perasauth.AuthorityGrant, bool, error) {
	owned := p.owned
	if !owned {
		owned = true
	}
	return p.grant, owned, p.err
}

func testRuntimePerasWitnesses(tb testing.TB, n int) []fsperas.WitnessReplica {
	tb.Helper()
	witnesses := make([]fsperas.WitnessReplica, 0, n)
	for i := range n {
		manager, err := wal.Open(wal.Config{Dir: tb.TempDir()})
		require.NoError(tb, err)
		tb.Cleanup(func() { _ = manager.Close() })
		log, err := fsperas.NewWALWitnessLog(manager, wal.DurabilityBuffered)
		require.NoError(tb, err)
		witness, err := fsperas.NewLocalWitnessReplica(fmt.Sprintf("witness-%d", i), log)
		require.NoError(tb, err)
		witnesses = append(witnesses, witness)
	}
	return witnesses
}

func testRuntimeCommitterGrant() perasauth.AuthorityGrant {
	return perasauth.AuthorityGrant{
		GrantID:         "grant-1",
		EpochID:         1,
		HolderID:        "holder-a",
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
		Scope: rootproto.PerasAuthorityScope{
			MountID:    "vol",
			MountKeyID: 1,
			Parents:    []uint64{1},
			Inodes:     []uint64{2},
		},
	}
}

func testRuntimePerasDelta(dentryKey, inodeKey []byte) compile.SemanticDelta {
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
