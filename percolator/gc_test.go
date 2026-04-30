package percolator

import (
	"testing"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestEffectiveMVCCSafePoint(t *testing.T) {
	require.Zero(t, EffectiveMVCCSafePoint(0, 10, 20))
	require.Equal(t, uint64(100), EffectiveMVCCSafePoint(100))
	require.Equal(t, uint64(40), EffectiveMVCCSafePoint(100, 0, 40, 80))
	require.Equal(t, uint64(100), EffectiveMVCCSafePoint(100, 0, 120))
}

func TestPlanWriteGCKeepsAnchorAndNewerVersions(t *testing.T) {
	versions := []GCWriteVersion{
		{CommitTs: 150, Write: Write{Kind: kvrpcpb.Mutation_Put, StartTs: 140}},
		{CommitTs: 90, Write: Write{Kind: kvrpcpb.Mutation_Put, StartTs: 80}},
		{CommitTs: 40, Write: Write{Kind: kvrpcpb.Mutation_Put, StartTs: 30}},
	}

	decisions := PlanWriteGC(versions, 100)
	require.Len(t, decisions, 3)
	require.True(t, decisions[0].Keep)
	require.False(t, decisions[0].Anchor)
	require.Equal(t, uint64(140), decisions[0].RetainDefaultStartTs)
	require.True(t, decisions[1].Keep)
	require.True(t, decisions[1].Anchor)
	require.Equal(t, uint64(80), decisions[1].RetainDefaultStartTs)
	require.False(t, decisions[2].Keep)
	require.Zero(t, decisions[2].RetainDefaultStartTs)
}

func TestPlanWriteGCPreservesInputOrderWhenFindingAnchor(t *testing.T) {
	versions := []GCWriteVersion{
		{CommitTs: 40, Write: Write{Kind: kvrpcpb.Mutation_Put, StartTs: 30}},
		{CommitTs: 150, Write: Write{Kind: kvrpcpb.Mutation_Put, StartTs: 140}},
		{CommitTs: 90, Write: Write{Kind: kvrpcpb.Mutation_Put, StartTs: 80}},
	}

	decisions := PlanWriteGC(versions, 100)
	require.False(t, decisions[0].Keep)
	require.True(t, decisions[1].Keep)
	require.True(t, decisions[2].Keep)
	require.True(t, decisions[2].Anchor)
}

func TestPlanWriteGCDisabledSafePointKeepsAllVersions(t *testing.T) {
	versions := []GCWriteVersion{
		{CommitTs: 20, Write: Write{Kind: kvrpcpb.Mutation_Put, StartTs: 10}},
		{CommitTs: 10, Write: Write{Kind: kvrpcpb.Mutation_Delete, StartTs: 9}},
	}

	decisions := PlanWriteGC(versions, 0)
	require.Len(t, decisions, 2)
	require.True(t, decisions[0].Keep)
	require.False(t, decisions[0].Anchor)
	require.True(t, decisions[1].Keep)
	require.False(t, decisions[1].Anchor)
}

func TestPlanWriteGCRetainsOnlyReferencedDefaultRecords(t *testing.T) {
	versions := []GCWriteVersion{
		{CommitTs: 120, Write: Write{Kind: kvrpcpb.Mutation_Put, StartTs: 110, ShortValue: []byte("short")}},
		{CommitTs: 90, Write: Write{Kind: kvrpcpb.Mutation_Delete, StartTs: 80}},
		{CommitTs: 40, Write: Write{Kind: kvrpcpb.Mutation_Rollback, StartTs: 30}},
		{CommitTs: 30, Write: Write{Kind: kvrpcpb.Mutation_Lock, StartTs: 20}},
	}

	decisions := PlanWriteGC(versions, 100)
	require.True(t, decisions[0].Keep)
	require.Zero(t, decisions[0].RetainDefaultStartTs)
	require.True(t, decisions[1].Keep)
	require.True(t, decisions[1].Anchor)
	require.Zero(t, decisions[1].RetainDefaultStartTs)
	require.False(t, decisions[2].Keep)
	require.False(t, decisions[3].Keep)

	decisions = PlanWriteGC(versions[3:], 100)
	require.True(t, decisions[0].Keep)
	require.True(t, decisions[0].Anchor)
	require.Equal(t, uint64(20), decisions[0].RetainDefaultStartTs)
}
