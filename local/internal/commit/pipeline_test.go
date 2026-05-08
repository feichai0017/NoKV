package commit

import (
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestShardForBatchUsesConfiguredUserKeyRouter(t *testing.T) {
	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("semantic-key"), 7), []byte("value"))
	defer entry.DecrRef()

	req := &Request{Entries: []*kv.Entry{entry}}
	batch := &CommitBatch{Reqs: []*CommitRequest{{Req: req}}}
	rr := 0

	shard := shardForBatch(batch, 4, &rr, func(userKey []byte, shardCount int) int {
		require.Equal(t, []byte("semantic-key"), userKey)
		require.Equal(t, 4, shardCount)
		return 3
	})

	require.Equal(t, 3, shard)
	require.Equal(t, 0, rr)
}

func TestShardForBatchFallsBackWhenRouterReturnsInvalidShard(t *testing.T) {
	userKey := []byte("semantic-key")
	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, userKey, 7), []byte("value"))
	defer entry.DecrRef()

	req := &Request{Entries: []*kv.Entry{entry}}
	batch := &CommitBatch{Reqs: []*CommitRequest{{Req: req}}}
	rr := 0

	shard := shardForBatch(batch, 4, &rr, func([]byte, int) int { return 99 })

	require.Equal(t, utils.ShardForUserKey(userKey, 4), shard)
}
