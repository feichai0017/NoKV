package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
)

func TestShardForInternalKeyUsesUserKeyRouter(t *testing.T) {
	userKey := []byte("fsmeta-create-key")
	internal := kv.InternalKey(kv.CFDefault, userKey, 99)
	requireShard := utils.ShardForUserKey(userKey, 4)
	if got := ShardForInternalKey(internal, 4); got != requireShard {
		t.Fatalf("ShardForInternalKey() = %d, want %d", got, requireShard)
	}
}
