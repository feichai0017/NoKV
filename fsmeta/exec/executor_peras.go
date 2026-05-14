package exec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

var perasClientIDFallbackSeq atomic.Uint64

func perasPutEffect(key, value []byte) compile.WriteEffect {
	return compile.WriteEffect{Kind: compile.EffectPut, Key: cloneBytes(key), Value: cloneBytes(value)}
}

func perasDeleteEffect(key []byte) compile.WriteEffect {
	return compile.WriteEffect{Kind: compile.EffectDelete, Key: cloneBytes(key)}
}

func (e *Executor) nextPerasOperationID(kind fsmeta.OperationKind) fsperas.OperationID {
	seq := uint64(1)
	if e != nil {
		seq = e.perasSeq.Add(1)
	}
	clientID := perasOperationClientID(kind)
	if e != nil && e.perasClientID != "" {
		clientID += "/" + e.perasClientID
	}
	return fsperas.OperationID{ClientID: clientID, Seq: seq}
}

func newPerasClientID() string {
	var entropy [12]byte
	if _, err := rand.Read(entropy[:]); err == nil {
		return hex.EncodeToString(entropy[:])
	}
	return fmt.Sprintf("%x-%x-%x", os.Getpid(), time.Now().UnixNano(), perasClientIDFallbackSeq.Add(1))
}

func perasOperationClientID(kind fsmeta.OperationKind) string {
	switch kind {
	case fsmeta.OperationCreate:
		return "fsmeta-exec/create"
	case fsmeta.OperationUpdateInode:
		return "fsmeta-exec/update_inode"
	case fsmeta.OperationRename:
		return "fsmeta-exec/rename"
	case fsmeta.OperationLink:
		return "fsmeta-exec/link"
	case fsmeta.OperationUnlink:
		return "fsmeta-exec/unlink"
	case fsmeta.OperationOpenWriteSession:
		return "fsmeta-exec/open_write_session"
	case fsmeta.OperationHeartbeatSession:
		return "fsmeta-exec/heartbeat_write_session"
	case fsmeta.OperationCloseSession:
		return "fsmeta-exec/close_write_session"
	default:
		return "fsmeta-exec/" + string(kind)
	}
}

func (e *Executor) perasQuotaMode() compile.QuotaMode {
	if e != nil && e.perasCommitter == nil && e.quotas != nil {
		return compile.QuotaModeShared
	}
	return compile.QuotaModeNone
}

func (e *Executor) perasQuotaAllowsVisibleCommit(ctx context.Context, changes []QuotaChange) (bool, error) {
	if e == nil || e.quotas == nil || len(changes) == 0 {
		return true, nil
	}
	admitter, ok := e.quotas.(PerasQuotaAdmitter)
	if !ok {
		return false, nil
	}
	return admitter.AllowPerasVisibleQuota(ctx, changes)
}
