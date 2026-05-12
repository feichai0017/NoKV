package exec

import (
	"context"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

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
	return fsperas.OperationID{ClientID: perasOperationClientID(kind), Seq: seq}
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

func perasDeltaHasConcreteWrites(delta compile.SemanticDelta) bool {
	if len(delta.WriteEffects) == 0 {
		return false
	}
	for _, effect := range delta.WriteEffects {
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return false
			}
		case compile.EffectDelete:
		default:
			return false
		}
	}
	return true
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
