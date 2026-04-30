package percolator

import "github.com/feichai0017/NoKV/engine/mvcc"

type GCWriteVersion = mvcc.GCWriteVersion
type GCWriteDecision = mvcc.GCWriteDecision

func EffectiveMVCCSafePoint(requested uint64, floors ...uint64) uint64 {
	return mvcc.EffectiveSafePoint(requested, floors...)
}

func PlanWriteGC(versions []GCWriteVersion, safePoint uint64) []GCWriteDecision {
	return mvcc.PlanWriteGC(versions, safePoint)
}

func AppendWriteGCDecisions(dst []GCWriteDecision, versions []GCWriteVersion, safePoint uint64) []GCWriteDecision {
	return mvcc.AppendWriteGCDecisions(dst, versions, safePoint)
}

func WriteNeedsDefaultRecord(write Write) bool {
	return mvcc.WriteNeedsDefaultRecord(write)
}
