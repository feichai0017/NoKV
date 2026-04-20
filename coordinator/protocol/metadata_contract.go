package protocol

import coordpb "github.com/feichai0017/NoKV/pb/coordinator"

// NormalizeFreshness collapses unspecified or unknown freshness requests to the
// protocol default contract.
func NormalizeFreshness(f coordpb.Freshness) coordpb.Freshness {
	switch f {
	case coordpb.Freshness_FRESHNESS_STRONG,
		coordpb.Freshness_FRESHNESS_BOUNDED,
		coordpb.Freshness_FRESHNESS_BEST_EFFORT:
		return f
	default:
		return coordpb.Freshness_FRESHNESS_BEST_EFFORT
	}
}

// MetadataServingContract derives the externally visible serving class and
// sync-health projection for one metadata-answer reply.
func MetadataServingContract(degraded coordpb.DegradedMode, catchUp coordpb.CatchUpState, rootLag uint64, servedByLeader bool) (coordpb.ServingClass, coordpb.SyncHealth) {
	syncHealth := metadataSyncHealth(degraded, catchUp, rootLag)
	switch {
	case degraded == coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE,
		catchUp == coordpb.CatchUpState_CATCH_UP_STATE_UNAVAILABLE,
		catchUp == coordpb.CatchUpState_CATCH_UP_STATE_BOOTSTRAP_REQUIRED:
		return coordpb.ServingClass_SERVING_CLASS_DEGRADED, syncHealth
	case rootLag == 0 && servedByLeader:
		return coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE, syncHealth
	default:
		return coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE, syncHealth
	}
}

func metadataSyncHealth(degraded coordpb.DegradedMode, catchUp coordpb.CatchUpState, rootLag uint64) coordpb.SyncHealth {
	switch {
	case degraded == coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE,
		catchUp == coordpb.CatchUpState_CATCH_UP_STATE_UNAVAILABLE:
		return coordpb.SyncHealth_SYNC_HEALTH_ROOT_UNAVAILABLE
	case catchUp == coordpb.CatchUpState_CATCH_UP_STATE_BOOTSTRAP_REQUIRED:
		return coordpb.SyncHealth_SYNC_HEALTH_BOOTSTRAP_REQUIRED
	case rootLag > 0 || catchUp == coordpb.CatchUpState_CATCH_UP_STATE_LAGGING:
		return coordpb.SyncHealth_SYNC_HEALTH_LAGGING
	default:
		return coordpb.SyncHealth_SYNC_HEALTH_HEALTHY
	}
}
