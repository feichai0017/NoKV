package protocol_test

import (
	"testing"

	coordprotocol "github.com/feichai0017/NoKV/coordinator/protocol"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

func TestNormalizeFreshness(t *testing.T) {
	require.Equal(t, coordpb.Freshness_FRESHNESS_BEST_EFFORT, coordprotocol.NormalizeFreshness(coordpb.Freshness_FRESHNESS_UNSPECIFIED))
	require.Equal(t, coordpb.Freshness_FRESHNESS_STRONG, coordprotocol.NormalizeFreshness(coordpb.Freshness_FRESHNESS_STRONG))
}

func TestMetadataServingContract(t *testing.T) {
	type testCase struct {
		name           string
		degraded       coordpb.DegradedMode
		catchUp        coordpb.CatchUpState
		rootLag        uint64
		servedByLeader bool
		wantClass      coordpb.ServingClass
		wantHealth     coordpb.SyncHealth
	}

	cases := []testCase{
		{
			name:           "authoritative healthy leader",
			degraded:       coordpb.DegradedMode_DEGRADED_MODE_HEALTHY,
			catchUp:        coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
			rootLag:        0,
			servedByLeader: true,
			wantClass:      coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
			wantHealth:     coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
		},
		{
			name:           "lagging becomes bounded stale",
			degraded:       coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING,
			catchUp:        coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
			rootLag:        1,
			servedByLeader: false,
			wantClass:      coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
			wantHealth:     coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
		},
		{
			name:           "root unavailable is degraded",
			degraded:       coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE,
			catchUp:        coordpb.CatchUpState_CATCH_UP_STATE_UNAVAILABLE,
			rootLag:        0,
			servedByLeader: true,
			wantClass:      coordpb.ServingClass_SERVING_CLASS_DEGRADED,
			wantHealth:     coordpb.SyncHealth_SYNC_HEALTH_ROOT_UNAVAILABLE,
		},
		{
			name:           "bootstrap required is degraded",
			degraded:       coordpb.DegradedMode_DEGRADED_MODE_HEALTHY,
			catchUp:        coordpb.CatchUpState_CATCH_UP_STATE_BOOTSTRAP_REQUIRED,
			rootLag:        0,
			servedByLeader: true,
			wantClass:      coordpb.ServingClass_SERVING_CLASS_DEGRADED,
			wantHealth:     coordpb.SyncHealth_SYNC_HEALTH_BOOTSTRAP_REQUIRED,
		},
		{
			name:           "non leader without lag stays bounded stale",
			degraded:       coordpb.DegradedMode_DEGRADED_MODE_HEALTHY,
			catchUp:        coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
			rootLag:        0,
			servedByLeader: false,
			wantClass:      coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
			wantHealth:     coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotClass, gotHealth := coordprotocol.MetadataServingContract(tc.degraded, tc.catchUp, tc.rootLag, tc.servedByLeader)
			require.Equal(t, tc.wantClass, gotClass)
			require.Equal(t, tc.wantHealth, gotHealth)
		})
	}
}
