// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/tso"
	"github.com/feichai0017/NoKV/meta/topology"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

func TestServiceDiagnosticsSnapshotIncludesEunomiaMetrics(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.eunomiaMetrics.recordGrantEraTransition(1, 2)
	svc.eunomiaMetrics.recordGateRejection(gateDutyAdmission)
	svc.eunomiaMetrics.recordGuaranteeViolation(guaranteeInheritance)

	metrics := svc.DiagnosticsSnapshot()["eunomia_metrics"].(map[string]any)
	require.Equal(t, uint64(1), metrics["grant_era_transitions_total"])
	require.Equal(t, map[string]any{
		"duty_admission": uint64(1),
	}, metrics["gate_rejections_total"])
	require.Equal(t, map[string]any{
		"primacy":     uint64(0),
		"inheritance": uint64(1),
		"silence":     uint64(0),
		"finality":    uint64(0),
	}, metrics["guarantee_violations_total"])
}

func TestServiceValidateGrantRecordsDutyRejectionMetric(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	err := svc.validateGateGrant(gateDutyAdmission, rootproto.DutyAllocID, rootproto.AuthorityGrant{
		GrantID:         "g1",
		HolderID:        "c1",
		ExpiresUnixNano: time.Unix(0, 200).UnixNano(),
		Era:             3,
		Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 10)},
	})
	require.Error(t, err)

	metrics := svc.DiagnosticsSnapshot()["eunomia_metrics"].(map[string]any)
	require.Equal(t, map[string]any{
		"duty_admission": uint64(1),
	}, metrics["gate_rejections_total"])
	require.Equal(t, map[string]any{
		"primacy":     uint64(0),
		"inheritance": uint64(0),
		"silence":     uint64(0),
		"finality":    uint64(0),
	}, metrics["guarantee_violations_total"])
}

func TestServiceAdmitDutyRejectsGrantAtRetiredEraFloor(t *testing.T) {
	now := time.Unix(100, 0)
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.now = func() time.Time { return now }
	svc.ConfigureAuthorityGrant("coord-1", time.Hour, 10*time.Minute)
	svc.refreshGrantMirror(rootview.Snapshot{
		ActiveGrants: []rootproto.AuthorityGrant{{
			GrantID:         "coord-1/tso/12",
			HolderID:        "coord-1",
			Era:             12,
			ExpiresUnixNano: now.Add(time.Hour).UnixNano(),
			Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 100)},
		}},
		RetiredEraFloors: []rootproto.AuthorityRetiredEraFloor{{
			DutyID:          rootproto.DutyTSO,
			Scope:           rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
			RetiredEraFloor: 12,
		}},
	})

	_, err := svc.admitDutyFromCachedGrant(rootproto.DutyTSO)

	require.Error(t, err)
	require.Contains(t, err.Error(), "silence violated")
	require.Contains(t, err.Error(), "retired_floor=12")
}

func TestServiceDiagnosticsMarksInheritedGrantFinality(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Allocator: rootview.AllocatorState{
				IDCurrent: 12,
				TSCurrent: 34,
			},
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/3",
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 20_000).UnixNano(),
				Era:             3,
				Duties: []rootproto.DutyGrant{
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20),
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 40),
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 7, 0),
				},
			}},
			RetiredGrants: []rootproto.GrantRetirement{
				{
					GrantID:            "c0/2",
					HolderID:           "c0",
					Era:                2,
					Mode:               rootproto.GrantRetirementSealedExact,
					Bounds:             []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 12)},
					InheritedByGrantID: "c1/3",
				},
			},
			Descriptors: rootCloneDescriptorsForTest(map[uint64]topology.Descriptor{
				1: {RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			}),
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	audit := svc.DiagnosticsSnapshot()["audit"].(map[string]any)
	require.Equal(t, true, audit["sealed_exact_completed"])
	require.Equal(t, false, audit["retired_not_inherited"])
	require.Equal(t, false, audit["invalid_successor_bound"])
}

func TestServiceEnsureGrantRecordsCoverageViolationMetric(t *testing.T) {
	store := &fakeStorage{
		leader:      true,
		campaignErr: rootstate.ErrInheritance,
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)

	metrics := svc.DiagnosticsSnapshot()["eunomia_metrics"].(map[string]any)
	require.Equal(t, map[string]any{
		"primacy":     uint64(0),
		"inheritance": uint64(1),
		"silence":     uint64(0),
		"finality":    uint64(0),
	}, metrics["guarantee_violations_total"])
}
