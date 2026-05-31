// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

func (s *Service) metadataRetentionOperations(ctx context.Context, regions []catalog.RegionStats) []*coordpb.SchedulerOperation {
	if s == nil || len(regions) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return nil
	}
	snapshot, err := s.currentRootSnapshot()
	if err != nil {
		return nil
	}
	retention := snapshot.SnapshotRetentionIndex()
	if !retention.Active() || retention.GlobalFloor == 0 {
		return nil
	}

	// Coordinator intentionally uses the global floor here. Per-mount floors
	// are more aggressive, but deriving mount identity from region key ranges
	// would couple the control plane to fsmeta key layout. The global floor is
	// root-derived and conservative for every hosted metadata region.
	seen := make(map[uint64]struct{}, len(regions))
	ops := make([]*coordpb.SchedulerOperation, 0, len(regions))
	for _, region := range regions {
		if region.RegionID == 0 {
			continue
		}
		if _, ok := seen[region.RegionID]; ok {
			continue
		}
		seen[region.RegionID] = struct{}{}
		ops = append(ops, &coordpb.SchedulerOperation{
			Type:           coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_PRUNE_METADATA_VERSIONS,
			RegionId:       region.RegionID,
			RetentionFloor: retention.GlobalFloor,
		})
	}
	return ops
}
