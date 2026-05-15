// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"fmt"

	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	"google.golang.org/grpc"
)

func buildPerasRuntime(
	ctx context.Context,
	lister witnessStoreLister,
	runner *Runner,
	router *fsmetawatch.Router,
	authority *runtimeperas.AuthorityManager,
	dialOpts []grpc.DialOption,
	opts Options,
) (*runtimeperas.Runtime, *witnessConnections, error) {
	witnessConns, err := buildWitnessConnections(ctx, lister, dialOpts, opts.PerasWitnessStoreIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("init peras witnesses: %w", err)
	}
	perasRuntime, err := runtimeperas.NewRuntime(runtimeperas.Config{
		Authority:                  authority,
		Witnesses:                  witnessConns.witnesses,
		Installer:                  newRaftstoreSegmentInstaller(runner, router),
		CatalogScanner:             raftstoreSegmentCatalogScanner{runner: runner},
		WatchPublisher:             router,
		VisibleLog:                 opts.PerasVisibleLog,
		Quorum:                     opts.PerasWitnessQuorum,
		SegmentWitnessRetries:      opts.PerasSegmentWitnessRetries,
		SegmentWitnessRetryBackoff: opts.PerasSegmentWitnessRetryBackoff,
		SegmentBatchSize:           opts.PerasSegmentBatchSize,
		AdmissionPendingLimit:      opts.PerasAdmissionPendingLimit,
		SegmentMaxReplayOperations: opts.PerasSegmentMaxReplayOperations,
		SegmentMaxReplayMutations:  opts.PerasSegmentMaxReplayMutations,
		SegmentMaxPayloadBytes:     opts.PerasSegmentMaxPayloadBytes,
		SegmentInstallParallelism:  opts.PerasSegmentInstallParallelism,
		SegmentFlushParallelism:    opts.PerasSegmentFlushParallelism,
		SegmentFlushEvery:          opts.PerasSegmentFlushEvery,
		BackgroundFlushTimeout:     opts.PerasBackgroundFlushTimeout,
		BackgroundErrorBackoff:     opts.PerasBackgroundErrorBackoff,
	})
	if err != nil {
		_ = witnessConns.Close()
		return nil, nil, fmt.Errorf("init peras runtime: %w", err)
	}
	return perasRuntime, witnessConns, nil
}
