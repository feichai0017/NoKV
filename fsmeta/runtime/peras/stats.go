// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

func (c *Runtime) Stats() map[string]any {
	if c == nil {
		return map[string]any{
			"commit_total":                               uint64(0),
			"flush_total":                                uint64(0),
			"segment_total":                              uint64(0),
			"seal_total":                                 uint64(0),
			"segment_operations_total":                   uint64(0),
			"segment_entries_total":                      uint64(0),
			"flush_latency_total_nanosecond":             uint64(0),
			"flush_latency_last_nanosecond":              uint64(0),
			"flush_latency_max_nanosecond":               uint64(0),
			"flush_latency_average_nanosecond":           uint64(0),
			"witness_latency_total_nanosecond":           uint64(0),
			"witness_latency_last_nanosecond":            uint64(0),
			"witness_latency_max_nanosecond":             uint64(0),
			"witness_latency_average_nanosecond":         uint64(0),
			"witness_batch_total":                        uint64(0),
			"witness_batch_records_total":                uint64(0),
			"witness_batch_records_last":                 uint64(0),
			"witness_batch_records_max":                  uint64(0),
			"witness_batch_bytes_total":                  uint64(0),
			"witness_batch_bytes_last":                   uint64(0),
			"witness_batch_bytes_max":                    uint64(0),
			"witness_replica_append_total":               uint64(0),
			"witness_replica_append_error_total":         uint64(0),
			"witness_replica_latency_total_nanosecond":   uint64(0),
			"witness_replica_latency_last_nanosecond":    uint64(0),
			"witness_replica_latency_max_nanosecond":     uint64(0),
			"witness_replica_latency_average_nanosecond": uint64(0),
			"witness_quorum_total":                       uint64(0),
			"witness_quorum_latency_total_nanosecond":    uint64(0),
			"witness_quorum_latency_last_nanosecond":     uint64(0),
			"witness_quorum_latency_max_nanosecond":      uint64(0),
			"witness_quorum_latency_average_nanosecond":  uint64(0),
			"witness_quorum_acks_last":                   uint64(0),
			"witness_quorum_acks_max":                    uint64(0),
			"install_latency_total_nanosecond":           uint64(0),
			"install_latency_last_nanosecond":            uint64(0),
			"install_latency_max_nanosecond":             uint64(0),
			"install_latency_average_nanosecond":         uint64(0),
			"install_payload_bytes_total":                uint64(0),
			"install_payload_bytes_last":                 uint64(0),
			"install_payload_bytes_max":                  uint64(0),
			"install_route_keys_total":                   uint64(0),
			"install_route_keys_last":                    uint64(0),
			"install_route_keys_max":                     uint64(0),
			"seal_latency_total_nanosecond":              uint64(0),
			"seal_latency_last_nanosecond":               uint64(0),
			"seal_latency_max_nanosecond":                uint64(0),
			"seal_latency_average_nanosecond":            uint64(0),
			"flush_batch_total":                          uint64(0),
			"flush_jobs_total":                           uint64(0),
			"flush_jobs_last":                            uint64(0),
			"flush_jobs_max":                             uint64(0),
			"last_segment_operations":                    uint64(0),
			"last_segment_input_mutations":               uint64(0),
			"last_segment_entries":                       uint64(0),
			"last_segment_coalesced":                     uint64(0),
			"last_segment_compression_x100":              uint64(0),
			"last_segment_root":                          [32]byte{},
			"last_error":                                 "",
			"error_total":                                uint64(0),
			"retry_total":                                uint64(0),
			"retry_unavailable_total":                    uint64(0),
			"retry_routing_total":                        uint64(0),
			"retry_stale_epoch_total":                    uint64(0),
			"retry_other_total":                          uint64(0),
			"background_skip_total":                      uint64(0),
			"background_error_total":                     uint64(0),
			"segment_catalog_load_total":                 uint64(0),
			"root_sealed_segment_total":                  uint64(0),
			"root_sealed_segment_missing_total":          uint64(0),
			"segment_recovery_install_total":             uint64(0),
			"segment_recovery_skip_total":                uint64(0),
			"visible_log_recover_total":                  uint64(0),
			"visible_log_recover_skip_total":             uint64(0),
			"visible_log_recover_old_epoch_total":        uint64(0),
			"visible_log_apply_marker_total":             uint64(0),
			"visible_log_apply_error_total":              uint64(0),
			"visible_log_enabled":                        false,
			"overlay_keys":                               0,
			"segment_keys":                               0,
			"overlay_directory_index_dirs":               0,
			"overlay_directory_index_dirty":              0,
			"segment_directory_index_dirs":               0,
			"segment_directory_index_dirty":              0,
			"predicate_known_keys":                       0,
			"predicate_empty_dirs":                       0,
			"predicate_empty_sessions":                   0,
			"holders":                                    0,
			"pending":                                    0,
			"segment_install_parallelism":                0,
			"segment_flush_parallelism":                  0,
			"background_flush_operation_limit":           0,
			"segment_install_queue_depth":                0,
			"segment_install_queue_capacity":             0,
			"segment_seal_queue_depth":                   0,
			"segment_seal_queue_capacity":                0,
			"witness_count":                              0,
			"quorum":                                     0,
		}
	}
	var overlayKeys, knownKeys, emptyDirs, emptySessions int
	var segmentKeys int
	var overlayDirs, overlayDirty, segmentDirs, segmentDirty int
	if c.read != nil {
		overlayKeys, knownKeys, emptyDirs, emptySessions = c.read.overlay.Stats()
		segmentKeys, _, _, _ = c.read.sealed.Stats()
		overlayDirs, overlayDirty = c.read.overlay.ReadIndexStats()
		segmentDirs, segmentDirty = c.read.sealed.ReadIndexStats()
	}
	holders, pending := 0, 0
	if c.epochs != nil {
		holders, pending = c.epochs.stats()
	}
	c.metrics.statsMu.RLock()
	lastSegmentStats := c.metrics.lastSegmentStats
	lastSegmentRoot := c.metrics.lastSegmentRoot
	lastError := c.metrics.lastError
	c.metrics.statsMu.RUnlock()
	installQueueDepth := 0
	installQueueCapacity := 0
	if c.installQ != nil {
		installQueueDepth = c.installQ.depth()
		installQueueCapacity = c.installQ.capacity()
	}
	sealQueueDepth := 0
	sealQueueCapacity := 0
	if c.sealQ != nil {
		sealQueueDepth = c.sealQ.depth()
		sealQueueCapacity = c.sealQ.capacity()
	}
	flushTotal := c.metrics.flushTotal.Load()
	sealTotal := c.metrics.sealTotal.Load()
	flushLatencyTotal := c.metrics.flushLatencyTotal.Load()
	witnessLatencyTotal := c.metrics.witnessLatencyTotal.Load()
	witnessReplicaAppendTotal := c.metrics.witnessReplicaAppendTotal.Load()
	witnessReplicaLatencyTotal := c.metrics.witnessReplicaLatencyTotal.Load()
	witnessQuorumTotal := c.metrics.witnessQuorumTotal.Load()
	witnessQuorumLatencyTotal := c.metrics.witnessQuorumLatencyTotal.Load()
	installLatencyTotal := c.metrics.installLatencyTotal.Load()
	sealLatencyTotal := c.metrics.sealLatencyTotal.Load()
	return map[string]any{
		"commit_total":                               c.metrics.commitTotal.Load(),
		"flush_total":                                flushTotal,
		"segment_total":                              c.metrics.segmentTotal.Load(),
		"seal_total":                                 sealTotal,
		"segment_operations_total":                   c.metrics.segmentOpsTotal.Load(),
		"segment_entries_total":                      c.metrics.segmentEntryTotal.Load(),
		"flush_latency_total_nanosecond":             flushLatencyTotal,
		"flush_latency_last_nanosecond":              c.metrics.flushLatencyLast.Load(),
		"flush_latency_max_nanosecond":               c.metrics.flushLatencyMax.Load(),
		"flush_latency_average_nanosecond":           averagePerasDuration(flushLatencyTotal, flushTotal),
		"witness_latency_total_nanosecond":           witnessLatencyTotal,
		"witness_latency_last_nanosecond":            c.metrics.witnessLatencyLast.Load(),
		"witness_latency_max_nanosecond":             c.metrics.witnessLatencyMax.Load(),
		"witness_latency_average_nanosecond":         averagePerasDuration(witnessLatencyTotal, flushTotal),
		"witness_batch_total":                        c.metrics.witnessBatchTotal.Load(),
		"witness_batch_records_total":                c.metrics.witnessBatchRecordsTotal.Load(),
		"witness_batch_records_last":                 c.metrics.witnessBatchRecordsLast.Load(),
		"witness_batch_records_max":                  c.metrics.witnessBatchRecordsMax.Load(),
		"witness_batch_bytes_total":                  c.metrics.witnessBatchBytesTotal.Load(),
		"witness_batch_bytes_last":                   c.metrics.witnessBatchBytesLast.Load(),
		"witness_batch_bytes_max":                    c.metrics.witnessBatchBytesMax.Load(),
		"witness_replica_append_total":               witnessReplicaAppendTotal,
		"witness_replica_append_error_total":         c.metrics.witnessReplicaAppendErrorTotal.Load(),
		"witness_replica_latency_total_nanosecond":   witnessReplicaLatencyTotal,
		"witness_replica_latency_last_nanosecond":    c.metrics.witnessReplicaLatencyLast.Load(),
		"witness_replica_latency_max_nanosecond":     c.metrics.witnessReplicaLatencyMax.Load(),
		"witness_replica_latency_average_nanosecond": averagePerasDuration(witnessReplicaLatencyTotal, witnessReplicaAppendTotal),
		"witness_quorum_total":                       witnessQuorumTotal,
		"witness_quorum_latency_total_nanosecond":    witnessQuorumLatencyTotal,
		"witness_quorum_latency_last_nanosecond":     c.metrics.witnessQuorumLatencyLast.Load(),
		"witness_quorum_latency_max_nanosecond":      c.metrics.witnessQuorumLatencyMax.Load(),
		"witness_quorum_latency_average_nanosecond":  averagePerasDuration(witnessQuorumLatencyTotal, witnessQuorumTotal),
		"witness_quorum_acks_last":                   c.metrics.witnessQuorumAcksLast.Load(),
		"witness_quorum_acks_max":                    c.metrics.witnessQuorumAcksMax.Load(),
		"install_latency_total_nanosecond":           installLatencyTotal,
		"install_latency_last_nanosecond":            c.metrics.installLatencyLast.Load(),
		"install_latency_max_nanosecond":             c.metrics.installLatencyMax.Load(),
		"install_latency_average_nanosecond":         averagePerasDuration(installLatencyTotal, flushTotal),
		"install_payload_bytes_total":                c.metrics.installPayloadTotal.Load(),
		"install_payload_bytes_last":                 c.metrics.installPayloadLast.Load(),
		"install_payload_bytes_max":                  c.metrics.installPayloadMax.Load(),
		"install_route_keys_total":                   c.metrics.installRoutesTotal.Load(),
		"install_route_keys_last":                    c.metrics.installRoutesLast.Load(),
		"install_route_keys_max":                     c.metrics.installRoutesMax.Load(),
		"seal_latency_total_nanosecond":              sealLatencyTotal,
		"seal_latency_last_nanosecond":               c.metrics.sealLatencyLast.Load(),
		"seal_latency_max_nanosecond":                c.metrics.sealLatencyMax.Load(),
		"seal_latency_average_nanosecond":            averagePerasDuration(sealLatencyTotal, sealTotal),
		"flush_batch_total":                          c.metrics.flushBatchTotal.Load(),
		"flush_jobs_total":                           c.metrics.flushJobTotal.Load(),
		"flush_jobs_last":                            c.metrics.flushJobLast.Load(),
		"flush_jobs_max":                             c.metrics.flushJobMax.Load(),
		"last_segment_operations":                    lastSegmentStats.OperationCount,
		"last_segment_input_mutations":               lastSegmentStats.InputMutationCount,
		"last_segment_entries":                       lastSegmentStats.EntryCount,
		"last_segment_coalesced":                     lastSegmentStats.CoalescedMutations,
		"last_segment_compression_x100":              uint64(lastSegmentStats.CompressionRatio * 100),
		"last_segment_root":                          lastSegmentRoot,
		"last_error":                                 lastError,
		"error_total":                                c.metrics.errorTotal.Load(),
		"retry_total":                                c.metrics.retryTotal.Load(),
		"retry_unavailable_total":                    c.metrics.retryUnavailable.Load(),
		"retry_routing_total":                        c.metrics.retryRouting.Load(),
		"retry_stale_epoch_total":                    c.metrics.retryStaleEpoch.Load(),
		"retry_other_total":                          c.metrics.retryOther.Load(),
		"background_skip_total":                      c.metrics.bgSkipTotal.Load(),
		"background_error_total":                     c.metrics.bgErrorTotal.Load(),
		"segment_catalog_load_total":                 c.metrics.catalogLoadTotal.Load(),
		"root_sealed_segment_total":                  c.metrics.rootSealTotal.Load(),
		"root_sealed_segment_missing_total":          c.metrics.rootSealMissingTotal.Load(),
		"segment_recovery_install_total":             c.metrics.recoveryInstallTotal.Load(),
		"segment_recovery_skip_total":                c.metrics.recoverySkipTotal.Load(),
		"visible_log_recover_total":                  c.metrics.visibleLogRecoverTotal.Load(),
		"visible_log_recover_skip_total":             c.metrics.visibleLogRecoverSkipTotal.Load(),
		"visible_log_recover_old_epoch_total":        c.metrics.visibleLogRecoverOldEpochTotal.Load(),
		"visible_log_apply_marker_total":             c.metrics.visibleLogApplyMarkerTotal.Load(),
		"visible_log_apply_error_total":              c.metrics.visibleLogApplyErrorTotal.Load(),
		"visible_log_enabled":                        c.visibleLog != nil,
		"visible_log_policy":                         c.visibleLogPolicy(),
		"overlay_keys":                               overlayKeys,
		"segment_keys":                               segmentKeys,
		"overlay_directory_index_dirs":               overlayDirs,
		"overlay_directory_index_dirty":              overlayDirty,
		"segment_directory_index_dirs":               segmentDirs,
		"segment_directory_index_dirty":              segmentDirty,
		"predicate_known_keys":                       knownKeys,
		"predicate_empty_dirs":                       emptyDirs,
		"predicate_empty_sessions":                   emptySessions,
		"holders":                                    holders,
		"pending":                                    pending,
		"segment_install_parallelism":                c.installN,
		"segment_flush_parallelism":                  c.flushN,
		"background_flush_operation_limit":           c.backgroundFlushMaxOpsPerHolder(),
		"segment_install_queue_depth":                installQueueDepth,
		"segment_install_queue_capacity":             installQueueCapacity,
		"segment_seal_queue_depth":                   sealQueueDepth,
		"segment_seal_queue_capacity":                sealQueueCapacity,
		"witness_count":                              len(c.witnesses),
		"quorum":                                     c.quorum,
	}
}

func (c *Runtime) visibleLogPolicy() string {
	if c == nil || c.visibleLog == nil {
		return "disabled"
	}
	reporter, ok := c.visibleLog.(interface{ VisibleLogPolicy() string })
	if !ok {
		return "custom"
	}
	return reporter.VisibleLogPolicy()
}
