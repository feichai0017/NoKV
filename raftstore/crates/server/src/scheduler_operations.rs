use nokv_holtstore::HoltMetadataStore;
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::coordinator::v1 as coordpb;

use crate::range_controller::HoltRangeController;

pub(crate) const MAX_PENDING_SCHEDULER_OPERATION_ATTEMPTS: u32 = 8;

pub(crate) async fn retry_pending_scheduler_operations(
    admin_endpoint: &str,
    pending_store: &HoltMetadataStore,
    range_controller: Option<&HoltRangeController>,
) {
    let pending = match pending_store.pending_scheduler_operations() {
        Ok(pending) => pending,
        Err(err) => {
            tracing::debug!(error = %err, "raftstore pending scheduler load failed");
            return;
        }
    };
    for item in pending {
        match execute_scheduler_operation(admin_endpoint, range_controller, &item.operation).await {
            Ok(SchedulerOperationOutcome::Applied)
            | Ok(SchedulerOperationOutcome::Invalid { .. }) => {
                if let Err(err) =
                    pending_store.clear_scheduler_operation_diagnostic(&item.operation)
                {
                    tracing::debug!(
                        error = %err,
                        "raftstore pending scheduler clear failed"
                    );
                    return;
                }
            }
            Ok(SchedulerOperationOutcome::Unsupported { kind, reason }) => {
                match record_pending_scheduler_operation_attempt(pending_store, &item.operation) {
                    Ok(attempts) if attempts >= MAX_PENDING_SCHEDULER_OPERATION_ATTEMPTS => {
                        if let Err(err) = pending_store.block_pending_scheduler_operation(
                            &item.operation,
                            attempts,
                            reason,
                        ) {
                            tracing::debug!(
                                error = %err,
                                "raftstore exhausted pending scheduler block failed"
                            );
                            return;
                        }
                        tracing::warn!(
                            ?kind,
                            %reason,
                            attempts,
                            "raftstore abandoned unsupported pending scheduler operation"
                        );
                        continue;
                    }
                    Ok(attempts) => {
                        tracing::debug!(
                            ?kind,
                            %reason,
                            attempts,
                            "raftstore pending scheduler operation still unsupported"
                        );
                    }
                    Err(err) => {
                        tracing::debug!(
                            error = %err,
                            "raftstore pending scheduler attempt update failed"
                        );
                        return;
                    }
                }
            }
            Err(err) => {
                match record_pending_scheduler_operation_attempt(pending_store, &item.operation) {
                    Ok(attempts) if attempts >= MAX_PENDING_SCHEDULER_OPERATION_ATTEMPTS => {
                        if let Err(block_err) = pending_store.block_pending_scheduler_operation(
                            &item.operation,
                            attempts,
                            &err.to_string(),
                        ) {
                            tracing::debug!(
                                error = %block_err,
                                "raftstore exhausted pending scheduler block failed"
                            );
                            return;
                        }
                        tracing::warn!(
                            error = %err,
                            attempts,
                            "raftstore abandoned pending scheduler operation after retry limit"
                        );
                        continue;
                    }
                    Ok(attempts) => {
                        tracing::debug!(
                            error = %err,
                            attempts,
                            "raftstore pending scheduler operation retry failed"
                        );
                    }
                    Err(update_err) => {
                        tracing::debug!(
                            error = %update_err,
                            "raftstore pending scheduler attempt update failed"
                        );
                    }
                }
                return;
            }
        }
    }
}

pub(crate) fn record_pending_scheduler_operation_attempt(
    store: &HoltMetadataStore,
    operation: &coordpb::SchedulerOperation,
) -> Result<u32, String> {
    store
        .increment_pending_scheduler_operation_attempts(operation)
        .map_err(|err| err.to_string())
}

pub(crate) fn record_scheduler_operation_outcome(
    store: Option<&HoltMetadataStore>,
    operation: &coordpb::SchedulerOperation,
    outcome: Result<SchedulerOperationOutcome, tonic::Status>,
) {
    match outcome {
        Ok(SchedulerOperationOutcome::Applied) => {
            clear_scheduler_operation_diagnostic(store, operation);
            tracing::debug!("raftstore applied coordinator operation");
        }
        Ok(SchedulerOperationOutcome::Invalid { reason }) => {
            clear_scheduler_operation_diagnostic(store, operation);
            tracing::debug!(
                %reason,
                "raftstore ignored invalid coordinator operation"
            );
        }
        Ok(SchedulerOperationOutcome::Unsupported { kind, reason }) => {
            record_pending_scheduler_operation(store, operation);
            tracing::warn!(
                ?kind,
                %reason,
                region_id = operation.region_id,
                source_peer_id = operation.source_peer_id,
                target_peer_id = operation.target_peer_id,
                source_region_id = operation.source_region_id,
                split_key_len = operation.split_key.len(),
                "raftstore received unsupported coordinator operation"
            );
        }
        Err(err) => {
            record_pending_scheduler_operation(store, operation);
            tracing::debug!(
                error = %err,
                "raftstore coordinator operation failed"
            );
        }
    }
}

pub(crate) fn record_pending_scheduler_operation(
    store: Option<&HoltMetadataStore>,
    operation: &coordpb::SchedulerOperation,
) {
    let Some(store) = store else {
        return;
    };
    if let Err(err) = store.record_pending_scheduler_operation(operation) {
        tracing::warn!(
            error = %err,
            region_id = operation.region_id,
            operation_type = operation.r#type,
            "raftstore failed to persist pending scheduler operation"
        );
    }
}

pub(crate) fn clear_scheduler_operation_diagnostic(
    store: Option<&HoltMetadataStore>,
    operation: &coordpb::SchedulerOperation,
) {
    let Some(store) = store else {
        return;
    };
    if let Err(err) = store.clear_scheduler_operation_diagnostic(operation) {
        tracing::warn!(
            error = %err,
            region_id = operation.region_id,
            operation_type = operation.r#type,
            "raftstore failed to clear scheduler operation diagnostic"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchedulerOperationOutcome {
    Applied,
    Invalid {
        reason: &'static str,
    },
    Unsupported {
        kind: coordpb::SchedulerOperationType,
        reason: &'static str,
    },
}

pub(crate) async fn execute_scheduler_operation(
    admin_endpoint: &str,
    range_controller: Option<&HoltRangeController>,
    operation: &coordpb::SchedulerOperation,
) -> Result<SchedulerOperationOutcome, tonic::Status> {
    let kind = coordpb::SchedulerOperationType::try_from(operation.r#type)
        .unwrap_or(coordpb::SchedulerOperationType::None);
    match kind {
        coordpb::SchedulerOperationType::LeaderTransfer => {
            if operation.region_id == 0
                || operation.source_peer_id == 0
                || operation.target_peer_id == 0
            {
                return Ok(SchedulerOperationOutcome::Invalid {
                    reason: "leader transfer requires region, source peer, and target peer",
                });
            }
            let mut client =
                adminpb::raft_admin_client::RaftAdminClient::connect(admin_endpoint.to_owned())
                    .await
                    .map_err(|err| tonic::Status::unavailable(err.to_string()))?;
            client
                .transfer_leader(adminpb::TransferLeaderRequest {
                    region_id: operation.region_id,
                    peer_id: operation.target_peer_id,
                })
                .await?;
            Ok(SchedulerOperationOutcome::Applied)
        }
        coordpb::SchedulerOperationType::SplitRegion => {
            if operation.region_id == 0
                || operation.split_key.is_empty()
                || operation
                    .split_child
                    .as_ref()
                    .is_none_or(|child| child.region_id == 0)
            {
                return Ok(SchedulerOperationOutcome::Invalid {
                    reason: "split requires region, split key, and child descriptor",
                });
            }
            if let Some(controller) = range_controller {
                return controller.execute_split(operation).await;
            }
            Ok(SchedulerOperationOutcome::Unsupported {
                kind,
                reason: "split execution is not implemented in raftstore yet",
            })
        }
        coordpb::SchedulerOperationType::MergeRegion => {
            if operation.region_id == 0 || operation.source_region_id == 0 {
                return Ok(SchedulerOperationOutcome::Invalid {
                    reason: "merge requires target region and source region",
                });
            }
            if let Some(controller) = range_controller {
                return controller.execute_merge(operation).await;
            }
            Ok(SchedulerOperationOutcome::Unsupported {
                kind,
                reason: "merge execution is not implemented in raftstore yet",
            })
        }
        coordpb::SchedulerOperationType::PruneMetadataVersions => {
            if operation.region_id == 0 || operation.retention_floor == 0 {
                return Ok(SchedulerOperationOutcome::Invalid {
                    reason: "metadata prune requires region and retention floor",
                });
            }
            let mut client =
                adminpb::raft_admin_client::RaftAdminClient::connect(admin_endpoint.to_owned())
                    .await
                    .map_err(|err| tonic::Status::unavailable(err.to_string()))?;
            client
                .prune_metadata_versions(adminpb::PruneMetadataVersionsRequest {
                    region_id: operation.region_id,
                    retention_floor: operation.retention_floor,
                })
                .await?;
            Ok(SchedulerOperationOutcome::Applied)
        }
        coordpb::SchedulerOperationType::None => Ok(SchedulerOperationOutcome::Invalid {
            reason: "scheduler operation type is none",
        }),
    }
}
