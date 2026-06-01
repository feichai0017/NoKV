//! HTTP diagnostics for the standalone Rust raftstore server.

use std::net::SocketAddr;

use nokv_holtstore::{
    holt_metadata_metrics_snapshot, HoltMetadataMetricsSnapshot, HoltMetadataStore,
};
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_raftnode::{
    raftnode_metrics_snapshot, RaftNodeMetricsSnapshot, RegionSnapshotEngine, RegionTrafficProvider,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::coordinator::coordinator_heartbeat_request_for_hosted_regions;
use crate::hosted_region::HostedRegionRegistry;
use nokv_raftstore_server::{metadata_batch_metrics_snapshot, MetadataBatchMetricsSnapshot};

pub(super) fn spawn_metrics_server<E>(
    metrics_addr: Option<SocketAddr>,
    store_id: u64,
    advertised_addr: String,
    registry: HostedRegionRegistry<E>,
    root_events: Option<HoltMetadataStore>,
) where
    E: Clone + RegionSnapshotEngine + RegionTrafficProvider + Send + Sync + 'static,
{
    let Some(metrics_addr) = metrics_addr else {
        return;
    };
    tokio::spawn(async move {
        if let Err(err) = run_metrics_server(
            metrics_addr,
            store_id,
            advertised_addr,
            registry,
            root_events,
        )
        .await
        {
            tracing::warn!(%metrics_addr, error = %err, "raftstore metrics server stopped");
        }
    });
}

async fn run_metrics_server<E>(
    metrics_addr: SocketAddr,
    store_id: u64,
    advertised_addr: String,
    registry: HostedRegionRegistry<E>,
    root_events: Option<HoltMetadataStore>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: Clone + RegionSnapshotEngine + RegionTrafficProvider + Send + Sync + 'static,
{
    let listener = TcpListener::bind(metrics_addr).await?;
    tracing::info!(%metrics_addr, "raftstore metrics server listening");
    loop {
        let (mut stream, _) = listener.accept().await?;
        let registry = registry.clone();
        let root_events = root_events.clone();
        let advertised_addr = advertised_addr.clone();
        tokio::spawn(async move {
            let mut request = [0_u8; 1024];
            let read = match stream.read(&mut request).await {
                Ok(read) => read,
                Err(err) => {
                    tracing::debug!(error = %err, "raftstore metrics read failed");
                    return;
                }
            };
            let request = String::from_utf8_lossy(&request[..read]);
            let (status, body) = if request.starts_with("GET /debug/vars ") {
                (
                    "200 OK",
                    metrics_payload(store_id, &advertised_addr, &registry, root_events.as_ref()),
                )
            } else {
                ("404 Not Found", "{}\n".to_owned())
            };
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            if let Err(err) = stream.write_all(response.as_bytes()).await {
                tracing::debug!(error = %err, "raftstore metrics write failed");
            }
        });
    }
}

fn metrics_payload<E>(
    store_id: u64,
    advertised_addr: &str,
    registry: &HostedRegionRegistry<E>,
    root_events: Option<&HoltMetadataStore>,
) -> String
where
    E: Clone + RegionSnapshotEngine + RegionTrafficProvider,
{
    let heartbeat = coordinator_heartbeat_request_for_hosted_regions(
        store_id,
        advertised_addr,
        registry,
        root_events,
    );
    match heartbeat {
        Ok(heartbeat) => metrics_payload_from_heartbeat(
            &heartbeat,
            metadata_batch_metrics_snapshot(),
            raftnode_metrics_snapshot(),
            holt_metadata_metrics_snapshot(),
        ),
        Err(err) => format!(
            "{{\"nokv_raftstore\":{{\"store_id\":{store_id},\"error\":\"{}\"}}}}\n",
            json_escape(&err)
        ),
    }
}

fn metrics_payload_from_heartbeat(
    heartbeat: &coordpb::StoreHeartbeatRequest,
    metadata_batch: MetadataBatchMetricsSnapshot,
    raftnode: RaftNodeMetricsSnapshot,
    holt_metadata: HoltMetadataMetricsSnapshot,
) -> String {
    let mut regions = String::new();
    for (index, stats) in heartbeat.region_stats.iter().enumerate() {
        if index > 0 {
            regions.push(',');
        }
        regions.push_str(&format!(
            "{{\"region_id\":{},\"leader_store_id\":{},\"read_qps\":{},\"write_qps\":{},\"write_bytes_per_sec\":{},\"atomic_mutate_qps\":{},\"pending_admin\":{}}}",
            stats.region_id,
            stats.leader_store_id,
            stats.read_qps,
            stats.write_qps,
            stats.write_bytes_per_sec,
            stats.atomic_mutate_qps,
            stats.pending_admin
        ));
    }
    format!(
        "{{\"nokv_raftstore\":{{\"store_id\":{},\"client_addr\":\"{}\",\"raft_addr\":\"{}\",\"region_num\":{},\"leader_num\":{},\"leader_region_ids\":{},\"metadata_commit_batch\":{},\"raftnode\":{},\"holt_metadata\":{},\"regions\":[{}]}}}}\n",
        heartbeat.store_id,
        json_escape(&heartbeat.client_addr),
        json_escape(&heartbeat.raft_addr),
        heartbeat.region_num,
        heartbeat.leader_num,
        json_u64_array(&heartbeat.leader_region_ids),
        metadata_batch_json(metadata_batch),
        raftnode_json(raftnode),
        holt_metadata_json(holt_metadata),
        regions
    )
}

fn json_u64_array(values: &[u64]) -> String {
    let mut out = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&value.to_string());
    }
    out.push(']');
    out
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}

fn metadata_batch_json(metrics: MetadataBatchMetricsSnapshot) -> String {
    format!(
        "{{\"requests_total\":{},\"queue_depth_current\":{},\"queue_depth_max\":{},\"enqueue_block_ns_total\":{},\"enqueue_block_ns_max\":{},\"batches_total\":{},\"batch_commands_total\":{},\"batch_commands_max\":{},\"batch_wait_ns_total\":{},\"batch_wait_ns_max\":{},\"inflight_batches_current\":{},\"inflight_batches_max\":{},\"pipeline_block_ns_total\":{},\"pipeline_block_ns_max\":{},\"batch_execute_ns_total\":{},\"batch_execute_ns_max\":{}}}",
        metrics.requests_total,
        metrics.queue_depth_current,
        metrics.queue_depth_max,
        metrics.enqueue_block_ns_total,
        metrics.enqueue_block_ns_max,
        metrics.batches_total,
        metrics.batch_commands_total,
        metrics.batch_commands_max,
        metrics.batch_wait_ns_total,
        metrics.batch_wait_ns_max,
        metrics.inflight_batches_current,
        metrics.inflight_batches_max,
        metrics.pipeline_block_ns_total,
        metrics.pipeline_block_ns_max,
        metrics.batch_execute_ns_total,
        metrics.batch_execute_ns_max
    )
}

fn raftnode_json(metrics: RaftNodeMetricsSnapshot) -> String {
    format!(
        "{{\"proposals_total\":{},\"proposal_commands_total\":{},\"proposal_commands_max\":{},\"proposal_ns_total\":{},\"proposal_ns_max\":{},\"read_barrier_requests_total\":{},\"read_barrier_shared_total\":{},\"read_barrier_executions_total\":{},\"read_barrier_errors_total\":{},\"read_barrier_ns_total\":{},\"read_barrier_ns_max\":{},\"log_append_calls_total\":{},\"log_entries_total\":{},\"log_entries_max\":{},\"log_append_ns_total\":{},\"log_append_ns_max\":{},\"log_flush_skipped_total\":{},\"log_group_flush_calls_total\":{},\"log_group_flush_entries_total\":{},\"log_group_flush_entries_max\":{},\"log_group_flush_callbacks_total\":{},\"log_group_flush_callbacks_max\":{},\"log_group_flush_errors_total\":{},\"log_sync_ns_total\":{},\"log_sync_ns_max\":{},\"state_machine_apply_calls_total\":{},\"state_machine_apply_entries_total\":{},\"state_machine_apply_entries_max\":{},\"state_machine_apply_ns_total\":{},\"state_machine_apply_ns_max\":{},\"metadata_apply_batches_total\":{},\"metadata_apply_commands_total\":{},\"metadata_apply_commands_max\":{},\"metadata_apply_ns_total\":{},\"metadata_apply_ns_max\":{},\"append_entries_client_calls_total\":{},\"append_entries_client_entries_total\":{},\"append_entries_client_entries_max\":{},\"append_entries_client_ns_total\":{},\"append_entries_client_ns_max\":{},\"append_entries_client_empty_calls_total\":{},\"append_entries_client_empty_ns_total\":{},\"append_entries_client_data_calls_total\":{},\"append_entries_client_data_ns_total\":{},\"append_entries_client_error_calls_total\":{},\"append_entries_client_error_ns_total\":{},\"append_entries_server_calls_total\":{},\"append_entries_server_entries_total\":{},\"append_entries_server_entries_max\":{},\"append_entries_server_ns_total\":{},\"append_entries_server_ns_max\":{},\"append_entries_server_empty_calls_total\":{},\"append_entries_server_empty_ns_total\":{},\"append_entries_server_data_calls_total\":{},\"append_entries_server_data_ns_total\":{}}}",
        metrics.proposals_total,
        metrics.proposal_commands_total,
        metrics.proposal_commands_max,
        metrics.proposal_ns_total,
        metrics.proposal_ns_max,
        metrics.read_barrier_requests_total,
        metrics.read_barrier_shared_total,
        metrics.read_barrier_executions_total,
        metrics.read_barrier_errors_total,
        metrics.read_barrier_ns_total,
        metrics.read_barrier_ns_max,
        metrics.log_append_calls_total,
        metrics.log_entries_total,
        metrics.log_entries_max,
        metrics.log_append_ns_total,
        metrics.log_append_ns_max,
        metrics.log_flush_skipped_total,
        metrics.log_group_flush_calls_total,
        metrics.log_group_flush_entries_total,
        metrics.log_group_flush_entries_max,
        metrics.log_group_flush_callbacks_total,
        metrics.log_group_flush_callbacks_max,
        metrics.log_group_flush_errors_total,
        metrics.log_sync_ns_total,
        metrics.log_sync_ns_max,
        metrics.state_machine_apply_calls_total,
        metrics.state_machine_apply_entries_total,
        metrics.state_machine_apply_entries_max,
        metrics.state_machine_apply_ns_total,
        metrics.state_machine_apply_ns_max,
        metrics.metadata_apply_batches_total,
        metrics.metadata_apply_commands_total,
        metrics.metadata_apply_commands_max,
        metrics.metadata_apply_ns_total,
        metrics.metadata_apply_ns_max,
        metrics.append_entries_client_calls_total,
        metrics.append_entries_client_entries_total,
        metrics.append_entries_client_entries_max,
        metrics.append_entries_client_ns_total,
        metrics.append_entries_client_ns_max,
        metrics.append_entries_client_empty_calls_total,
        metrics.append_entries_client_empty_ns_total,
        metrics.append_entries_client_data_calls_total,
        metrics.append_entries_client_data_ns_total,
        metrics.append_entries_client_error_calls_total,
        metrics.append_entries_client_error_ns_total,
        metrics.append_entries_server_calls_total,
        metrics.append_entries_server_entries_total,
        metrics.append_entries_server_entries_max,
        metrics.append_entries_server_ns_total,
        metrics.append_entries_server_ns_max,
        metrics.append_entries_server_empty_calls_total,
        metrics.append_entries_server_empty_ns_total,
        metrics.append_entries_server_data_calls_total,
        metrics.append_entries_server_data_ns_total
    )
}

fn holt_metadata_json(metrics: HoltMetadataMetricsSnapshot) -> String {
    format!(
        "{{\"commit_batches_total\":{},\"commit_commands_total\":{},\"commit_commands_max\":{},\"commit_writes_total\":{},\"commit_writes_max\":{},\"prepare_ns_total\":{},\"prepare_ns_max\":{},\"atomic_ns_total\":{},\"atomic_ns_max\":{},\"total_ns_total\":{},\"total_ns_max\":{}}}",
        metrics.commit_batches_total,
        metrics.commit_commands_total,
        metrics.commit_commands_max,
        metrics.commit_writes_total,
        metrics.commit_writes_max,
        metrics.prepare_ns_total,
        metrics.prepare_ns_max,
        metrics.atomic_ns_total,
        metrics.atomic_ns_max,
        metrics.total_ns_total,
        metrics.total_ns_max
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_payload_uses_expvar_compatible_root_key() {
        let payload = metrics_payload_from_heartbeat(
            &coordpb::StoreHeartbeatRequest {
                store_id: 7,
                region_num: 2,
                leader_num: 1,
                leader_region_ids: vec![11],
                client_addr: "127.0.0.1:20170".to_owned(),
                raft_addr: "127.0.0.1:20170".to_owned(),
                region_stats: vec![coordpb::RegionRuntimeStats {
                    region_id: 11,
                    leader_store_id: 7,
                    read_qps: 3,
                    write_qps: 2,
                    write_bytes_per_sec: 128,
                    atomic_mutate_qps: 1,
                    pending_admin: true,
                    ..Default::default()
                }],
                ..Default::default()
            },
            MetadataBatchMetricsSnapshot {
                requests_total: 5,
                queue_depth_current: 0,
                queue_depth_max: 3,
                enqueue_block_ns_total: 7,
                enqueue_block_ns_max: 5,
                batches_total: 2,
                batch_commands_total: 5,
                batch_commands_max: 4,
                batch_wait_ns_total: 12,
                batch_wait_ns_max: 9,
                inflight_batches_current: 0,
                inflight_batches_max: 2,
                pipeline_block_ns_total: 4,
                pipeline_block_ns_max: 3,
                batch_execute_ns_total: 20,
                batch_execute_ns_max: 15,
            },
            RaftNodeMetricsSnapshot {
                proposals_total: 2,
                proposal_commands_total: 5,
                proposal_commands_max: 4,
                proposal_ns_total: 100,
                proposal_ns_max: 90,
                read_barrier_requests_total: 6,
                read_barrier_shared_total: 4,
                read_barrier_executions_total: 2,
                read_barrier_errors_total: 0,
                read_barrier_ns_total: 33,
                read_barrier_ns_max: 22,
                log_append_calls_total: 2,
                log_entries_total: 2,
                log_entries_max: 1,
                log_append_ns_total: 30,
                log_append_ns_max: 20,
                log_flush_skipped_total: 3,
                log_group_flush_calls_total: 4,
                log_group_flush_entries_total: 8,
                log_group_flush_entries_max: 5,
                log_group_flush_callbacks_total: 6,
                log_group_flush_callbacks_max: 3,
                log_group_flush_errors_total: 1,
                log_sync_ns_total: 40,
                log_sync_ns_max: 25,
                state_machine_apply_calls_total: 2,
                state_machine_apply_entries_total: 2,
                state_machine_apply_entries_max: 1,
                state_machine_apply_ns_total: 50,
                state_machine_apply_ns_max: 30,
                metadata_apply_batches_total: 2,
                metadata_apply_commands_total: 5,
                metadata_apply_commands_max: 4,
                metadata_apply_ns_total: 60,
                metadata_apply_ns_max: 35,
                append_entries_client_calls_total: 3,
                append_entries_client_entries_total: 8,
                append_entries_client_entries_max: 5,
                append_entries_client_ns_total: 70,
                append_entries_client_ns_max: 40,
                append_entries_client_empty_calls_total: 1,
                append_entries_client_empty_ns_total: 10,
                append_entries_client_data_calls_total: 2,
                append_entries_client_data_ns_total: 60,
                append_entries_client_error_calls_total: 0,
                append_entries_client_error_ns_total: 0,
                append_entries_server_calls_total: 3,
                append_entries_server_entries_total: 8,
                append_entries_server_entries_max: 5,
                append_entries_server_ns_total: 80,
                append_entries_server_ns_max: 45,
                append_entries_server_empty_calls_total: 1,
                append_entries_server_empty_ns_total: 11,
                append_entries_server_data_calls_total: 2,
                append_entries_server_data_ns_total: 69,
            },
            HoltMetadataMetricsSnapshot {
                commit_batches_total: 2,
                commit_commands_total: 5,
                commit_commands_max: 4,
                commit_writes_total: 9,
                commit_writes_max: 7,
                prepare_ns_total: 11,
                prepare_ns_max: 8,
                atomic_ns_total: 12,
                atomic_ns_max: 9,
                total_ns_total: 23,
                total_ns_max: 17,
            },
        );

        assert!(payload.starts_with("{\"nokv_raftstore\""));
        assert!(payload.contains("\"store_id\":7"));
        assert!(payload.contains("\"region_num\":2"));
        assert!(payload.contains("\"leader_region_ids\":[11]"));
        assert!(payload.contains("\"metadata_commit_batch\""));
        assert!(payload.contains("\"raftnode\""));
        assert!(payload.contains("\"holt_metadata\""));
        assert!(payload.contains("\"requests_total\":5"));
        assert!(payload.contains("\"queue_depth_max\":3"));
        assert!(payload.contains("\"batch_commands_max\":4"));
        assert!(payload.contains("\"inflight_batches_max\":2"));
        assert!(payload.contains("\"pipeline_block_ns_total\":4"));
        assert!(payload.contains("\"proposal_commands_total\":5"));
        assert!(payload.contains("\"read_barrier_requests_total\":6"));
        assert!(payload.contains("\"read_barrier_shared_total\":4"));
        assert!(payload.contains("\"log_flush_skipped_total\":3"));
        assert!(payload.contains("\"log_group_flush_calls_total\":4"));
        assert!(payload.contains("\"log_group_flush_errors_total\":1"));
        assert!(payload.contains("\"append_entries_client_calls_total\":3"));
        assert!(payload.contains("\"append_entries_client_empty_calls_total\":1"));
        assert!(payload.contains("\"append_entries_client_data_calls_total\":2"));
        assert!(payload.contains("\"commit_writes_total\":9"));
        assert!(payload.contains("\"pending_admin\":true"));
        assert!(payload.ends_with('\n'));
    }
}
