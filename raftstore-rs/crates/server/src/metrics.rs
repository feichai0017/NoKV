//! HTTP diagnostics for the standalone Rust raftstore server.

use std::net::SocketAddr;

use nokv_holtstore::HoltMvccStore;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_raftnode::{RegionSnapshotEngine, RegionTrafficProvider};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::{coordinator_heartbeat_request_for_hosted_regions, HostedRegionRegistry};

pub(super) fn spawn_metrics_server<E>(
    metrics_addr: Option<SocketAddr>,
    store_id: u64,
    addr: SocketAddr,
    registry: HostedRegionRegistry<E>,
    root_events: Option<HoltMvccStore>,
) where
    E: Clone + RegionSnapshotEngine + RegionTrafficProvider + Send + Sync + 'static,
{
    let Some(metrics_addr) = metrics_addr else {
        return;
    };
    tokio::spawn(async move {
        if let Err(err) =
            run_metrics_server(metrics_addr, store_id, addr, registry, root_events).await
        {
            tracing::warn!(%metrics_addr, error = %err, "rust raftstore metrics server stopped");
        }
    });
}

async fn run_metrics_server<E>(
    metrics_addr: SocketAddr,
    store_id: u64,
    addr: SocketAddr,
    registry: HostedRegionRegistry<E>,
    root_events: Option<HoltMvccStore>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: Clone + RegionSnapshotEngine + RegionTrafficProvider + Send + Sync + 'static,
{
    let listener = TcpListener::bind(metrics_addr).await?;
    tracing::info!(%metrics_addr, "rust raftstore metrics server listening");
    loop {
        let (mut stream, _) = listener.accept().await?;
        let registry = registry.clone();
        let root_events = root_events.clone();
        tokio::spawn(async move {
            let mut request = [0_u8; 1024];
            let read = match stream.read(&mut request).await {
                Ok(read) => read,
                Err(err) => {
                    tracing::debug!(error = %err, "rust raftstore metrics read failed");
                    return;
                }
            };
            let request = String::from_utf8_lossy(&request[..read]);
            let (status, body) = if request.starts_with("GET /debug/vars ") {
                (
                    "200 OK",
                    rust_metrics_payload(store_id, addr, &registry, root_events.as_ref()),
                )
            } else {
                ("404 Not Found", "{}\n".to_owned())
            };
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            if let Err(err) = stream.write_all(response.as_bytes()).await {
                tracing::debug!(error = %err, "rust raftstore metrics write failed");
            }
        });
    }
}

fn rust_metrics_payload<E>(
    store_id: u64,
    addr: SocketAddr,
    registry: &HostedRegionRegistry<E>,
    root_events: Option<&HoltMvccStore>,
) -> String
where
    E: Clone + RegionSnapshotEngine + RegionTrafficProvider,
{
    let heartbeat =
        coordinator_heartbeat_request_for_hosted_regions(store_id, addr, registry, root_events);
    match heartbeat {
        Ok(heartbeat) => rust_metrics_payload_from_heartbeat(&heartbeat),
        Err(err) => format!(
            "{{\"nokv_raftstore_rs\":{{\"store_id\":{store_id},\"error\":\"{}\"}}}}\n",
            json_escape(&err)
        ),
    }
}

fn rust_metrics_payload_from_heartbeat(heartbeat: &coordpb::StoreHeartbeatRequest) -> String {
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
        "{{\"nokv_raftstore_rs\":{{\"store_id\":{},\"client_addr\":\"{}\",\"raft_addr\":\"{}\",\"region_num\":{},\"leader_num\":{},\"leader_region_ids\":{},\"regions\":[{}]}}}}\n",
        heartbeat.store_id,
        json_escape(&heartbeat.client_addr),
        json_escape(&heartbeat.raft_addr),
        heartbeat.region_num,
        heartbeat.leader_num,
        json_u64_array(&heartbeat.leader_region_ids),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_metrics_payload_uses_expvar_compatible_root_key() {
        let payload = rust_metrics_payload_from_heartbeat(&coordpb::StoreHeartbeatRequest {
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
        });

        assert!(payload.starts_with("{\"nokv_raftstore_rs\""));
        assert!(payload.contains("\"store_id\":7"));
        assert!(payload.contains("\"region_num\":2"));
        assert!(payload.contains("\"leader_region_ids\":[11]"));
        assert!(payload.contains("\"pending_admin\":true"));
        assert!(payload.ends_with('\n'));
    }
}
