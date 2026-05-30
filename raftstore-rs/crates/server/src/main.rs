//! Standalone Rust raftstore server entrypoint for local compatibility tests.

use std::net::SocketAddr;
use std::path::PathBuf;

use nokv_holtstore::HoltMvccStore;
use nokv_mvcc::MvccStore;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::{
    AppliedKvEngine, OpenRaftRegion, PersistentAppliedKvEngine, RegionLogStorage,
    RegionSnapshotEngine, RegionStateMachine, SegmentedEntryLog, TonicRaftNetworkFactory,
};
use nokv_raftstore_server::{
    apply_status_from_holt, HoltApplyStatusSink, HoltRegionDescriptorSink, PeerEndpointCatalog,
    RegionAdmission,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("NOKV_RUST_RAFTSTORE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:23880".to_owned())
        .parse::<SocketAddr>()?;
    let identity = ServerIdentity::from_env()?;
    let peer_endpoints = peer_endpoint_catalog_from_env()?;
    let mut temp_log_dir = None;
    if let Ok(path) = std::env::var("NOKV_RUST_RAFTSTORE_HOLT_DIR") {
        tracing::info!(%addr, %path, "starting rust raftstore server with Holt MVCC");
        let log_dir = raft_log_dir(Some(PathBuf::from(&path)), &mut temp_log_dir)?;
        let mvcc = HoltMvccStore::open_file(path)?;
        let descriptor =
            mvcc.load_or_bootstrap_region_descriptor(&default_region_descriptor(identity))?;
        let admission = RegionAdmission::from_descriptor(&descriptor, identity.bootstrap)?;
        let apply_status = mvcc
            .get_region_apply_state(descriptor.region_id)?
            .map(apply_status_from_holt)
            .unwrap_or(nokv_raftnode::ApplyStatus {
                region_id: descriptor.region_id,
                term: 1,
                applied_index: 0,
            });
        let engine = AppliedKvEngine::with_status(apply_status, mvcc.clone());
        let engine = PersistentAppliedKvEngine::new(engine, HoltApplyStatusSink::new(mvcc.clone()));
        let region = open_openraft_region(identity, addr, log_dir, engine).await?;
        nokv_raftstore_server::serve_with_openraft_region_admission_peer_endpoints_and_descriptor_sink(
            addr,
            region,
            admission,
            peer_endpoints,
            HoltRegionDescriptorSink::new(mvcc),
        )
        .await?;
    } else {
        tracing::info!(%addr, "starting rust raftstore server with in-memory MVCC");
        let log_dir = raft_log_dir(None, &mut temp_log_dir)?;
        let engine = AppliedKvEngine::new(identity.region_id, MvccStore::new());
        let region = open_openraft_region(identity, addr, log_dir, engine).await?;
        nokv_raftstore_server::serve_with_openraft_region_admission_and_peer_endpoints(
            addr,
            region,
            RegionAdmission::from_descriptor(
                &default_region_descriptor(identity),
                identity.bootstrap,
            )?,
            peer_endpoints,
        )
        .await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ServerIdentity {
    region_id: u64,
    store_id: u64,
    peer_id: u64,
    bootstrap: bool,
}

impl Default for ServerIdentity {
    fn default() -> Self {
        Self {
            region_id: 1,
            store_id: 1,
            peer_id: 1,
            bootstrap: true,
        }
    }
}

impl ServerIdentity {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Self::from_values(
            std::env::var("NOKV_RUST_RAFTSTORE_REGION_ID").ok(),
            std::env::var("NOKV_RUST_RAFTSTORE_STORE_ID").ok(),
            std::env::var("NOKV_RUST_RAFTSTORE_PEER_ID").ok(),
            std::env::var("NOKV_RUST_RAFTSTORE_BOOTSTRAP").ok(),
        )
    }

    fn from_values(
        region_id: Option<String>,
        store_id: Option<String>,
        peer_id: Option<String>,
        bootstrap: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let default = Self::default();
        Ok(Self {
            region_id: parse_required_nonzero_u64(
                "NOKV_RUST_RAFTSTORE_REGION_ID",
                region_id,
                default.region_id,
            )?,
            store_id: parse_required_nonzero_u64(
                "NOKV_RUST_RAFTSTORE_STORE_ID",
                store_id,
                default.store_id,
            )?,
            peer_id: parse_required_nonzero_u64(
                "NOKV_RUST_RAFTSTORE_PEER_ID",
                peer_id,
                default.peer_id,
            )?,
            bootstrap: parse_bootstrap_flag(bootstrap, default.bootstrap)?,
        })
    }
}

fn parse_required_nonzero_u64(
    name: &str,
    value: Option<String>,
    default: u64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let Some(value) = value else {
        return Ok(default);
    };
    let parsed = value.parse::<u64>()?;
    if parsed == 0 {
        return Err(format!("{name} must be non-zero").into());
    }
    Ok(parsed)
}

fn parse_bootstrap_flag(
    value: Option<String>,
    default: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err("NOKV_RUST_RAFTSTORE_BOOTSTRAP must be true or false".into()),
    }
}

fn peer_endpoint_catalog_from_env() -> Result<PeerEndpointCatalog, Box<dyn std::error::Error>> {
    let catalog = PeerEndpointCatalog::require_configured();
    let Ok(raw) = std::env::var("NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS") else {
        return Ok(catalog);
    };
    for item in raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (peer_id, endpoint) = item.split_once('=').ok_or_else(|| {
            format!("invalid NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS entry {item:?}: expected peer_id=endpoint")
        })?;
        catalog.insert_peer(peer_id.parse()?, endpoint.to_owned())?;
    }
    Ok(catalog)
}

async fn open_openraft_region<E>(
    identity: ServerIdentity,
    addr: SocketAddr,
    log_dir: PathBuf,
    engine: E,
) -> Result<OpenRaftRegion<E>, Box<dyn std::error::Error>>
where
    E: RegionSnapshotEngine,
{
    let log = SegmentedEntryLog::open(identity.region_id, log_dir)?;
    let state_machine = RegionStateMachine::new(engine);
    if identity.bootstrap {
        return Ok(OpenRaftRegion::bootstrap_single_node_with_network(
            identity.peer_id,
            identity.region_id,
            RegionLogStorage::new(log),
            state_machine,
            TonicRaftNetworkFactory::new(identity.region_id),
            addr.to_string(),
        )
        .await?);
    }
    Ok(OpenRaftRegion::open_with_network(
        identity.peer_id,
        identity.region_id,
        RegionLogStorage::new(log),
        state_machine,
        TonicRaftNetworkFactory::new(identity.region_id),
    )
    .await?)
}

fn raft_log_dir(
    persistent_root: Option<PathBuf>,
    temp_log_dir: &mut Option<tempfile::TempDir>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(path) = std::env::var("NOKV_RUST_RAFTSTORE_LOG_DIR") {
        return Ok(PathBuf::from(path));
    }
    if let Some(root) = persistent_root {
        return Ok(root.join("raftlog"));
    }
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_path_buf();
    *temp_log_dir = Some(dir);
    Ok(path)
}

fn default_region_descriptor(identity: ServerIdentity) -> metapb::RegionDescriptor {
    metapb::RegionDescriptor {
        region_id: identity.region_id,
        epoch: Some(metapb::RegionEpoch {
            version: 1,
            conf_version: 1,
        }),
        peers: vec![metapb::RegionPeer {
            store_id: identity.store_id,
            peer_id: identity.peer_id,
        }],
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_identity_defaults_to_single_node_bootstrap() {
        assert_eq!(
            ServerIdentity::from_values(None, None, None, None).unwrap(),
            ServerIdentity::default()
        );
    }

    #[test]
    fn server_identity_parses_joining_peer() {
        let identity = ServerIdentity::from_values(
            Some("9".to_owned()),
            Some("12".to_owned()),
            Some("34".to_owned()),
            Some("false".to_owned()),
        )
        .unwrap();
        assert_eq!(
            identity,
            ServerIdentity {
                region_id: 9,
                store_id: 12,
                peer_id: 34,
                bootstrap: false,
            }
        );
    }

    #[test]
    fn server_identity_rejects_zero_peer() {
        let err = ServerIdentity::from_values(None, None, Some("0".to_owned()), None).unwrap_err();
        assert!(err.to_string().contains("NOKV_RUST_RAFTSTORE_PEER_ID"));
    }

    #[test]
    fn server_identity_rejects_invalid_bootstrap_flag() {
        let err =
            ServerIdentity::from_values(None, None, None, Some("maybe".to_owned())).unwrap_err();
        assert!(err.to_string().contains("NOKV_RUST_RAFTSTORE_BOOTSTRAP"));
    }

    #[tokio::test]
    async fn non_bootstrap_start_opens_joining_peer_without_initializing_membership() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ServerIdentity {
            region_id: 7,
            store_id: 2,
            peer_id: 2,
            bootstrap: false,
        };
        let region = open_openraft_region(
            identity,
            "127.0.0.1:0".parse().unwrap(),
            dir.path().to_path_buf(),
            AppliedKvEngine::new(identity.region_id, MvccStore::new()),
        )
        .await
        .unwrap();
        let metrics = region.raft_handle().metrics().borrow().clone();
        assert!(metrics.current_leader.is_none());
        assert!(metrics.membership_config.voter_ids().next().is_none());
    }
}
