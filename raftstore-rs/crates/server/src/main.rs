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
use nokv_raftstore_server::{apply_status_from_holt, HoltApplyStatusSink, RegionAdmission};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("NOKV_RUST_RAFTSTORE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:23880".to_owned())
        .parse::<SocketAddr>()?;
    let mut temp_log_dir = None;
    if let Ok(path) = std::env::var("NOKV_RUST_RAFTSTORE_HOLT_DIR") {
        tracing::info!(%addr, %path, "starting rust raftstore server with Holt MVCC");
        let log_dir = raft_log_dir(Some(PathBuf::from(&path)), &mut temp_log_dir)?;
        let mvcc = HoltMvccStore::open_file(path)?;
        let descriptor = mvcc.load_or_bootstrap_region_descriptor(&default_region_descriptor())?;
        let admission = RegionAdmission::from_descriptor(&descriptor, true)?;
        let apply_status = mvcc
            .get_region_apply_state(descriptor.region_id)?
            .map(apply_status_from_holt)
            .unwrap_or(nokv_raftnode::ApplyStatus {
                region_id: descriptor.region_id,
                term: 1,
                applied_index: 0,
            });
        let engine = AppliedKvEngine::with_status(apply_status, mvcc.clone());
        let engine = PersistentAppliedKvEngine::new(engine, HoltApplyStatusSink::new(mvcc));
        let region = bootstrap_openraft_region(
            admission.peer_id,
            admission.region_id,
            addr,
            log_dir,
            engine,
        )
        .await?;
        nokv_raftstore_server::serve_with_openraft_region_and_admission(addr, region, admission)
            .await?;
    } else {
        tracing::info!(%addr, "starting rust raftstore server with in-memory MVCC");
        let log_dir = raft_log_dir(None, &mut temp_log_dir)?;
        let engine = AppliedKvEngine::new(1, MvccStore::new());
        let region = bootstrap_openraft_region(1, 1, addr, log_dir, engine).await?;
        nokv_raftstore_server::serve_with_openraft_region_and_admission(
            addr,
            region,
            RegionAdmission::default(),
        )
        .await?;
    }
    Ok(())
}

async fn bootstrap_openraft_region<E>(
    node_id: u64,
    region_id: u64,
    addr: SocketAddr,
    log_dir: PathBuf,
    engine: E,
) -> Result<OpenRaftRegion<E>, Box<dyn std::error::Error>>
where
    E: RegionSnapshotEngine,
{
    let log = SegmentedEntryLog::open(region_id, log_dir)?;
    let state_machine = RegionStateMachine::new(engine);
    Ok(OpenRaftRegion::bootstrap_single_node_with_network(
        node_id,
        region_id,
        RegionLogStorage::new(log),
        state_machine,
        TonicRaftNetworkFactory::new(region_id),
        addr.to_string(),
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

fn default_region_descriptor() -> metapb::RegionDescriptor {
    metapb::RegionDescriptor {
        region_id: 1,
        epoch: Some(metapb::RegionEpoch {
            version: 1,
            conf_version: 1,
        }),
        peers: vec![metapb::RegionPeer {
            store_id: 1,
            peer_id: 1,
        }],
        ..Default::default()
    }
}
