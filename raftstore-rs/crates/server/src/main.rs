//! Standalone Rust raftstore server entrypoint for local compatibility tests.

use std::net::SocketAddr;

use nokv_holtstore::HoltMvccStore;
use nokv_mvcc::MvccStore;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::AppliedKvEngine;
use nokv_raftstore_server::RegionAdmission;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("NOKV_RUST_RAFTSTORE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:23880".to_owned())
        .parse::<SocketAddr>()?;
    if let Ok(path) = std::env::var("NOKV_RUST_RAFTSTORE_HOLT_DIR") {
        tracing::info!(%addr, %path, "starting rust raftstore server with Holt MVCC");
        let mvcc = HoltMvccStore::open_file(path)?;
        let descriptor = mvcc.load_or_bootstrap_region_descriptor(&default_region_descriptor())?;
        let admission = RegionAdmission::from_descriptor(&descriptor, true)?;
        let engine = AppliedKvEngine::new(descriptor.region_id, mvcc);
        nokv_raftstore_server::serve_with_region_engine_and_admission(addr, engine, admission)
            .await?;
    } else {
        tracing::info!(%addr, "starting rust raftstore compatibility server with in-memory MVCC");
        let engine = AppliedKvEngine::new(1, MvccStore::new());
        nokv_raftstore_server::serve_with_region_engine(addr, engine).await?;
    }
    Ok(())
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
