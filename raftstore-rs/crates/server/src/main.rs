//! Standalone Rust raftstore server entrypoint for local compatibility tests.

use std::net::SocketAddr;

use nokv_holtstore::{HoltMvccStore, RegionApplyState};
use nokv_mvcc::MvccStore;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::{AppliedKvEngine, ApplyStatus, ApplyStatusSink, PersistentAppliedKvEngine};
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
        let apply_status = mvcc
            .get_region_apply_state(descriptor.region_id)?
            .map(apply_status_from_holt)
            .unwrap_or(ApplyStatus {
                region_id: descriptor.region_id,
                term: 1,
                applied_index: 0,
            });
        let engine = AppliedKvEngine::with_status(apply_status, mvcc.clone());
        let engine = PersistentAppliedKvEngine::new(engine, HoltApplyStatusSink { store: mvcc });
        nokv_raftstore_server::serve_with_region_engine_and_admission(addr, engine, admission)
            .await?;
    } else {
        tracing::info!(%addr, "starting rust raftstore compatibility server with in-memory MVCC");
        let engine = AppliedKvEngine::new(1, MvccStore::new());
        nokv_raftstore_server::serve_with_region_engine(addr, engine).await?;
    }
    Ok(())
}

#[derive(Clone)]
struct HoltApplyStatusSink {
    store: HoltMvccStore,
}

impl ApplyStatusSink for HoltApplyStatusSink {
    fn save_apply_status(&self, status: &ApplyStatus) -> nokv_mvcc::Result<()> {
        self.store
            .put_region_apply_state(&RegionApplyState {
                region_id: status.region_id,
                term: status.term,
                applied_index: status.applied_index,
                truncated_term: 0,
                truncated_index: 0,
            })
            .and_then(|_| self.store.checkpoint())
            .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))
    }
}

fn apply_status_from_holt(state: RegionApplyState) -> ApplyStatus {
    ApplyStatus {
        region_id: state.region_id,
        term: state.term,
        applied_index: state.applied_index,
    }
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
