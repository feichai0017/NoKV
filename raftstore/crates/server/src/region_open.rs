use std::path::{Path, PathBuf};
use std::time::Duration;

use nokv_raftnode::{
    OpenRaftRegion, RegionLogStorage, RegionSnapshotEngine, RegionStateMachine, SegmentedEntryLog,
    TonicRaftNetworkFactory,
};

use crate::startup::ServerIdentity;

pub(crate) async fn open_openraft_region<E>(
    identity: ServerIdentity,
    addr: &str,
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
            addr.to_owned(),
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

pub(crate) fn raft_log_dir_for_region(
    persistent_root: Option<&Path>,
    identity: ServerIdentity,
    multi_region: bool,
    temp_log_dir: &mut Option<tempfile::TempDir>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(path) = std::env::var("NOKV_RAFTSTORE_LOG_DIR") {
        let root = PathBuf::from(path);
        return Ok(region_log_dir(root, identity.region_id, multi_region));
    }
    if let Some(root) = persistent_root {
        return Ok(region_log_dir(
            root.join("raftlog"),
            identity.region_id,
            multi_region,
        ));
    }
    if temp_log_dir.is_none() {
        *temp_log_dir = Some(tempfile::tempdir()?);
    }
    let root = temp_log_dir
        .as_ref()
        .expect("temp log dir is initialized")
        .path()
        .to_path_buf();
    Ok(region_log_dir(root, identity.region_id, multi_region))
}

pub(crate) fn region_log_dir(root: PathBuf, region_id: u64, multi_region: bool) -> PathBuf {
    if multi_region {
        root.join(format!("region-{region_id}"))
    } else {
        root
    }
}

pub(crate) fn spawn_recovered_region_leadership_retries<E>(
    regions: Vec<(ServerIdentity, OpenRaftRegion<E>)>,
) where
    E: RegionSnapshotEngine + Send + Sync + 'static,
{
    for (identity, region) in regions {
        tokio::spawn(async move {
            for attempt in 1..=50 {
                let voter_count = region
                    .raft_handle()
                    .metrics()
                    .borrow()
                    .membership_config
                    .voter_ids()
                    .count();
                let election = if voter_count <= 1 {
                    region.elect_and_wait(identity.peer_id).await
                } else {
                    region.wait_for_leader(identity.peer_id).await
                };
                match election {
                    Ok(()) => match region.ensure_linearizable().await {
                        Ok(()) => return,
                        Err(err) => {
                            tracing::debug!(
                                region_id = identity.region_id,
                                peer_id = identity.peer_id,
                                attempt,
                                error = %err,
                                "raftstore recovered region linearizable wait failed"
                            );
                        }
                    },
                    Err(err) => {
                        tracing::debug!(
                            region_id = identity.region_id,
                            peer_id = identity.peer_id,
                            voter_count,
                            attempt,
                            error = %err,
                            "raftstore recovered region leadership wait failed"
                        );
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            tracing::warn!(
                region_id = identity.region_id,
                peer_id = identity.peer_id,
                "raftstore recovered region did not elect a startup leader"
            );
        });
    }
}
