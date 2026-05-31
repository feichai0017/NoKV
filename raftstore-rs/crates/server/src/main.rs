//! Standalone Rust raftstore server entrypoint for local compatibility tests.

mod bootstrap;
mod coordinator;
mod metrics;
mod startup;

use std::net::SocketAddr;
use std::path::PathBuf;

#[cfg(test)]
pub(crate) use bootstrap::*;
use bootstrap::{serve_holt_regions, serve_memory_regions};
use coordinator::coordinator_heartbeat_config_from_env;
#[cfg(test)]
pub(crate) use coordinator::*;
#[cfg(test)]
pub(crate) use startup::*;
use startup::{
    advertised_addr_from_env, peer_endpoint_catalog_from_env, validate_startup_region_ranges,
    RegionRangeCatalog, ServerArgs, ServerIdentity,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = ServerArgs::parse(std::env::args().skip(1))?;
    let addr = std::env::var("NOKV_RUST_RAFTSTORE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:23880".to_owned())
        .parse::<SocketAddr>()?;
    let advertised_addr = advertised_addr_from_env(addr)?;
    let identities = ServerIdentity::from_env_list()?;
    let region_ranges = RegionRangeCatalog::from_env()?;
    validate_startup_region_ranges(&identities, &region_ranges)?;
    let coordinator = coordinator_heartbeat_config_from_env()?;
    let peer_endpoints = peer_endpoint_catalog_from_env()?;
    let mut temp_log_dir = None;
    if let Ok(path) = std::env::var("NOKV_RUST_RAFTSTORE_HOLT_DIR") {
        serve_holt_regions(
            addr,
            advertised_addr,
            identities,
            coordinator,
            peer_endpoints,
            region_ranges,
            PathBuf::from(path),
            args.metrics_addr,
            &mut temp_log_dir,
        )
        .await?;
    } else {
        serve_memory_regions(
            addr,
            advertised_addr,
            identities,
            coordinator,
            peer_endpoints,
            region_ranges,
            args.metrics_addr,
            &mut temp_log_dir,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
