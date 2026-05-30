//! Standalone Rust raftstore server entrypoint for local compatibility tests.

use std::net::SocketAddr;

use nokv_holtstore::HoltMvccStore;
use nokv_mvcc::MvccStore;
use nokv_raftnode::AppliedKvEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("NOKV_RUST_RAFTSTORE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:23880".to_owned())
        .parse::<SocketAddr>()?;
    if let Ok(path) = std::env::var("NOKV_RUST_RAFTSTORE_HOLT_DIR") {
        tracing::info!(%addr, %path, "starting rust raftstore server with Holt MVCC");
        let engine = AppliedKvEngine::new(1, HoltMvccStore::open_file(path)?);
        nokv_raftstore_server::serve_with_engine(addr, engine).await?;
    } else {
        tracing::info!(%addr, "starting rust raftstore compatibility server with in-memory MVCC");
        let engine = AppliedKvEngine::new(1, MvccStore::new());
        nokv_raftstore_server::serve_with_engine(addr, engine).await?;
    }
    Ok(())
}
