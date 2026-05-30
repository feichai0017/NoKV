//! Standalone Rust raftstore server entrypoint for local compatibility tests.

use std::net::SocketAddr;

use nokv_holtstore::HoltMvccStore;
use nokv_mvcc::MvccStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("NOKV_RUST_RAFTSTORE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:23880".to_owned())
        .parse::<SocketAddr>()?;
    if let Ok(path) = std::env::var("NOKV_RUST_RAFTSTORE_HOLT_DIR") {
        tracing::info!(%addr, %path, "starting rust raftstore server with Holt MVCC");
        nokv_raftstore_server::serve_with_engine(addr, HoltMvccStore::open_file(path)?).await?;
    } else {
        tracing::info!(%addr, "starting rust raftstore compatibility server with in-memory MVCC");
        nokv_raftstore_server::serve(addr, MvccStore::new()).await?;
    }
    Ok(())
}
