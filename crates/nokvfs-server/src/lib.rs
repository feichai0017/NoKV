//! Long-running NoKV metadata service process.
//!
//! This crate owns process wiring and a small health/control endpoint around
//! the in-process metadata service. It does not own namespace semantics,
//! metadata layout, object provider internals, FUSE policy, or wire protocol
//! compatibility.

mod http;
mod metadata;
mod options;
mod rpc;
mod server;

pub use options::{ServerOptions, DEFAULT_SERVER_BIND};
pub use server::{run, Server, ServerError};
