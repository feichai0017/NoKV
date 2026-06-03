//! Long-running NoKV-FS metadata service process.
//!
//! This crate owns process wiring and a small health/control endpoint around
//! the in-process metadata service. It does not own namespace semantics,
//! metadata layout, object provider internals, FUSE policy, or remote protocol
//! compatibility.

mod http;
mod options;
mod server;

pub use options::{ServerOptions, DEFAULT_SERVER_BIND};
pub use server::{run, Server, ServerError};
