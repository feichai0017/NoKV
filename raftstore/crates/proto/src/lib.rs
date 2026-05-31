//! Generated Rust bindings for the existing NoKV protobuf wire contract.
//!
//! This crate is the only Rust crate that includes the repository protobufs.
//! Rust raftstore code consumes these generated types to stay wire-compatible
//! with the Go fsmeta, coordinator, and raftstore clients.

pub mod nokv {
    pub mod error {
        pub mod v1 {
            tonic::include_proto!("nokv.error.v1");
        }
    }

    pub mod meta {
        pub mod v1 {
            tonic::include_proto!("nokv.meta.v1");
        }
    }

    pub mod coordinator {
        pub mod v1 {
            tonic::include_proto!("nokv.coordinator.v1");
        }
    }

    pub mod metadata {
        pub mod v1 {
            tonic::include_proto!("nokv.metadata.v1");
        }
    }

    pub mod admin {
        pub mod v1 {
            tonic::include_proto!("nokv.admin.v1");
        }
    }

    pub mod fsmeta {
        pub mod v1 {
            tonic::include_proto!("nokv.fsmeta.v1");
        }
    }
}
