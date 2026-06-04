//! Path-oriented Rust client for NoKV.
//!
//! This crate owns SDK ergonomics over the remote `metad` service. It does not
//! own metadata layout, Holt trees, object-store internals, FUSE, or metadata
//! wire-format definitions.

use std::fmt;

mod remote;

use nokvfs_meta::MetadError;
use nokvfs_object::ObjectError;
use nokvfs_types::PathError;

pub use remote::{
    RemoteBodyReadPlan, RemoteMetadataClient, RemoteMetadataClientOptions, RemoteNoKvFsClient,
    RemotePreparedArtifact,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactMetadata {
    pub producer: String,
    pub digest_uri: String,
    pub content_type: String,
    pub manifest_id: String,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug)]
pub enum ClientError {
    EmptyPath,
    RelativePath,
    ParentTraversal,
    InvalidName(String),
    RootHasNoParent,
    NotFound(String),
    NotDirectory(String),
    Metadata(MetadError),
    Object(ObjectError),
    Remote(String),
    Io(String),
    Protocol(String),
}

impl From<MetadError> for ClientError {
    fn from(err: MetadError) -> Self {
        Self::Metadata(err)
    }
}

impl From<PathError> for ClientError {
    fn from(err: PathError) -> Self {
        match err {
            PathError::Empty => Self::EmptyPath,
            PathError::Relative => Self::RelativePath,
            PathError::ParentTraversal => Self::ParentTraversal,
            PathError::InvalidName(err) => Self::InvalidName(err.to_string()),
        }
    }
}

impl From<ObjectError> for ClientError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPath => write!(f, "path is empty"),
            Self::RelativePath => write!(f, "path must be absolute"),
            Self::ParentTraversal => write!(f, "path must not contain '..'"),
            Self::InvalidName(err) => write!(f, "invalid path component: {err}"),
            Self::RootHasNoParent => write!(f, "root path has no parent"),
            Self::NotFound(path) => write!(f, "path component not found: {path}"),
            Self::NotDirectory(path) => write!(f, "path component is not a directory: {path}"),
            Self::Metadata(err) => write!(f, "metadata service error: {err}"),
            Self::Object(err) => write!(f, "object store error: {err}"),
            Self::Remote(err) => write!(f, "remote metadata error: {err}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Protocol(err) => write!(f, "metadata protocol error: {err}"),
        }
    }
}

impl std::error::Error for ClientError {}
