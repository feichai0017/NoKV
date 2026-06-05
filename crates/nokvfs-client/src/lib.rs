//! Path-oriented Rust client for NoKV.
//!
//! This crate owns SDK ergonomics over the metadata service. It does not
//! own metadata layout, Holt trees, object-store internals, FUSE, or metadata
//! wire-format definitions.

use std::fmt;

mod artifact;
mod service;

use nokvfs_meta::{MetadError, MetadataError};
use nokvfs_object::ObjectError;
use nokvfs_types::{PathError, PathMetadata};

pub use artifact::{
    normalize_artifact_path, ArtifactBackend, ArtifactInfo, ArtifactRepository,
    ArtifactRepositoryOptions,
};
pub use service::{
    ClientBodyReadPlan, ClientMetadataPosition, ClientPreparedArtifact, MetadataClient,
    MetadataClientOptions, NoKvFsClient,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceRead {
    pub metadata: PathMetadata,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub enum ClientError {
    EmptyPath,
    RelativePath,
    ParentTraversal,
    InvalidArtifactPath(String),
    ArtifactIsDirectory(String),
    ArtifactIsFile(String),
    InvalidName(String),
    RootHasNoParent,
    NotFound(String),
    NotDirectory(String),
    ReadNotFresh {
        required_term: u64,
        required_index: u64,
        applied_term: Option<u64>,
        applied_index: Option<u64>,
    },
    ForwardToLeader {
        leader_id: Option<u64>,
        address: Option<std::net::SocketAddr>,
    },
    Metadata(MetadError),
    Object(ObjectError),
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
            Self::InvalidArtifactPath(err) => write!(f, "invalid artifact path: {err}"),
            Self::ArtifactIsDirectory(path) => write!(f, "artifact is a directory: {path}"),
            Self::ArtifactIsFile(path) => write!(f, "artifact is a file: {path}"),
            Self::InvalidName(err) => write!(f, "invalid path component: {err}"),
            Self::RootHasNoParent => write!(f, "root path has no parent"),
            Self::NotFound(path) => write!(f, "path component not found: {path}"),
            Self::NotDirectory(path) => write!(f, "path component is not a directory: {path}"),
            Self::ReadNotFresh {
                required_term,
                required_index,
                applied_term,
                applied_index,
            } => write!(
                f,
                "metadata read requires applied frontier {required_term}:{required_index}, current applied frontier is {}",
                match (applied_term, applied_index) {
                    (Some(term), Some(index)) => format!("{term}:{index}"),
                    _ => "none".to_owned(),
                }
            ),
            Self::ForwardToLeader { leader_id, address } => {
                write!(f, "metadata write must be forwarded to leader")?;
                if let Some(leader_id) = leader_id {
                    write!(f, " {leader_id}")?;
                }
                if let Some(address) = address {
                    write!(f, " at {address}")?;
                }
                Ok(())
            }
            Self::Metadata(err) => write!(f, "metadata service error: {err}"),
            Self::Object(err) => write!(f, "object store error: {err}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Protocol(err) => write!(f, "metadata protocol error: {err}"),
        }
    }
}

impl std::error::Error for ClientError {}

fn is_metadata_predicate_failed(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::Metadata(MetadError::Metadata(MetadataError::PredicateFailed))
    )
}

fn is_not_found(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::NotFound(_) | ClientError::Metadata(MetadError::NotFound)
    )
}
