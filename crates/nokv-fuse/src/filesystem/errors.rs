use fuser::Errno;
use nokv_meta::MetadError;

use crate::backend::FuseBackendError;

pub(super) fn errno(err: impl Into<FuseBackendError>) -> Errno {
    match err.into() {
        FuseBackendError::Metadata(err) => metadata_errno(err),
        FuseBackendError::Client(nokv_client::ClientError::Metadata(err)) => metadata_errno(err),
        FuseBackendError::Client(nokv_client::ClientError::NotFound(_)) => Errno::ENOENT,
        FuseBackendError::Client(nokv_client::ClientError::ForwardToLeader { .. }) => Errno::EAGAIN,
        FuseBackendError::Client(nokv_client::ClientError::LockConflict(_)) => Errno::EAGAIN,
        FuseBackendError::Client(nokv_client::ClientError::Object(_))
        | FuseBackendError::Client(nokv_client::ClientError::Io(_))
        | FuseBackendError::Client(nokv_client::ClientError::Protocol(_))
        | FuseBackendError::Client(nokv_client::ClientError::ReadNotFresh { .. })
        | FuseBackendError::Client(nokv_client::ClientError::EmptyPath)
        | FuseBackendError::Client(nokv_client::ClientError::RelativePath)
        | FuseBackendError::Client(nokv_client::ClientError::ParentTraversal)
        | FuseBackendError::Client(nokv_client::ClientError::InvalidArtifactPath(_))
        | FuseBackendError::Client(nokv_client::ClientError::ArtifactIsDirectory(_))
        | FuseBackendError::Client(nokv_client::ClientError::ArtifactIsFile(_))
        | FuseBackendError::Client(nokv_client::ClientError::InvalidName(_))
        | FuseBackendError::Client(nokv_client::ClientError::RootHasNoParent)
        | FuseBackendError::Object(_) => Errno::EIO,
    }
}

fn metadata_errno(err: MetadError) -> Errno {
    match err {
        MetadError::Model(_) => Errno::EINVAL,
        MetadError::InvalidPath(_) => Errno::EINVAL,
        MetadError::NotFound => Errno::ENOENT,
        MetadError::NotFile => Errno::EISDIR,
        MetadError::NotDirectory => Errno::ENOTDIR,
        MetadError::DirectoryNotEmpty => Errno::ENOTEMPTY,
        MetadError::CannotRemoveRoot => Errno::EBUSY,
        MetadError::StaleBodyGeneration { .. } => Errno::ESTALE,
        MetadError::LockConflict(_) => Errno::EAGAIN,
        MetadError::MissingBodyDescriptor
        | MetadError::Metadata(_)
        | MetadError::Object(_)
        | MetadError::PublishArtifactFailed { .. }
        | MetadError::Codec(_)
        | MetadError::BodySizeMismatch { .. }
        | MetadError::InvalidPreparedArtifact(_)
        | MetadError::AllocatorExhausted => Errno::EIO,
    }
}
