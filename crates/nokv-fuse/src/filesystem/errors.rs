use fuser::Errno;
use nokv_meta::MetadError;

use crate::backend::FuseBackendError;

pub(super) fn errno(err: impl Into<FuseBackendError>) -> Errno {
    classify(&err.into())
}

fn classify(err: &FuseBackendError) -> Errno {
    match err {
        FuseBackendError::Metadata(e) => metadata_errno(e),
        FuseBackendError::Client(nokv_client::ClientError::Metadata(e)) => metadata_errno(e),
        FuseBackendError::Client(nokv_client::ClientError::NotFound(_)) => Errno::ENOENT,
        FuseBackendError::Client(nokv_client::ClientError::LockConflict(_)) => Errno::EAGAIN,
        // `EIO` is the lossy bucket: object-upload failures and raw IO collapse
        // into it and otherwise reach userspace as an opaque `Input/output
        // error`. Surface the real cause on the mount's stderr (captured in the
        // mount log) so write-path failures are diagnosable instead of silent.
        FuseBackendError::Client(nokv_client::ClientError::Object(_))
        | FuseBackendError::Client(nokv_client::ClientError::Io(_))
        | FuseBackendError::Client(nokv_client::ClientError::Protocol(_))
        | FuseBackendError::Client(nokv_client::ClientError::EmptyPath)
        | FuseBackendError::Client(nokv_client::ClientError::RelativePath)
        | FuseBackendError::Client(nokv_client::ClientError::ParentTraversal)
        | FuseBackendError::Client(nokv_client::ClientError::InvalidArtifactPath(_))
        | FuseBackendError::Client(nokv_client::ClientError::ArtifactIsDirectory(_))
        | FuseBackendError::Client(nokv_client::ClientError::ArtifactIsFile(_))
        | FuseBackendError::Client(nokv_client::ClientError::InvalidName(_))
        | FuseBackendError::Client(nokv_client::ClientError::RootHasNoParent)
        | FuseBackendError::Object(_) => {
            eprintln!("nokv-fuse: operation failed -> EIO: {err:?}");
            Errno::EIO
        }
    }
}

fn metadata_errno(err: &MetadError) -> Errno {
    match err {
        MetadError::Model(_) => Errno::EINVAL,
        MetadError::InvalidPath(_) => Errno::EINVAL,
        MetadError::InvalidQuery(_) => Errno::EINVAL,
        MetadError::NotFound => Errno::ENOENT,
        MetadError::NotFile => Errno::EISDIR,
        MetadError::NotDirectory => Errno::ENOTDIR,
        MetadError::DirectoryNotEmpty => Errno::ENOTEMPTY,
        MetadError::CannotRemoveRoot => Errno::EBUSY,
        MetadError::StaleBodyGeneration { .. } => Errno::ESTALE,
        MetadError::LockConflict(_) => Errno::EAGAIN,
        // See the note above: surface the underlying metadata/object/allocator
        // failure before it collapses into an opaque `EIO`.
        MetadError::MissingBodyDescriptor
        | MetadError::Metadata(_)
        | MetadError::Object(_)
        | MetadError::PublishArtifactFailed { .. }
        | MetadError::Codec(_)
        | MetadError::BodySizeMismatch { .. }
        | MetadError::InvalidPreparedArtifact(_)
        | MetadError::AllocatorExhausted => {
            eprintln!("nokv-fuse: metadata operation failed -> EIO: {err:?}");
            Errno::EIO
        }
    }
}
