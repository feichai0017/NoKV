//! Path-oriented Rust client for NoKV-FS.
//!
//! This crate owns SDK ergonomics over the in-process `metad` service. It does
//! not own metadata layout, Holt trees, object-store internals, FUSE, or remote
//! wire protocols.

use std::fmt;

use nokvfs_meta::command::MetadataStore;
use nokvfs_meta::{
    DentryWithAttr, MetadError, NoKvFs, ObjectTransferStats, PublishArtifact, RenameReplaceResult,
};
use nokvfs_object::ObjectStore;
use nokvfs_types::{DentryName, FileType, InodeId};

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
}

pub struct NoKvFsClient<M, O> {
    service: NoKvFs<M, O>,
}

impl<M, O> NoKvFsClient<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn new(service: NoKvFs<M, O>) -> Self {
        Self { service }
    }

    pub fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), ClientError> {
        self.service.bootstrap_root(mode, uid, gid)?;
        Ok(())
    }

    pub fn mkdir(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        self.service
            .create_dir(parent, name, mode, uid, gid)
            .map_err(Into::into)
    }

    pub fn create_file(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        self.service
            .create_file(parent, name, mode, uid, gid)
            .map_err(Into::into)
    }

    pub fn put_artifact(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        self.service
            .publish_artifact(PublishArtifact {
                parent,
                name,
                producer: metadata.producer,
                digest_uri: metadata.digest_uri,
                content_type: metadata.content_type,
                manifest_id: metadata.manifest_id,
                bytes,
                mode: metadata.mode,
                uid: metadata.uid,
                gid: metadata.gid,
            })
            .map_err(Into::into)
    }

    pub fn lookup(&self, path: &str) -> Result<Option<DentryWithAttr>, ClientError> {
        if is_root_path(path)? {
            return Ok(None);
        }
        let (parent, name) = self.resolve_parent(path)?;
        self.service.lookup_plus(parent, &name).map_err(Into::into)
    }

    pub fn list(&self, path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        let inode = self.resolve_directory(path)?;
        self.service.read_dir_plus(inode).map_err(Into::into)
    }

    pub fn remove(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        self.service.remove_file(parent, &name).map_err(Into::into)
    }

    pub fn rmdir(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        self.service
            .remove_empty_dir(parent, &name)
            .map_err(Into::into)
    }

    pub fn rename(&self, source: &str, destination: &str) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(source)?;
        let (new_parent, new_name) = self.resolve_parent(destination)?;
        self.service
            .rename(parent, &name, new_parent, new_name)
            .map_err(Into::into)
    }

    pub fn rename_replace(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, ClientError> {
        let (parent, name) = self.resolve_parent(source)?;
        let (new_parent, new_name) = self.resolve_parent(destination)?;
        self.service
            .rename_replace(parent, &name, new_parent, new_name)
            .map_err(Into::into)
    }

    pub fn cat(&self, path: &str) -> Result<Vec<u8>, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        self.service
            .read_artifact(parent, &name)
            .map_err(Into::into)
    }

    pub fn into_inner(self) -> NoKvFs<M, O> {
        self.service
    }

    pub fn object_stats(&self) -> ObjectTransferStats {
        self.service.object_stats()
    }

    fn resolve_parent(&self, path: &str) -> Result<(InodeId, DentryName), ClientError> {
        let mut components = parse_absolute_path(path)?;
        let name = components.pop().ok_or(ClientError::RootHasNoParent)?;
        let parent = self.resolve_components_as_directory(&components)?;
        Ok((parent, name))
    }

    fn resolve_directory(&self, path: &str) -> Result<InodeId, ClientError> {
        let components = parse_absolute_path(path)?;
        self.resolve_components_as_directory(&components)
    }

    fn resolve_components_as_directory(
        &self,
        components: &[DentryName],
    ) -> Result<InodeId, ClientError> {
        let mut current = InodeId::root();
        for name in components {
            let label = display_name(name);
            let entry = self
                .service
                .lookup_plus(current, name)?
                .ok_or_else(|| ClientError::NotFound(label.clone()))?;
            if entry.attr.file_type != FileType::Directory {
                return Err(ClientError::NotDirectory(label));
            }
            current = entry.attr.inode;
        }
        Ok(current)
    }
}

fn parse_absolute_path(path: &str) -> Result<Vec<DentryName>, ClientError> {
    if path.is_empty() {
        return Err(ClientError::EmptyPath);
    }
    if !path.starts_with('/') {
        return Err(ClientError::RelativePath);
    }
    let mut out = Vec::new();
    for raw in path.split('/').filter(|part| !part.is_empty()) {
        if raw == "." {
            continue;
        }
        if raw == ".." {
            return Err(ClientError::ParentTraversal);
        }
        out.push(
            DentryName::new(raw.as_bytes().to_vec())
                .map_err(|err| ClientError::InvalidName(err.to_string()))?,
        );
    }
    Ok(out)
}

fn is_root_path(path: &str) -> Result<bool, ClientError> {
    Ok(parse_absolute_path(path)?.is_empty())
}

fn display_name(name: &DentryName) -> String {
    String::from_utf8_lossy(name.as_bytes()).into_owned()
}

impl From<MetadError> for ClientError {
    fn from(err: MetadError) -> Self {
        Self::Metadata(err)
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
        }
    }
}

impl std::error::Error for ClientError {}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_meta::holtstore::HoltMetadataStore;
    use nokvfs_meta::NoKvFs;
    use nokvfs_object::MemoryObjectStore;
    use nokvfs_types::MountId;

    fn client() -> NoKvFsClient<HoltMetadataStore, MemoryObjectStore> {
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            MemoryObjectStore::new(),
        );
        let client = NoKvFsClient::new(service);
        client.bootstrap_root(0o755, 1000, 1000).unwrap();
        client
    }

    #[test]
    fn mkdir_put_list_and_cat_by_path() {
        let client = client();
        client.mkdir("/runs", 0o755, 1000, 1000).unwrap();
        let empty = client
            .create_file("/runs/empty", 0o644, 1000, 1000)
            .unwrap();
        assert_eq!(empty.attr.size, 0);
        assert_eq!(empty.body, None);
        client.mkdir("/runs/1", 0o755, 1000, 1000).unwrap();
        let published = client
            .put_artifact(
                "/runs/1/checkpoint.json",
                b"{\"step\":1}".to_vec(),
                ArtifactMetadata {
                    producer: "unit-test".to_owned(),
                    digest_uri: "sha256:demo".to_owned(),
                    content_type: "application/json".to_owned(),
                    manifest_id: "runs/1/checkpoint.json".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            )
            .unwrap();

        assert_eq!(
            client.lookup("/runs/1/checkpoint.json").unwrap(),
            Some(published.clone())
        );
        assert_eq!(client.list("/runs/1").unwrap(), vec![published]);
        assert_eq!(
            client.cat("/runs/1/checkpoint.json").unwrap(),
            b"{\"step\":1}"
        );
    }

    #[test]
    fn path_resolution_rejects_relative_and_parent_paths() {
        let client = client();
        assert!(matches!(
            client.list("runs"),
            Err(ClientError::RelativePath)
        ));
        assert!(matches!(
            client.list("/../runs"),
            Err(ClientError::ParentTraversal)
        ));
    }

    #[test]
    fn missing_parent_is_reported_before_publish() {
        let client = client();
        let err = client
            .put_artifact(
                "/missing/file",
                b"x".to_vec(),
                ArtifactMetadata {
                    producer: "unit-test".to_owned(),
                    digest_uri: "sha256:x".to_owned(),
                    content_type: "text/plain".to_owned(),
                    manifest_id: "missing/file".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            )
            .unwrap_err();
        assert!(matches!(err, ClientError::NotFound(name) if name == "missing"));
    }

    #[test]
    fn remove_rmdir_and_rename_by_path() {
        let client = client();
        client.mkdir("/runs", 0o755, 1000, 1000).unwrap();
        client.mkdir("/runs/old", 0o755, 1000, 1000).unwrap();
        let renamed = client.rename("/runs/old", "/runs/new").unwrap();
        assert_eq!(renamed.dentry.name.as_bytes(), b"new");
        assert!(client.lookup("/runs/old").unwrap().is_none());
        assert!(client.lookup("/runs/new").unwrap().is_some());

        let removed_dir = client.rmdir("/runs/new").unwrap();
        assert_eq!(removed_dir.dentry.name.as_bytes(), b"new");

        client
            .put_artifact(
                "/runs/file",
                b"x".to_vec(),
                ArtifactMetadata {
                    producer: "unit-test".to_owned(),
                    digest_uri: "sha256:x".to_owned(),
                    content_type: "text/plain".to_owned(),
                    manifest_id: "runs/file".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            )
            .unwrap();
        let removed_file = client.remove("/runs/file").unwrap();
        assert_eq!(removed_file.body.as_ref().unwrap().manifest_id, "runs/file");
        assert!(client.lookup("/runs/file").unwrap().is_none());
    }

    #[test]
    fn rename_replace_by_path_returns_replaced_body() {
        let client = client();
        client.mkdir("/runs", 0o755, 1000, 1000).unwrap();
        for (path, manifest_id, body) in [
            ("/runs/stage", "runs/stage", b"new".to_vec()),
            ("/runs/final", "runs/final-old", b"old".to_vec()),
        ] {
            client
                .put_artifact(
                    path,
                    body,
                    ArtifactMetadata {
                        producer: "unit-test".to_owned(),
                        digest_uri: "sha256:test".to_owned(),
                        content_type: "application/octet-stream".to_owned(),
                        manifest_id: manifest_id.to_owned(),
                        mode: 0o644,
                        uid: 1000,
                        gid: 1000,
                    },
                )
                .unwrap();
        }

        let result = client.rename_replace("/runs/stage", "/runs/final").unwrap();
        assert_eq!(
            result.replaced.unwrap().body.unwrap().manifest_id,
            "runs/final-old"
        );
        assert!(client.lookup("/runs/stage").unwrap().is_none());
        assert_eq!(client.cat("/runs/final").unwrap(), b"new");
    }
}
