use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use nokvfs_types::MountId;

use crate::{
    CheckpointArtifact, CheckpointFrontier, CheckpointManifest, LogIndex, LogPosition, LogTerm,
    SharedLogError,
};
use nokvfs_meta::Version;

const CHECKPOINT_MAGIC: &[u8; 8] = b"NKFSCKP2";

pub trait CheckpointCatalog {
    fn publish(&self, manifest: CheckpointManifest) -> Result<(), SharedLogError>;
    fn latest_for_mount(
        &self,
        mount: MountId,
    ) -> Result<Option<CheckpointManifest>, SharedLogError>;
}

#[derive(Debug, Default)]
pub struct MemoryCheckpointCatalog {
    manifests: Mutex<BTreeMap<MountId, CheckpointManifest>>,
}

#[derive(Debug)]
pub struct FileCheckpointCatalog {
    path: PathBuf,
    inner: Mutex<Option<CheckpointManifest>>,
}

impl MemoryCheckpointCatalog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CheckpointCatalog for MemoryCheckpointCatalog {
    fn publish(&self, manifest: CheckpointManifest) -> Result<(), SharedLogError> {
        if manifest.id.is_empty() {
            return Err(SharedLogError::EmptyCheckpointId);
        }
        if manifest.artifact.uri.is_empty() {
            return Err(SharedLogError::EmptyCheckpointArtifactUri);
        }
        let mut manifests = self
            .manifests
            .lock()
            .map_err(|_| SharedLogError::Backend("checkpoint catalog mutex poisoned".to_owned()))?;
        let replace = manifests
            .get(&manifest.mount)
            .map(|current| checkpoint_is_newer(&manifest, current))
            .unwrap_or(true);
        if replace {
            manifests.insert(manifest.mount, manifest);
        }
        Ok(())
    }

    fn latest_for_mount(
        &self,
        mount: MountId,
    ) -> Result<Option<CheckpointManifest>, SharedLogError> {
        self.manifests
            .lock()
            .map(|manifests| manifests.get(&mount).cloned())
            .map_err(|_| SharedLogError::Backend("checkpoint catalog mutex poisoned".to_owned()))
    }
}

impl FileCheckpointCatalog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedLogError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(to_backend_error)?;
            }
        }
        let manifest = read_manifest(&path)?;
        Ok(Self {
            path,
            inner: Mutex::new(manifest),
        })
    }
}

impl CheckpointCatalog for FileCheckpointCatalog {
    fn publish(&self, manifest: CheckpointManifest) -> Result<(), SharedLogError> {
        if manifest.id.is_empty() {
            return Err(SharedLogError::EmptyCheckpointId);
        }
        if manifest.artifact.uri.is_empty() {
            return Err(SharedLogError::EmptyCheckpointArtifactUri);
        }
        let mut current = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("checkpoint catalog mutex poisoned".to_owned()))?;
        let replace = current
            .as_ref()
            .map(|existing| checkpoint_is_newer(&manifest, existing))
            .unwrap_or(true);
        if replace {
            write_manifest(&self.path, &manifest)?;
            *current = Some(manifest);
        }
        Ok(())
    }

    fn latest_for_mount(
        &self,
        mount: MountId,
    ) -> Result<Option<CheckpointManifest>, SharedLogError> {
        self.inner
            .lock()
            .map(|manifest| {
                manifest
                    .as_ref()
                    .filter(|manifest| manifest.mount == mount)
                    .cloned()
            })
            .map_err(|_| SharedLogError::Backend("checkpoint catalog mutex poisoned".to_owned()))
    }
}

fn checkpoint_is_newer(next: &CheckpointManifest, current: &CheckpointManifest) -> bool {
    (
        next.frontier.applied_position.term,
        next.frontier.applied_position.index,
    ) >= (
        current.frontier.applied_position.term,
        current.frontier.applied_position.index,
    )
}

fn read_manifest(path: &Path) -> Result<Option<CheckpointManifest>, SharedLogError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(to_backend_error(err)),
    };
    let mut encoded = Vec::new();
    file.read_to_end(&mut encoded).map_err(to_backend_error)?;
    if encoded.is_empty() {
        return Ok(None);
    }
    decode_manifest(&encoded).map(Some)
}

fn write_manifest(path: &Path, manifest: &CheckpointManifest) -> Result<(), SharedLogError> {
    let tmp = checkpoint_temp_path(path);
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(to_backend_error)?;
        file.write_all(&encode_manifest(manifest)?)
            .map_err(to_backend_error)?;
        file.flush().map_err(to_backend_error)?;
        file.sync_data().map_err(to_backend_error)?;
    }
    fs::rename(&tmp, path).map_err(to_backend_error)?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_data();
            }
        }
    }
    Ok(())
}

fn checkpoint_temp_path(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let file_name = path
        .file_name()
        .map(|name| {
            let mut name = name.to_os_string();
            name.push(".tmp");
            name
        })
        .unwrap_or_else(|| "metadata.checkpoint.tmp".into());
    tmp.set_file_name(file_name);
    tmp
}

fn encode_manifest(manifest: &CheckpointManifest) -> Result<Vec<u8>, SharedLogError> {
    let id_len = u32::try_from(manifest.id.len())
        .map_err(|_| SharedLogError::Backend("checkpoint id is too large".to_owned()))?;
    let uri_len = u32::try_from(manifest.artifact.uri.len())
        .map_err(|_| SharedLogError::Backend("checkpoint artifact uri is too large".to_owned()))?;
    let digest_len = u32::try_from(manifest.artifact.digest.len()).map_err(|_| {
        SharedLogError::Backend("checkpoint artifact digest is too large".to_owned())
    })?;
    let mut out = Vec::with_capacity(
        8 + 3 * 4
            + 8 * 8
            + manifest.id.len()
            + manifest.artifact.uri.len()
            + manifest.artifact.digest.len(),
    );
    out.extend_from_slice(CHECKPOINT_MAGIC);
    out.extend_from_slice(&id_len.to_be_bytes());
    out.extend_from_slice(&uri_len.to_be_bytes());
    out.extend_from_slice(&digest_len.to_be_bytes());
    push_u64(&mut out, manifest.mount.get());
    push_u64(&mut out, manifest.frontier.durable_position.term.get());
    push_u64(&mut out, manifest.frontier.durable_position.index.get());
    push_u64(&mut out, manifest.frontier.applied_position.term.get());
    push_u64(&mut out, manifest.frontier.applied_position.index.get());
    push_u64(&mut out, manifest.frontier.min_retained_index.get());
    push_u64(&mut out, manifest.frontier.max_commit_version.get());
    push_u64(&mut out, manifest.artifact.size_bytes);
    out.extend_from_slice(&manifest.id);
    out.extend_from_slice(&manifest.artifact.uri);
    out.extend_from_slice(&manifest.artifact.digest);
    Ok(out)
}

fn decode_manifest(encoded: &[u8]) -> Result<CheckpointManifest, SharedLogError> {
    let mut input = ManifestDecoder::new(encoded);
    input.magic()?;
    let id_len = input.u32()? as usize;
    let uri_len = input.u32()? as usize;
    let digest_len = input.u32()? as usize;
    let mount = MountId::new(input.u64()?)
        .map_err(|err| SharedLogError::Backend(format!("invalid checkpoint mount id: {err}")))?;
    let durable_term = LogTerm::new(input.u64()?)?;
    let durable_index = LogIndex::new(input.u64()?)?;
    let applied_term = LogTerm::new(input.u64()?)?;
    let applied_index = LogIndex::new(input.u64()?)?;
    let min_retained_index = LogIndex::new(input.u64()?)?;
    let max_commit_version =
        Version::new(input.u64()?).map_err(|err| SharedLogError::Backend(err.to_string()))?;
    let size_bytes = input.u64()?;
    let id = input.bytes(id_len)?;
    let uri = input.bytes(uri_len)?;
    let digest = input.bytes(digest_len)?;
    input.finish()?;
    CheckpointManifest::new(
        id,
        mount,
        CheckpointFrontier {
            durable_position: LogPosition {
                term: durable_term,
                index: durable_index,
            },
            applied_position: LogPosition {
                term: applied_term,
                index: applied_index,
            },
            min_retained_index,
            max_commit_version,
        },
        CheckpointArtifact::new(uri, digest, size_bytes)?,
    )
}

struct ManifestDecoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> ManifestDecoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn magic(&mut self) -> Result<(), SharedLogError> {
        let magic = self.take(CHECKPOINT_MAGIC.len())?;
        if magic != CHECKPOINT_MAGIC {
            return Err(SharedLogError::Backend(
                "checkpoint catalog marker has invalid magic".to_owned(),
            ));
        }
        Ok(())
    }

    fn u32(&mut self) -> Result<u32, SharedLogError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes(
            bytes
                .try_into()
                .expect("checkpoint u32 field has fixed width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, SharedLogError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes(
            bytes
                .try_into()
                .expect("checkpoint u64 field has fixed width"),
        ))
    }

    fn bytes(&mut self, len: usize) -> Result<Vec<u8>, SharedLogError> {
        self.take(len).map(<[u8]>::to_vec)
    }

    fn finish(self) -> Result<(), SharedLogError> {
        if self.offset != self.input.len() {
            return Err(SharedLogError::Backend(
                "checkpoint catalog marker has trailing bytes".to_owned(),
            ));
        }
        Ok(())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], SharedLogError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            SharedLogError::Backend("checkpoint marker length overflow".to_owned())
        })?;
        if end > self.input.len() {
            return Err(SharedLogError::Backend(
                "checkpoint catalog marker is truncated".to_owned(),
            ));
        }
        let out = &self.input[self.offset..end];
        self.offset = end;
        Ok(out)
    }
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn to_backend_error(err: impl std::fmt::Display) -> SharedLogError {
    SharedLogError::Backend(err.to_string())
}
