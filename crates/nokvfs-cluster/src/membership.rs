use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use nokvfs_types::MountId;

use crate::{LogTerm, MetadataMembership, SharedLogError};

const MEMBERSHIP_MAGIC: &[u8; 8] = b"NKFSMB01";

pub trait MembershipCatalog {
    fn publish(&self, membership: MetadataMembership) -> Result<(), SharedLogError>;
    fn latest_for_mount(
        &self,
        mount: MountId,
    ) -> Result<Option<MetadataMembership>, SharedLogError>;
}

#[derive(Debug, Default)]
pub struct MemoryMembershipCatalog {
    memberships: Mutex<BTreeMap<MountId, MetadataMembership>>,
}

#[derive(Debug)]
pub struct FileMembershipCatalog {
    path: PathBuf,
    inner: Mutex<Option<MetadataMembership>>,
}

impl MemoryMembershipCatalog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MembershipCatalog for MemoryMembershipCatalog {
    fn publish(&self, membership: MetadataMembership) -> Result<(), SharedLogError> {
        let mut memberships = self
            .memberships
            .lock()
            .map_err(|_| SharedLogError::Backend("membership catalog mutex poisoned".to_owned()))?;
        publish_membership(&mut memberships, membership)
    }

    fn latest_for_mount(
        &self,
        mount: MountId,
    ) -> Result<Option<MetadataMembership>, SharedLogError> {
        self.memberships
            .lock()
            .map(|memberships| memberships.get(&mount).cloned())
            .map_err(|_| SharedLogError::Backend("membership catalog mutex poisoned".to_owned()))
    }
}

impl FileMembershipCatalog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedLogError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(to_backend_error)?;
            }
        }
        let membership = read_membership(&path)?;
        Ok(Self {
            path,
            inner: Mutex::new(membership),
        })
    }
}

impl MembershipCatalog for FileMembershipCatalog {
    fn publish(&self, membership: MetadataMembership) -> Result<(), SharedLogError> {
        let mut current = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("membership catalog mutex poisoned".to_owned()))?;
        if let Some(existing) = current.as_ref() {
            validate_membership_replace(existing, &membership)?;
            if existing == &membership {
                return Ok(());
            }
        }
        write_membership(&self.path, &membership)?;
        *current = Some(membership);
        Ok(())
    }

    fn latest_for_mount(
        &self,
        mount: MountId,
    ) -> Result<Option<MetadataMembership>, SharedLogError> {
        self.inner
            .lock()
            .map(|membership| {
                membership
                    .as_ref()
                    .filter(|membership| membership.mount == mount)
                    .cloned()
            })
            .map_err(|_| SharedLogError::Backend("membership catalog mutex poisoned".to_owned()))
    }
}

fn publish_membership(
    memberships: &mut BTreeMap<MountId, MetadataMembership>,
    membership: MetadataMembership,
) -> Result<(), SharedLogError> {
    if let Some(existing) = memberships.get(&membership.mount) {
        validate_membership_replace(existing, &membership)?;
        if existing == &membership {
            return Ok(());
        }
    }
    memberships.insert(membership.mount, membership);
    Ok(())
}

fn validate_membership_replace(
    existing: &MetadataMembership,
    next: &MetadataMembership,
) -> Result<(), SharedLogError> {
    if next.term < existing.term {
        return Err(SharedLogError::StaleTerm {
            current: existing.term,
            proposed: next.term,
        });
    }
    if next.term == existing.term && next != existing {
        return Err(SharedLogError::MembershipConflict {
            mount: existing.mount,
            term: existing.term,
        });
    }
    Ok(())
}

fn read_membership(path: &Path) -> Result<Option<MetadataMembership>, SharedLogError> {
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
    decode_membership(&encoded).map(Some)
}

fn write_membership(path: &Path, membership: &MetadataMembership) -> Result<(), SharedLogError> {
    let tmp = membership_temp_path(path);
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(to_backend_error)?;
        file.write_all(&encode_membership(membership)?)
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

fn membership_temp_path(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let file_name = path
        .file_name()
        .map(|name| {
            let mut name = name.to_os_string();
            name.push(".tmp");
            name
        })
        .unwrap_or_else(|| "metadata.membership.tmp".into());
    tmp.set_file_name(file_name);
    tmp
}

fn encode_membership(membership: &MetadataMembership) -> Result<Vec<u8>, SharedLogError> {
    let voter_len = u32::try_from(membership.voters.len())
        .map_err(|_| SharedLogError::Backend("metadata voter set is too large".to_owned()))?;
    let learner_len = u32::try_from(membership.learners.len())
        .map_err(|_| SharedLogError::Backend("metadata learner set is too large".to_owned()))?;
    let mut out = Vec::with_capacity(
        8 + 2 * 4 + 3 * 8 + 8 * (membership.voters.len() + membership.learners.len()),
    );
    out.extend_from_slice(MEMBERSHIP_MAGIC);
    out.extend_from_slice(&voter_len.to_be_bytes());
    out.extend_from_slice(&learner_len.to_be_bytes());
    push_u64(&mut out, membership.mount.get());
    push_u64(&mut out, membership.term.get());
    push_u64(&mut out, membership.leader.get());
    for voter in &membership.voters {
        push_u64(&mut out, voter.get());
    }
    for learner in &membership.learners {
        push_u64(&mut out, learner.get());
    }
    Ok(out)
}

fn decode_membership(encoded: &[u8]) -> Result<MetadataMembership, SharedLogError> {
    let mut input = MembershipDecoder::new(encoded);
    input.magic()?;
    let voter_len = input.u32()? as usize;
    let learner_len = input.u32()? as usize;
    let mount = MountId::new(input.u64()?)
        .map_err(|err| SharedLogError::Backend(format!("invalid membership mount id: {err}")))?;
    let term = LogTerm::new(input.u64()?)?;
    let leader = crate::NodeId::new(input.u64()?)?;
    let mut voters = Vec::with_capacity(voter_len);
    for _ in 0..voter_len {
        voters.push(crate::NodeId::new(input.u64()?)?);
    }
    let mut learners = Vec::with_capacity(learner_len);
    for _ in 0..learner_len {
        learners.push(crate::NodeId::new(input.u64()?)?);
    }
    input.finish()?;
    MetadataMembership::new(mount, term, leader, voters, learners)
}

struct MembershipDecoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> MembershipDecoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn magic(&mut self) -> Result<(), SharedLogError> {
        let magic = self.take(MEMBERSHIP_MAGIC.len())?;
        if magic != MEMBERSHIP_MAGIC {
            return Err(SharedLogError::Backend(
                "membership catalog marker has invalid magic".to_owned(),
            ));
        }
        Ok(())
    }

    fn u32(&mut self) -> Result<u32, SharedLogError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes(
            bytes
                .try_into()
                .expect("membership u32 field has fixed width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, SharedLogError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes(
            bytes
                .try_into()
                .expect("membership u64 field has fixed width"),
        ))
    }

    fn finish(self) -> Result<(), SharedLogError> {
        if self.offset != self.input.len() {
            return Err(SharedLogError::Backend(
                "membership catalog marker has trailing bytes".to_owned(),
            ));
        }
        Ok(())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], SharedLogError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            SharedLogError::Backend("membership marker length overflow".to_owned())
        })?;
        if end > self.input.len() {
            return Err(SharedLogError::Backend(
                "membership catalog marker is truncated".to_owned(),
            ));
        }
        let bytes = &self.input[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn to_backend_error(err: impl std::fmt::Display) -> SharedLogError {
    SharedLogError::Backend(err.to_string())
}
