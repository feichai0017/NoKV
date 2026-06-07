use fuser::{Errno, INodeNo, RenameFlags};
use nokv_types::AdvisoryLockKind;

#[derive(Clone, Copy, Debug)]
pub(super) struct FuseLockRequest {
    pub(super) ino: INodeNo,
    pub(super) owner: fuser::LockOwner,
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) typ: i32,
    pub(super) pid: u32,
    pub(super) wait: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FuseRenameMode {
    ReplaceIfTargetExists,
    #[cfg(target_os = "linux")]
    NoReplace,
}

pub(super) fn advisory_lock_kind_from_fuse(typ: i32) -> Result<AdvisoryLockKind, Errno> {
    if typ == i32::from(libc::F_RDLCK) {
        Ok(AdvisoryLockKind::Read)
    } else if typ == i32::from(libc::F_WRLCK) {
        Ok(AdvisoryLockKind::Write)
    } else if typ == i32::from(libc::F_UNLCK) {
        Ok(AdvisoryLockKind::Unlock)
    } else {
        Err(Errno::EINVAL)
    }
}

pub(super) fn advisory_lock_kind_to_fuse(kind: AdvisoryLockKind) -> Result<i32, Errno> {
    match kind {
        AdvisoryLockKind::Read => Ok(i32::from(libc::F_RDLCK)),
        AdvisoryLockKind::Write => Ok(i32::from(libc::F_WRLCK)),
        AdvisoryLockKind::Unlock => Ok(i32::from(libc::F_UNLCK)),
    }
}

pub(super) fn fuse_rename_mode(flags: RenameFlags) -> Result<FuseRenameMode, Errno> {
    if flags.is_empty() {
        return Ok(FuseRenameMode::ReplaceIfTargetExists);
    }
    #[cfg(target_os = "linux")]
    {
        if flags == RenameFlags::RENAME_NOREPLACE {
            return Ok(FuseRenameMode::NoReplace);
        }
    }
    Err(Errno::EINVAL)
}
