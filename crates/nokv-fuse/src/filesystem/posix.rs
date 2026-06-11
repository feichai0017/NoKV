use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::time::{SystemTime, UNIX_EPOCH};

use fuser::{AccessFlags, Errno, INodeNo, TimeOrNow};
use nokv_types::{DentryName, FileType, InodeAttr, InodeId};

const MODE_TYPE_MASK: u32 = 0o170000;
const MODE_NAMED_PIPE: u32 = 0o010000;
const MODE_CHAR_DEVICE: u32 = 0o020000;
const MODE_DIRECTORY: u32 = 0o040000;
const MODE_BLOCK_DEVICE: u32 = 0o060000;
const MODE_REGULAR_FILE: u32 = 0o100000;
const MODE_SYMLINK: u32 = 0o120000;
const MODE_SOCKET: u32 = 0o140000;
const FUSE_COPY_FILE_RANGE_MAX_BYTES: u64 = 1024 * 1024;
const FALLOC_FL_KEEP_SIZE: i32 = 0x01;

pub(super) fn inode_id(ino: INodeNo) -> Result<InodeId, Errno> {
    InodeId::new(ino.0).map_err(|_| Errno::EINVAL)
}

pub(super) fn file_type_from_mknod_mode(mode: u32) -> Result<FileType, Errno> {
    match mode & MODE_TYPE_MASK {
        0 | MODE_REGULAR_FILE => Ok(FileType::File),
        MODE_NAMED_PIPE => Ok(FileType::NamedPipe),
        MODE_CHAR_DEVICE => Ok(FileType::CharDevice),
        MODE_BLOCK_DEVICE => Ok(FileType::BlockDevice),
        MODE_SOCKET => Ok(FileType::Socket),
        MODE_DIRECTORY | MODE_SYMLINK => Err(Errno::EINVAL),
        _ => Err(Errno::EINVAL),
    }
}

pub(super) fn dentry_name(name: &OsStr) -> Result<DentryName, Errno> {
    DentryName::new(name.as_bytes().to_vec()).map_err(|_| Errno::EINVAL)
}

pub(super) fn time_or_now_ms(value: TimeOrNow) -> u64 {
    match value {
        TimeOrNow::SpecificTime(time) => system_time_ms(time),
        TimeOrNow::Now => system_time_ms(SystemTime::now()),
    }
}

pub(super) fn system_time_ms(time: SystemTime) -> u64 {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

pub(super) fn resolve_fallocate_size(
    current_size: u64,
    offset: u64,
    length: u64,
    mode: i32,
) -> Result<Option<u64>, Errno> {
    if length == 0 {
        return Err(Errno::EINVAL);
    }
    let end = offset.checked_add(length).ok_or(Errno::EINVAL)?;
    match mode {
        0 => Ok(Some(current_size.max(end))),
        FALLOC_FL_KEEP_SIZE => Ok(None),
        _ => Err(Errno::EOPNOTSUPP),
    }
}

pub(super) fn copy_file_range_size(len: u64) -> u32 {
    len.min(FUSE_COPY_FILE_RANGE_MAX_BYTES)
        .min(u64::from(u32::MAX)) as u32
}

pub(super) fn resolve_lseek(size: u64, offset: i64, whence: i32) -> Result<i64, Errno> {
    match whence {
        libc::SEEK_SET => {
            u64::try_from(offset).map_err(|_| Errno::EINVAL)?;
            Ok(offset)
        }
        libc::SEEK_END => {
            let size = i128::from(size);
            let next = size + i128::from(offset);
            if !(0..=i128::from(i64::MAX)).contains(&next) {
                return Err(Errno::EINVAL);
            }
            Ok(next as i64)
        }
        libc::SEEK_DATA => {
            let offset = u64::try_from(offset).map_err(|_| Errno::EINVAL)?;
            if offset >= size {
                return Err(Errno::ENXIO);
            }
            i64::try_from(offset).map_err(|_| Errno::EINVAL)
        }
        libc::SEEK_HOLE => {
            let offset = u64::try_from(offset).map_err(|_| Errno::EINVAL)?;
            if offset > size {
                return Err(Errno::ENXIO);
            }
            i64::try_from(size).map_err(|_| Errno::EINVAL)
        }
        // FUSE does not pass the kernel's current file offset, so SEEK_CUR
        // cannot be answered accurately at the filesystem boundary.
        libc::SEEK_CUR => Err(Errno::EINVAL),
        _ => Err(Errno::EINVAL),
    }
}

pub(super) fn validate_access_mask(mask: AccessFlags) -> Result<(), Errno> {
    let supported = AccessFlags::R_OK | AccessFlags::W_OK | AccessFlags::X_OK;
    if mask.bits() & !supported.bits() == 0 {
        Ok(())
    } else {
        Err(Errno::EINVAL)
    }
}

pub(super) fn access_allowed(attr: &InodeAttr, uid: u32, gid: u32, mask: AccessFlags) -> bool {
    if mask.is_empty() {
        return true;
    }
    if uid == 0 {
        return !mask.contains(AccessFlags::X_OK)
            || attr.file_type == FileType::Directory
            || attr.mode & 0o111 != 0;
    }
    let shift = if uid == attr.uid {
        6
    } else if gid == attr.gid {
        3
    } else {
        0
    };
    let perms = (attr.mode >> shift) & 0o7;
    (!mask.contains(AccessFlags::R_OK) || perms & 0o4 != 0)
        && (!mask.contains(AccessFlags::W_OK) || perms & 0o2 != 0)
        && (!mask.contains(AccessFlags::X_OK) || perms & 0o1 != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lseek_resolves_end_data_and_hole_offsets() {
        assert_eq!(resolve_lseek(100, 5, libc::SEEK_SET).unwrap(), 5);
        assert_eq!(resolve_lseek(100, -10, libc::SEEK_END).unwrap(), 90);
        assert_eq!(resolve_lseek(100, 10, libc::SEEK_DATA).unwrap(), 10);
        assert_eq!(resolve_lseek(100, 10, libc::SEEK_HOLE).unwrap(), 100);
        assert_eq!(resolve_lseek(100, 100, libc::SEEK_HOLE).unwrap(), 100);
    }

    #[test]
    fn fallocate_size_resolves_sparse_extension() {
        assert_eq!(resolve_fallocate_size(10, 3, 4, 0).unwrap(), Some(10));
        assert_eq!(resolve_fallocate_size(10, 8, 5, 0).unwrap(), Some(13));
        assert_eq!(
            resolve_fallocate_size(10, 8, 5, FALLOC_FL_KEEP_SIZE).unwrap(),
            None
        );
    }

    #[test]
    fn fallocate_rejects_empty_overflow_or_unsupported_mode() {
        assert_eq!(
            resolve_fallocate_size(10, 0, 0, 0).unwrap_err().code(),
            Errno::EINVAL.code()
        );
        assert_eq!(
            resolve_fallocate_size(10, u64::MAX, 1, 0)
                .unwrap_err()
                .code(),
            Errno::EINVAL.code()
        );
        assert_eq!(
            resolve_fallocate_size(10, 0, 1, 0x02).unwrap_err().code(),
            Errno::EOPNOTSUPP.code()
        );
    }

    #[test]
    fn copy_file_range_size_is_bounded_for_fuse_thread_memory() {
        assert_eq!(copy_file_range_size(0), 0);
        assert_eq!(copy_file_range_size(7), 7);
        assert_eq!(
            copy_file_range_size(FUSE_COPY_FILE_RANGE_MAX_BYTES + 1),
            FUSE_COPY_FILE_RANGE_MAX_BYTES as u32
        );
    }

    #[test]
    fn lseek_rejects_invalid_or_unanswerable_offsets() {
        assert_eq!(
            resolve_lseek(100, -1, libc::SEEK_SET).unwrap_err().code(),
            Errno::EINVAL.code()
        );
        assert_eq!(
            resolve_lseek(100, 100, libc::SEEK_DATA).unwrap_err().code(),
            Errno::ENXIO.code()
        );
        assert_eq!(
            resolve_lseek(100, 101, libc::SEEK_HOLE).unwrap_err().code(),
            Errno::ENXIO.code()
        );
        assert_eq!(
            resolve_lseek(100, 0, libc::SEEK_CUR).unwrap_err().code(),
            Errno::EINVAL.code()
        );
    }

    #[test]
    fn access_helper_honors_owner_group_other_and_root_execute() {
        let attr = InodeAttr {
            inode: InodeId::new(42).unwrap(),
            file_type: FileType::File,
            mode: 0o640,
            uid: 1000,
            gid: 2000,
            rdev: 0,
            nlink: 1,
            size: 0,
            generation: 1,
            mtime_ms: 1,
            ctime_ms: 1,
        };
        assert!(access_allowed(&attr, 1000, 9, AccessFlags::R_OK));
        assert!(access_allowed(&attr, 1000, 9, AccessFlags::W_OK));
        assert!(access_allowed(&attr, 9, 2000, AccessFlags::R_OK));
        assert!(!access_allowed(&attr, 9, 2000, AccessFlags::W_OK));
        assert!(!access_allowed(&attr, 9, 9, AccessFlags::R_OK));
        assert!(!access_allowed(&attr, 0, 0, AccessFlags::X_OK));

        let executable_dir = InodeAttr {
            file_type: FileType::Directory,
            mode: 0o000,
            ..attr.clone()
        };
        assert!(access_allowed(&executable_dir, 0, 0, AccessFlags::X_OK));
        assert!(validate_access_mask(AccessFlags::from_bits_retain(0x4000)).is_err());
    }
}
