use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{FileAttr, FileType as FuseFileType, INodeNo};
use nokv_types::{FileType, InodeAttr};

const BLOCK_SIZE: u32 = 4096;
const BLOCK_BYTES: u64 = 512;

pub fn fuse_file_type(file_type: FileType) -> FuseFileType {
    match file_type {
        FileType::File => FuseFileType::RegularFile,
        FileType::Directory => FuseFileType::Directory,
        FileType::Symlink => FuseFileType::Symlink,
        FileType::NamedPipe => FuseFileType::NamedPipe,
        FileType::CharDevice => FuseFileType::CharDevice,
        FileType::BlockDevice => FuseFileType::BlockDevice,
        FileType::Socket => FuseFileType::Socket,
    }
}

pub fn file_attr(attr: &InodeAttr) -> FileAttr {
    let mtime = system_time_from_millis(attr.mtime_ms);
    let ctime = system_time_from_millis(attr.ctime_ms);
    FileAttr {
        ino: INodeNo(attr.inode.get()),
        size: attr.size,
        blocks: attr.size.div_ceil(BLOCK_BYTES),
        atime: mtime,
        mtime,
        ctime,
        crtime: ctime,
        kind: fuse_file_type(attr.file_type),
        perm: (attr.mode & 0o7777) as u16,
        nlink: match attr.file_type {
            FileType::Directory => 2,
            FileType::File
            | FileType::Symlink
            | FileType::NamedPipe
            | FileType::CharDevice
            | FileType::BlockDevice
            | FileType::Socket => 1,
        },
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        blksize: BLOCK_SIZE,
        flags: 0,
    }
}

fn system_time_from_millis(millis: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_millis(millis))
        .unwrap_or(UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_types::InodeId;

    #[test]
    fn file_attr_maps_model_file_to_fuse_attr() {
        let attr = InodeAttr {
            inode: InodeId::new(42).unwrap(),
            file_type: FileType::File,
            mode: 0o640,
            uid: 501,
            gid: 20,
            rdev: 0,
            size: 513,
            generation: 9,
            mtime_ms: 10,
            ctime_ms: 11,
        };
        let fuse = file_attr(&attr);
        assert_eq!(fuse.ino, INodeNo(42));
        assert_eq!(fuse.kind, FuseFileType::RegularFile);
        assert_eq!(fuse.perm, 0o640);
        assert_eq!(fuse.blocks, 2);
        assert_eq!(fuse.uid, 501);
        assert_eq!(fuse.gid, 20);
    }

    #[test]
    fn directories_report_directory_kind_and_two_links() {
        let attr = InodeAttr {
            inode: InodeId::new(7).unwrap(),
            file_type: FileType::Directory,
            mode: 0o755,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            size: 0,
            generation: 1,
            mtime_ms: 1,
            ctime_ms: 1,
        };
        let fuse = file_attr(&attr);
        assert_eq!(fuse.kind, FuseFileType::Directory);
        assert_eq!(fuse.nlink, 2);
    }

    #[test]
    fn special_nodes_report_kind_and_device_number() {
        let attr = InodeAttr {
            inode: InodeId::new(9).unwrap(),
            file_type: FileType::CharDevice,
            mode: 0o666,
            uid: 0,
            gid: 0,
            rdev: 0x1234,
            size: 0,
            generation: 3,
            mtime_ms: 1,
            ctime_ms: 1,
        };
        let fuse = file_attr(&attr);
        assert_eq!(fuse.kind, FuseFileType::CharDevice);
        assert_eq!(fuse.rdev, 0x1234);
        assert_eq!(fuse.nlink, 1);
    }
}
