//! Holt-friendly key layout for NoKV-FS metadata.
//!
//! This crate owns ordered keys and family-local prefixes. It does not own
//! namespace semantics, metadata execution, Holt tree handles, Raft state, or
//! object-store references.

mod codec;

use nokv_fs_model::{DentryName, InodeId, MountId, RecordFamily};

pub const U64_WIDTH: usize = 8;
const U32_WIDTH: usize = 4;

pub use codec::{
    decode_body_descriptor, decode_dentry_projection, decode_inode_attr, encode_body_descriptor,
    encode_dentry_projection, encode_inode_attr, CodecError,
};

pub fn inode_key(mount: MountId, inode: InodeId) -> Vec<u8> {
    let mut out = inode_prefix(mount);
    push_u64(&mut out, inode.get());
    out
}

pub fn inode_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 2);
    push_u64(&mut out, mount.get());
    out
}

pub fn dentry_prefix(mount: MountId, parent: InodeId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 2);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, parent.get());
    out
}

pub fn dentry_key(mount: MountId, parent: InodeId, name: &DentryName) -> Vec<u8> {
    let mut out = dentry_prefix(mount, parent);
    out.extend_from_slice(name.as_bytes());
    out
}

pub fn parent_key(mount: MountId, child: InodeId, parent: InodeId, name: &DentryName) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 3 + name.as_bytes().len());
    push_u64(&mut out, mount.get());
    push_u64(&mut out, child.get());
    push_u64(&mut out, parent.get());
    out.extend_from_slice(name.as_bytes());
    out
}

pub fn chunk_manifest_prefix(mount: MountId, inode: InodeId, generation: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 3);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, inode.get());
    push_u64(&mut out, generation);
    out
}

pub fn chunk_manifest_key(
    mount: MountId,
    inode: InodeId,
    generation: u64,
    chunk_index: u64,
) -> Vec<u8> {
    let mut out = chunk_manifest_prefix(mount, inode, generation);
    push_u64(&mut out, chunk_index);
    out
}

pub fn watch_log_key(mount: MountId, scope: InodeId, apply_index: u64, event_id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 4);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, scope.get());
    push_u64(&mut out, apply_index);
    push_u64(&mut out, event_id);
    out
}

pub fn history_key(family: RecordFamily, user_key: &[u8], commit_version: u64) -> Vec<u8> {
    let mut out = history_prefix(family, user_key);
    push_u64(&mut out, u64::MAX - commit_version);
    out
}

pub fn history_prefix(family: RecordFamily, user_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + U32_WIDTH + user_key.len());
    out.push(family_tag(family));
    out.extend_from_slice(&(user_key.len() as u32).to_be_bytes());
    out.extend_from_slice(user_key);
    out
}

pub fn family_tag(family: RecordFamily) -> u8 {
    match family {
        RecordFamily::Mount => 1,
        RecordFamily::Inode => 2,
        RecordFamily::Dentry => 3,
        RecordFamily::Parent => 4,
        RecordFamily::ChunkManifest => 5,
        RecordFamily::Session => 6,
        RecordFamily::PathIndex => 7,
        RecordFamily::Watch => 8,
        RecordFamily::Snapshot => 9,
        RecordFamily::CommandDedupe => 10,
        RecordFamily::History => 11,
    }
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mount() -> MountId {
        MountId::new(7).unwrap()
    }

    fn inode(id: u64) -> InodeId {
        InodeId::new(id).unwrap()
    }

    fn name(raw: &[u8]) -> DentryName {
        DentryName::new(raw.to_vec()).unwrap()
    }

    #[test]
    fn dentry_keys_for_one_parent_share_a_contiguous_prefix() {
        let prefix = dentry_prefix(mount(), inode(9));
        let a = dentry_key(mount(), inode(9), &name(b"a"));
        let b = dentry_key(mount(), inode(9), &name(b"b"));
        let other_parent = dentry_key(mount(), inode(10), &name(b"a"));

        assert!(a.starts_with(&prefix));
        assert!(b.starts_with(&prefix));
        assert!(!other_parent.starts_with(&prefix));
        assert!(a < b);
    }

    #[test]
    fn big_endian_ids_keep_numeric_order() {
        assert!(inode_key(mount(), inode(2)) < inode_key(mount(), inode(10)));
    }

    #[test]
    fn inode_keys_for_one_mount_share_a_prefix() {
        let prefix = inode_prefix(mount());
        let key = inode_key(mount(), inode(42));
        let other_mount = inode_key(MountId::new(8).unwrap(), inode(42));
        assert!(key.starts_with(&prefix));
        assert!(!other_mount.starts_with(&prefix));
    }

    #[test]
    fn history_key_orders_newer_versions_first_for_same_user_key() {
        let key = inode_key(mount(), inode(2));
        let newer = history_key(RecordFamily::Inode, &key, 100);
        let older = history_key(RecordFamily::Inode, &key, 90);
        assert!(newer < older);
    }

    #[test]
    fn history_prefix_is_exact_for_user_key() {
        let a = history_prefix(RecordFamily::Dentry, b"a");
        let aa = history_prefix(RecordFamily::Dentry, b"aa");
        assert!(!aa.starts_with(&a));
    }
}
