//! Holt-friendly key layout for NoKV metadata.
//!
//! This crate owns ordered keys and family-local prefixes. It does not own
//! namespace semantics, metadata execution, Holt tree handles, Raft state, or
//! object-store references.

mod codec;

use nokv_types::{DentryName, InodeId, MountId, RecordFamily};

pub const U64_WIDTH: usize = 8;
const U32_WIDTH: usize = 4;
pub const PATH_INDEX_DELIMITER: u8 = b'/';

pub use codec::{
    decode_allocator_state, decode_body_descriptor, decode_chunk_manifest,
    decode_dentry_projection, decode_inode_attr, decode_object_gc_record, decode_snapshot_pin,
    decode_watch_event, encode_allocator_state, encode_body_descriptor, encode_chunk_manifest,
    encode_dentry_projection, encode_inode_attr, encode_object_gc_record, encode_snapshot_pin,
    encode_watch_event, CodecError,
};

pub fn allocator_key(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH + 9);
    push_u64(&mut out, mount.get());
    out.extend_from_slice(b"allocator");
    out
}

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

pub fn dentry_mount_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH);
    push_u64(&mut out, mount.get());
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

pub fn xattr_prefix(mount: MountId, inode: InodeId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 2);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, inode.get());
    out
}

pub fn xattr_key(mount: MountId, inode: InodeId, name: &[u8]) -> Vec<u8> {
    let mut out = xattr_prefix(mount, inode);
    out.extend_from_slice(name);
    out
}

pub fn path_index_key(mount: MountId, components: &[DentryName]) -> Vec<u8> {
    let mut out = path_index_root_prefix(mount);
    for (index, component) in components.iter().enumerate() {
        if index > 0 {
            out.push(PATH_INDEX_DELIMITER);
        }
        out.extend_from_slice(component.as_bytes());
    }
    out
}

pub fn path_index_prefix(mount: MountId, components: &[DentryName]) -> Vec<u8> {
    let mut out = path_index_key(mount, components);
    if !out.ends_with(&[PATH_INDEX_DELIMITER]) {
        out.push(PATH_INDEX_DELIMITER);
    }
    out
}

fn path_index_root_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH + 1);
    push_u64(&mut out, mount.get());
    out.push(PATH_INDEX_DELIMITER);
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
    let mut out = watch_log_prefix(mount, scope);
    push_u64(&mut out, apply_index);
    push_u64(&mut out, event_id);
    out
}

pub fn watch_log_prefix(mount: MountId, scope: InodeId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 2);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, scope.get());
    out
}

pub fn snapshot_pin_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH);
    push_u64(&mut out, mount.get());
    out
}

pub fn snapshot_pin_key(mount: MountId, snapshot_id: u64) -> Vec<u8> {
    let mut out = snapshot_pin_prefix(mount);
    push_u64(&mut out, snapshot_id);
    out
}

pub fn gc_queue_prefix(mount: MountId) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH);
    push_u64(&mut out, mount.get());
    out
}

pub fn gc_object_key(
    mount: MountId,
    enqueue_version: u64,
    inode: InodeId,
    generation: u64,
    chunk_index: u64,
    block_index: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(U64_WIDTH * 6);
    push_u64(&mut out, mount.get());
    push_u64(&mut out, enqueue_version);
    push_u64(&mut out, inode.get());
    push_u64(&mut out, generation);
    push_u64(&mut out, chunk_index);
    push_u64(&mut out, block_index);
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
        RecordFamily::Xattr => 5,
        RecordFamily::ChunkManifest => 6,
        RecordFamily::Session => 7,
        RecordFamily::PathIndex => 8,
        RecordFamily::Watch => 9,
        RecordFamily::Snapshot => 10,
        RecordFamily::Gc => 11,
        RecordFamily::CommandDedupe => 12,
        RecordFamily::History => 13,
        RecordFamily::System => 14,
    }
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_types::{WatchEvent, WatchEventKind};

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
    fn allocator_key_is_mount_scoped() {
        let key = allocator_key(mount());
        assert!(key.starts_with(&mount().get().to_be_bytes()));
        assert_ne!(key, allocator_key(MountId::new(8).unwrap()));
    }

    #[test]
    fn allocator_state_codec_is_fixed_width() {
        let encoded = encode_allocator_state(42, 99);
        assert_eq!(encoded.len(), U64_WIDTH * 2);
        assert_eq!(decode_allocator_state(&encoded).unwrap(), (42, 99));

        let mut trailing = encoded;
        trailing.push(1);
        assert_eq!(
            decode_allocator_state(&trailing).unwrap_err(),
            CodecError::TrailingBytes
        );
    }

    #[test]
    fn watch_event_codec_preserves_typed_event_fields() {
        let event = WatchEvent {
            kind: WatchEventKind::PublishArtifact,
            parent: Some(inode(9)),
            name: Some(name(b"checkpoint.bin")),
            inode: inode(10),
            version: 42,
        };
        let encoded = encode_watch_event(&event);
        assert_eq!(decode_watch_event(&encoded).unwrap(), event);
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
    fn path_index_keys_are_component_prefix_safe() {
        let parent = path_index_prefix(mount(), &[name(b"runs")]);
        let parent_exact = path_index_key(mount(), &[name(b"runs")]);
        let child = path_index_key(mount(), &[name(b"runs"), name(b"ckpt")]);
        let sibling_prefix = path_index_key(mount(), &[name(b"runs-long")]);

        assert!(child.starts_with(&parent));
        assert!(!parent_exact.ends_with(&[PATH_INDEX_DELIMITER]));
        assert!(!sibling_prefix.starts_with(&parent));
    }

    #[test]
    fn xattr_keys_for_one_inode_share_a_contiguous_prefix() {
        let prefix = xattr_prefix(mount(), inode(9));
        let a = xattr_key(mount(), inode(9), b"user.a");
        let b = xattr_key(mount(), inode(9), b"user.b");
        let other_inode = xattr_key(mount(), inode(10), b"user.a");

        assert!(a.starts_with(&prefix));
        assert!(b.starts_with(&prefix));
        assert!(!other_inode.starts_with(&prefix));
        assert!(a < b);
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
    fn gc_keys_are_mount_and_version_ordered() {
        let key = gc_object_key(mount(), 10, inode(2), 3, 4, 5);
        let other_mount = gc_object_key(MountId::new(8).unwrap(), 10, inode(2), 3, 4, 5);
        let later = gc_object_key(mount(), 11, inode(2), 3, 4, 5);

        assert!(key.starts_with(&gc_queue_prefix(mount())));
        assert!(!other_mount.starts_with(&gc_queue_prefix(mount())));
        assert!(key < later);
    }

    #[test]
    fn snapshot_pin_keys_are_mount_scoped() {
        let key = snapshot_pin_key(mount(), 10);
        let other_mount = snapshot_pin_key(MountId::new(8).unwrap(), 10);
        let later = snapshot_pin_key(mount(), 11);

        assert!(key.starts_with(&snapshot_pin_prefix(mount())));
        assert!(!other_mount.starts_with(&snapshot_pin_prefix(mount())));
        assert!(key < later);
    }

    #[test]
    fn watch_log_keys_are_scope_and_cursor_ordered() {
        let key = watch_log_key(mount(), inode(2), 10, 0);
        let later = watch_log_key(mount(), inode(2), 10, 1);
        let other_scope = watch_log_key(mount(), inode(3), 10, 0);

        assert!(key.starts_with(&watch_log_prefix(mount(), inode(2))));
        assert!(!other_scope.starts_with(&watch_log_prefix(mount(), inode(2))));
        assert!(key < later);
    }

    #[test]
    fn history_prefix_is_exact_for_user_key() {
        let a = history_prefix(RecordFamily::Dentry, b"a");
        let aa = history_prefix(RecordFamily::Dentry, b"aa");
        assert!(!aa.starts_with(&a));
    }
}
