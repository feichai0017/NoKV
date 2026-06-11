//! Filesystem consistency check (fsck).
//!
//! Verifies the **live** namespace has no dangling block references: every
//! object a live file points at must still exist in the object store. This is
//! the defense-in-depth complement to the object-first write ordering —
//! ordering *prevents* metadata from ever referencing a missing object, and
//! fsck *detects* any drift after the fact (an out-of-band object deletion, an
//! eventual-consistency anomaly in external storage, or a latent bug).
//!
//! The scan walks live inodes at their **current body generation**, so
//! superseded and snapshot-pinned generations are not mistaken for dangling
//! references, and a clone's borrowed (source-owned) block keys resolve against
//! the source objects that still exist. Reclaiming *orphan* objects (written but
//! never referenced) is the opposite direction and requires an object `list`; it
//! is not part of this scan.

use super::*;
use crate::layout::inode_prefix;

/// A live file's block reference whose backing object is missing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DanglingBlock {
    pub inode: u64,
    pub generation: u64,
    pub object_key: String,
}

/// The outcome of an [`NoKvFs::fsck_dangling_blocks`] scan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FsckReport {
    pub inodes_scanned: usize,
    pub files_scanned: usize,
    pub blocks_checked: usize,
    pub dangling: Vec<DanglingBlock>,
}

impl FsckReport {
    pub fn is_consistent(&self) -> bool {
        self.dangling.is_empty()
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    /// Scan up to `limit` live inodes (`0` = all) and verify that every block a
    /// live file references still exists in the object store.
    pub fn fsck_dangling_blocks(&self, limit: usize) -> Result<FsckReport, MetadError> {
        let version = self.read_version()?;
        let inode_rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Inode,
            prefix: inode_prefix(self.mount),
            start_after: None,
            version,
            limit,
            purpose: ReadPurpose::UserStrong,
        })?;

        let mut report = FsckReport {
            inodes_scanned: inode_rows.len(),
            ..FsckReport::default()
        };

        for row in inode_rows {
            let attr = decode_inode_attr(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            if attr.file_type != FileType::File {
                continue;
            }
            let Some(body) = self.body_descriptor(attr.inode)? else {
                continue; // a file with no body yet has nothing to verify
            };
            report.files_scanned += 1;

            let manifest_rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::ChunkManifest,
                prefix: chunk_manifest_prefix(self.mount, attr.inode, body.generation),
                start_after: None,
                version,
                limit: 0,
                purpose: ReadPurpose::UserStrong,
            })?;
            for manifest_row in manifest_rows {
                if chunk_index_from_manifest_key(&manifest_row.key)? == BODY_SUMMARY_CHUNK_INDEX {
                    continue;
                }
                let manifest = decode_chunk_manifest(&manifest_row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                for block in manifest.slices.iter().flat_map(|slice| slice.blocks.iter()) {
                    report.blocks_checked += 1;
                    let key = ObjectKey::new(block.object_key.clone())?;
                    if self.objects.head(&key)?.is_none() {
                        report.dangling.push(DanglingBlock {
                            inode: attr.inode.get(),
                            generation: body.generation,
                            object_key: block.object_key.clone(),
                        });
                    }
                }
            }
        }
        Ok(report)
    }
}
