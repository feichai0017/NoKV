use std::collections::BTreeSet;

use nokv_mvcc::MetadataEngine;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::raft::v1 as raftpb;

use crate::RegionId;

use super::{invalid_raft_command, AppliedMetadataEngine};

impl<E> AppliedMetadataEngine<E>
where
    E: MetadataEngine,
{
    pub(super) fn apply_admin_command_at(
        &self,
        term: u64,
        index: u64,
        command: raftpb::AdminCommand,
    ) -> nokv_mvcc::Result<()> {
        let kind = raftpb::admin_command::Type::try_from(command.r#type)
            .unwrap_or(raftpb::admin_command::Type::Unknown);
        match kind {
            raftpb::admin_command::Type::Split => {
                let split = command
                    .split
                    .ok_or_else(|| invalid_raft_command("split admin payload is required"))?;
                self.apply_split_command_at(term, index, split)
            }
            raftpb::admin_command::Type::Merge => {
                let merge = command
                    .merge
                    .ok_or_else(|| invalid_raft_command("merge admin payload is required"))?;
                self.apply_merge_command_at(term, index, merge)
            }
            raftpb::admin_command::Type::Unknown => {
                Err(invalid_raft_command("unknown admin command type"))
            }
        }
    }

    fn apply_split_command_at(
        &self,
        term: u64,
        index: u64,
        split: raftpb::SplitCommand,
    ) -> nokv_mvcc::Result<()> {
        if split.parent_region_id != self.inner.region_id {
            return Err(invalid_raft_command(
                "split parent region does not match apply region",
            ));
        }
        if split.split_key.is_empty() {
            return Err(invalid_raft_command("split key is required"));
        }
        let parent = self.region_descriptor()?.ok_or_else(|| {
            invalid_raft_command("split parent descriptor must be installed before apply")
        })?;
        if split.split_key <= parent.start_key
            || (!parent.end_key.is_empty() && split.split_key >= parent.end_key)
        {
            return Err(invalid_raft_command(
                "split key must be inside parent descriptor range",
            ));
        }
        let mut child = split
            .child
            .ok_or_else(|| invalid_raft_command("split child descriptor is required"))?;
        if child.region_id == 0 {
            return Err(invalid_raft_command("split child region id is required"));
        }
        if child.start_key.is_empty() {
            child.start_key = split.split_key.clone();
        }
        if child.start_key != split.split_key {
            return Err(invalid_raft_command(
                "split child start key must equal split key",
            ));
        }
        if child.end_key != parent.end_key {
            return Err(invalid_raft_command(
                "split child end key must equal original parent end key",
            ));
        }

        let parent_epoch = parent.epoch.clone();
        let parent_hash = parent.hash.clone();
        let mut descriptor = parent.clone();
        descriptor.end_key = split.split_key;
        let epoch = descriptor.epoch.get_or_insert_with(Default::default);
        epoch.version = epoch.version.saturating_add(1);
        descriptor.hash.clear();
        if let Some(parent_epoch) = parent_epoch.clone() {
            push_split_lineage_once(
                &mut descriptor,
                parent.region_id,
                parent_epoch.clone(),
                &parent_hash,
            );
            if child.epoch.is_none() {
                child.epoch = Some(parent_epoch);
            }
        }
        if let Some(parent_epoch) = parent_epoch {
            push_split_lineage_once(&mut child, parent.region_id, parent_epoch, &parent_hash);
        }
        self.record_topology_descriptor(child)?;
        self.apply_region_descriptor_at(term, index, descriptor)
    }

    fn apply_merge_command_at(
        &self,
        term: u64,
        index: u64,
        merge: raftpb::MergeCommand,
    ) -> nokv_mvcc::Result<()> {
        if merge.target_region_id != self.inner.region_id {
            return Err(invalid_raft_command(
                "merge target region does not match apply region",
            ));
        }
        if merge.target_region_id == 0 || merge.source_region_id == 0 {
            return Err(invalid_raft_command(
                "merge target and source region ids are required",
            ));
        }
        if merge.target_region_id == merge.source_region_id {
            return Err(invalid_raft_command(
                "merge source region must differ from target region",
            ));
        }

        let target = self.region_descriptor()?.ok_or_else(|| {
            invalid_raft_command("merge target descriptor must be installed before apply")
        })?;
        if merge_source_already_absorbed(&target, merge.source_region_id) {
            let _ = self.take_topology_descriptor(merge.source_region_id)?;
            self.record_applied_status(term, index);
            return Ok(());
        }

        let source = self
            .topology_descriptor(merge.source_region_id)?
            .ok_or_else(|| {
                invalid_raft_command("merge source descriptor must be available before apply")
            })?;
        let descriptor = build_merge_descriptor_for_apply(&target, &source)?;
        let _ = self.take_topology_descriptor(merge.source_region_id)?;
        self.apply_region_descriptor_at(term, index, descriptor)
    }
}

fn push_split_lineage_once(
    descriptor: &mut metapb::RegionDescriptor,
    parent_region_id: RegionId,
    parent_epoch: metapb::RegionEpoch,
    parent_hash: &[u8],
) {
    let kind = metapb::DescriptorLineageKind::SplitParent as i32;
    if descriptor
        .lineage
        .iter()
        .any(|lineage| lineage.region_id == parent_region_id && lineage.kind == kind)
    {
        return;
    }
    descriptor.lineage.push(metapb::DescriptorLineageRef {
        region_id: parent_region_id,
        epoch: Some(parent_epoch),
        hash: parent_hash.to_vec(),
        kind,
    });
}

fn build_merge_descriptor_for_apply(
    target: &metapb::RegionDescriptor,
    source: &metapb::RegionDescriptor,
) -> nokv_mvcc::Result<metapb::RegionDescriptor> {
    if target.region_id == 0 || source.region_id == 0 {
        return Err(invalid_raft_command(
            "merge target and source region ids are required",
        ));
    }
    if target.end_key != source.start_key {
        return Err(invalid_raft_command(
            "merge source must be the target's right sibling",
        ));
    }
    ensure_merge_store_coverage_for_apply(target, source)?;
    let source_epoch = source
        .epoch
        .clone()
        .ok_or_else(|| invalid_raft_command("merge source epoch is required"))?;
    let target_epoch = target
        .epoch
        .clone()
        .ok_or_else(|| invalid_raft_command("merge target epoch is required"))?;

    let mut descriptor = target.clone();
    descriptor.end_key = source.end_key.clone();
    let epoch = descriptor.epoch.get_or_insert(target_epoch);
    epoch.version = epoch.version.saturating_add(1);
    descriptor.hash.clear();
    push_merge_lineage_once(
        &mut descriptor,
        source.region_id,
        source_epoch,
        &source.hash,
    );
    Ok(descriptor)
}

fn push_merge_lineage_once(
    descriptor: &mut metapb::RegionDescriptor,
    source_region_id: RegionId,
    source_epoch: metapb::RegionEpoch,
    source_hash: &[u8],
) {
    let kind = metapb::DescriptorLineageKind::MergeSource as i32;
    if descriptor
        .lineage
        .iter()
        .any(|lineage| lineage.region_id == source_region_id && lineage.kind == kind)
    {
        return;
    }
    descriptor.lineage.push(metapb::DescriptorLineageRef {
        region_id: source_region_id,
        epoch: Some(source_epoch),
        hash: source_hash.to_vec(),
        kind,
    });
}

fn merge_source_already_absorbed(
    target: &metapb::RegionDescriptor,
    source_region_id: RegionId,
) -> bool {
    target.lineage.iter().any(|lineage| {
        lineage.region_id == source_region_id
            && lineage.kind == metapb::DescriptorLineageKind::MergeSource as i32
    })
}

fn ensure_merge_store_coverage_for_apply(
    target: &metapb::RegionDescriptor,
    source: &metapb::RegionDescriptor,
) -> nokv_mvcc::Result<()> {
    let target_stores = region_peer_store_ids_for_apply(target)?;
    let source_stores = region_peer_store_ids_for_apply(source)?;
    if target_stores == source_stores {
        return Ok(());
    }
    Err(invalid_raft_command(
        "merge target and source must cover the same store set",
    ))
}

fn region_peer_store_ids_for_apply(
    descriptor: &metapb::RegionDescriptor,
) -> nokv_mvcc::Result<BTreeSet<u64>> {
    let mut stores = BTreeSet::new();
    for peer in &descriptor.peers {
        if peer.store_id == 0 || peer.peer_id == 0 {
            return Err(invalid_raft_command("merge descriptor has an invalid peer"));
        }
        if !stores.insert(peer.store_id) {
            return Err(invalid_raft_command(
                "merge descriptor has duplicate peer stores",
            ));
        }
    }
    if stores.is_empty() {
        return Err(invalid_raft_command("merge descriptor has no peers"));
    }
    Ok(stores)
}
