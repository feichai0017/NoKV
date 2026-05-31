use std::collections::{BTreeMap, BTreeSet, HashSet};

use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::BasicNode;
use nokv_raftstore_server::PeerEndpointCatalog;

pub(crate) fn ensure_merge_store_coverage(
    target: &metapb::RegionDescriptor,
    source: &metapb::RegionDescriptor,
) -> Result<(), tonic::Status> {
    let target_stores = region_peer_store_ids(target)?;
    let source_stores = region_peer_store_ids(source)?;
    if target_stores == source_stores {
        return Ok(());
    }
    Err(tonic::Status::unimplemented(format!(
        "merge target region {} and source region {} must cover the same store set before raftstore can safely retire source peers",
        target.region_id, source.region_id
    )))
}

pub(crate) fn region_peer_store_ids(
    descriptor: &metapb::RegionDescriptor,
) -> Result<BTreeSet<u64>, tonic::Status> {
    let mut stores = BTreeSet::new();
    for peer in &descriptor.peers {
        if peer.store_id == 0 || peer.peer_id == 0 {
            return Err(tonic::Status::invalid_argument(format!(
                "region {} has an invalid peer entry",
                descriptor.region_id
            )));
        }
        if !stores.insert(peer.store_id) {
            return Err(tonic::Status::invalid_argument(format!(
                "region {} has duplicate peer store {}",
                descriptor.region_id, peer.store_id
            )));
        }
    }
    if stores.is_empty() {
        return Err(tonic::Status::invalid_argument(format!(
            "region {} has no peers",
            descriptor.region_id
        )));
    }
    Ok(stores)
}

pub(crate) fn merged_source_region_ids_for_store(
    descriptors: &[metapb::RegionDescriptor],
    store_id: u64,
) -> HashSet<u64> {
    descriptors
        .iter()
        .filter(|descriptor| {
            descriptor
                .peers
                .iter()
                .any(|peer| peer.store_id == store_id && peer.peer_id != 0)
        })
        .flat_map(|descriptor| {
            descriptor
                .lineage
                .iter()
                .filter(|lineage| {
                    lineage.region_id != 0
                        && lineage.kind == metapb::DescriptorLineageKind::MergeSource as i32
                })
                .map(|lineage| lineage.region_id)
        })
        .collect()
}

pub(crate) fn local_peer_for_store(
    descriptor: &metapb::RegionDescriptor,
    store_id: u64,
) -> Result<metapb::RegionPeer, tonic::Status> {
    descriptor
        .peers
        .iter()
        .find(|peer| peer.store_id == store_id)
        .cloned()
        .ok_or_else(|| {
            tonic::Status::failed_precondition(format!(
                "region {} has no peer on store {}",
                descriptor.region_id, store_id
            ))
        })
}

pub(crate) fn local_peer_is_first(
    descriptor: &metapb::RegionDescriptor,
    local_peer: &metapb::RegionPeer,
) -> bool {
    descriptor.peers.first().is_some_and(|peer| {
        peer.store_id == local_peer.store_id && peer.peer_id == local_peer.peer_id
    })
}

pub(crate) fn descriptor_membership_nodes(
    descriptor: &metapb::RegionDescriptor,
    local_peer: &metapb::RegionPeer,
    local_addr: &str,
    peer_endpoints: &PeerEndpointCatalog,
) -> Result<BTreeMap<u64, BasicNode>, tonic::Status> {
    let mut members = BTreeMap::new();
    for peer in &descriptor.peers {
        if peer.store_id == 0 || peer.peer_id == 0 {
            return Err(tonic::Status::invalid_argument(format!(
                "region {} has an invalid peer entry",
                descriptor.region_id
            )));
        }
        let node = if peer.store_id == local_peer.store_id && peer.peer_id == local_peer.peer_id {
            BasicNode::new(local_addr.to_owned())
        } else {
            peer_endpoints.node_for_peer(peer.store_id, peer.peer_id)?
        };
        if members.insert(peer.peer_id, node).is_some() {
            return Err(tonic::Status::invalid_argument(format!(
                "region {} has duplicate peer id {}",
                descriptor.region_id, peer.peer_id
            )));
        }
    }
    Ok(members)
}

pub(crate) fn build_split_descriptors(
    parent: &metapb::RegionDescriptor,
    split_key: &[u8],
    child: &metapb::RegionDescriptor,
) -> Result<(metapb::RegionDescriptor, metapb::RegionDescriptor), tonic::Status> {
    if parent.region_id == 0 || child.region_id == 0 {
        return Err(tonic::Status::invalid_argument(
            "split parent and child region ids are required",
        ));
    }
    if split_key.is_empty()
        || split_key <= parent.start_key.as_slice()
        || (!parent.end_key.is_empty() && split_key >= parent.end_key.as_slice())
    {
        return Err(tonic::Status::invalid_argument(
            "split key must be inside parent range",
        ));
    }
    let Some(parent_epoch) = parent.epoch.clone() else {
        return Err(tonic::Status::invalid_argument(
            "split parent epoch is required",
        ));
    };
    let mut left = parent.clone();
    left.end_key = split_key.to_vec();
    let epoch = left.epoch.get_or_insert_with(Default::default);
    epoch.version = epoch.version.saturating_add(1);
    left.hash.clear();
    append_split_lineage(&mut left, parent, &parent_epoch);

    let mut right = child.clone();
    if right.start_key.is_empty() {
        right.start_key = split_key.to_vec();
    }
    if right.start_key != split_key {
        return Err(tonic::Status::invalid_argument(
            "split child start key must equal split key",
        ));
    }
    if right.end_key != parent.end_key {
        return Err(tonic::Status::invalid_argument(
            "split child end key must equal original parent end key",
        ));
    }
    if right.epoch.is_none() {
        right.epoch = Some(parent_epoch.clone());
    }
    if right.peers.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "split child peers are required",
        ));
    }
    right.hash.clear();
    append_split_lineage(&mut right, parent, &parent_epoch);
    Ok((left, right))
}

pub(crate) fn append_split_lineage(
    descriptor: &mut metapb::RegionDescriptor,
    parent: &metapb::RegionDescriptor,
    parent_epoch: &metapb::RegionEpoch,
) {
    descriptor.lineage.push(metapb::DescriptorLineageRef {
        region_id: parent.region_id,
        epoch: Some(parent_epoch.clone()),
        hash: parent.hash.clone(),
        kind: metapb::DescriptorLineageKind::SplitParent as i32,
    });
}

pub(crate) fn build_merge_descriptor(
    target: &metapb::RegionDescriptor,
    source: &metapb::RegionDescriptor,
) -> Result<metapb::RegionDescriptor, tonic::Status> {
    if target.region_id == 0 || source.region_id == 0 {
        return Err(tonic::Status::invalid_argument(
            "merge target and source region ids are required",
        ));
    }
    if target.end_key != source.start_key {
        return Err(tonic::Status::unimplemented(
            "raftstore merge currently requires the source region to be the target's right sibling",
        ));
    }
    let Some(source_epoch) = source.epoch.clone() else {
        return Err(tonic::Status::invalid_argument(
            "merge source epoch is required",
        ));
    };
    let Some(target_epoch) = target.epoch.clone() else {
        return Err(tonic::Status::invalid_argument(
            "merge target epoch is required",
        ));
    };
    let mut merged = target.clone();
    merged.end_key = source.end_key.clone();
    let epoch = merged.epoch.get_or_insert(target_epoch);
    epoch.version = epoch.version.saturating_add(1);
    merged.hash.clear();
    merged.lineage.push(metapb::DescriptorLineageRef {
        region_id: source.region_id,
        epoch: Some(source_epoch),
        hash: source.hash.clone(),
        kind: metapb::DescriptorLineageKind::MergeSource as i32,
    });
    Ok(merged)
}

pub(crate) fn merge_region_ids(
    target: &metapb::RegionDescriptor,
    source: &metapb::RegionDescriptor,
) -> (u64, u64) {
    if source.start_key < target.start_key {
        (source.region_id, target.region_id)
    } else {
        (target.region_id, source.region_id)
    }
}

pub(crate) fn merge_source_already_absorbed(
    target: &metapb::RegionDescriptor,
    source_region_id: u64,
) -> bool {
    target.lineage.iter().any(|lineage| {
        lineage.region_id == source_region_id
            && lineage.kind == metapb::DescriptorLineageKind::MergeSource as i32
    })
}

pub(crate) fn split_root_event(
    kind: metapb::RootEventKind,
    left: &metapb::RegionDescriptor,
    right: &metapb::RegionDescriptor,
) -> metapb::RootEvent {
    metapb::RootEvent {
        kind: kind as i32,
        payload: Some(metapb::root_event::Payload::RangeSplit(
            metapb::RootRangeSplit {
                parent_region_id: left.region_id,
                split_key: right.start_key.clone(),
                left: Some(left.clone()),
                right: Some(right.clone()),
                ..Default::default()
            },
        )),
    }
}

pub(crate) fn merge_root_event(
    kind: metapb::RootEventKind,
    left_region_id: u64,
    right_region_id: u64,
    merged: &metapb::RegionDescriptor,
) -> metapb::RootEvent {
    metapb::RootEvent {
        kind: kind as i32,
        payload: Some(metapb::root_event::Payload::RangeMerge(
            metapb::RootRangeMerge {
                left_region_id,
                right_region_id,
                merged: Some(merged.clone()),
                ..Default::default()
            },
        )),
    }
}
