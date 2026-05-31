use std::collections::{BTreeMap, BTreeSet};

use nokv_raftlog::LogEntry;
use openraft::{BasicNode, CommittedLeaderId, EntryPayload, LogId, Membership};
use prost::Message;

use crate::{
    Error, NodeId, OpenRaftEntry, Proposal, ProposalPayload, ProposalPayloadKind, RegionId,
};

const ENTRY_CODEC_VERSION: u32 = 1;
const PAYLOAD_BLANK: u32 = 1;
const PAYLOAD_NORMAL: u32 = 2;
const PAYLOAD_MEMBERSHIP: u32 = 3;
const NORMAL_RAFT_COMMAND: u32 = 1;
const NORMAL_REGION_DESCRIPTOR: u32 = 2;
const NORMAL_ADMIN_COMMAND: u32 = 3;
const NORMAL_METADATA_COMMAND: u32 = 4;

#[derive(Clone, PartialEq, Message)]
struct PersistedEntry {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint32, tag = "2")]
    payload_kind: u32,
    #[prost(bytes, tag = "3")]
    normal_payload: Vec<u8>,
    #[prost(message, optional, tag = "4")]
    membership: Option<PersistedMembership>,
    #[prost(uint64, tag = "5")]
    leader_node_id: NodeId,
    #[prost(uint32, tag = "6")]
    normal_kind: u32,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedMembership {
    #[prost(message, repeated, tag = "1")]
    configs: Vec<PersistedMembershipConfig>,
    #[prost(message, repeated, tag = "2")]
    nodes: Vec<PersistedNode>,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedMembershipConfig {
    #[prost(uint64, repeated, tag = "1")]
    voters: Vec<NodeId>,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedNode {
    #[prost(uint64, tag = "1")]
    id: NodeId,
    #[prost(string, tag = "2")]
    addr: String,
}

pub fn encode_log_entry(region_id: RegionId, entry: &OpenRaftEntry) -> Result<LogEntry, Error> {
    let persisted = match &entry.payload {
        EntryPayload::Blank => PersistedEntry {
            version: ENTRY_CODEC_VERSION,
            payload_kind: PAYLOAD_BLANK,
            normal_payload: Vec::new(),
            membership: None,
            leader_node_id: entry.log_id.leader_id.node_id,
            normal_kind: 0,
        },
        EntryPayload::Normal(proposal) => {
            if proposal.region_id != region_id {
                return Err(Error::LogRegionMismatch {
                    record_region_id: region_id,
                    proposal_region_id: proposal.region_id,
                });
            }
            PersistedEntry {
                version: ENTRY_CODEC_VERSION,
                payload_kind: PAYLOAD_NORMAL,
                normal_payload: proposal.payload_bytes().to_vec(),
                membership: None,
                leader_node_id: entry.log_id.leader_id.node_id,
                normal_kind: encode_normal_kind(proposal.payload_kind()),
            }
        }
        EntryPayload::Membership(membership) => PersistedEntry {
            version: ENTRY_CODEC_VERSION,
            payload_kind: PAYLOAD_MEMBERSHIP,
            normal_payload: Vec::new(),
            membership: Some(encode_membership(membership)),
            leader_node_id: entry.log_id.leader_id.node_id,
            normal_kind: 0,
        },
    };

    let mut payload = Vec::with_capacity(persisted.encoded_len());
    persisted.encode(&mut payload)?;
    Ok(LogEntry {
        region_id,
        index: entry.log_id.index,
        term: entry.log_id.leader_id.term,
        payload,
    })
}

pub fn decode_log_entry(record: &LogEntry) -> Result<OpenRaftEntry, Error> {
    let persisted = PersistedEntry::decode(record.payload.as_slice())?;
    if persisted.version != ENTRY_CODEC_VERSION {
        return Err(Error::InvalidLogPayload(format!(
            "unsupported raft log codec version {}",
            persisted.version
        )));
    }
    let payload = match persisted.payload_kind {
        PAYLOAD_BLANK => EntryPayload::Blank,
        PAYLOAD_NORMAL => {
            let proposal_payload = match persisted.normal_kind {
                0 | NORMAL_RAFT_COMMAND => ProposalPayload::RaftCommand(persisted.normal_payload),
                NORMAL_REGION_DESCRIPTOR => {
                    ProposalPayload::RegionDescriptor(persisted.normal_payload)
                }
                NORMAL_ADMIN_COMMAND => ProposalPayload::AdminCommand(persisted.normal_payload),
                NORMAL_METADATA_COMMAND => {
                    ProposalPayload::MetadataCommand(persisted.normal_payload)
                }
                other => {
                    return Err(Error::InvalidLogPayload(format!(
                        "unsupported normal proposal kind {other}",
                    )))
                }
            };
            let proposal = Proposal {
                region_id: record.region_id,
                payload: proposal_payload,
            };
            match proposal.payload_kind() {
                ProposalPayloadKind::RaftCommand => {
                    proposal.decode_raft_command()?;
                }
                ProposalPayloadKind::MetadataCommand => {
                    proposal.decode_metadata_command()?;
                }
                ProposalPayloadKind::RegionDescriptor => {
                    proposal.decode_region_descriptor()?;
                }
                ProposalPayloadKind::AdminCommand => {
                    proposal.decode_admin_command()?;
                }
            }
            EntryPayload::Normal(proposal)
        }
        PAYLOAD_MEMBERSHIP => {
            let membership = persisted.membership.ok_or_else(|| {
                Error::InvalidLogPayload("membership entry missing membership payload".to_owned())
            })?;
            EntryPayload::Membership(decode_membership(membership)?)
        }
        other => {
            return Err(Error::InvalidLogPayload(format!(
                "unsupported raft log payload kind {other}",
            )))
        }
    };

    Ok(OpenRaftEntry {
        log_id: LogId::new(
            CommittedLeaderId::new(record.term, persisted.leader_node_id),
            record.index,
        ),
        payload,
    })
}

fn encode_normal_kind(kind: ProposalPayloadKind) -> u32 {
    match kind {
        ProposalPayloadKind::RaftCommand => NORMAL_RAFT_COMMAND,
        ProposalPayloadKind::MetadataCommand => NORMAL_METADATA_COMMAND,
        ProposalPayloadKind::RegionDescriptor => NORMAL_REGION_DESCRIPTOR,
        ProposalPayloadKind::AdminCommand => NORMAL_ADMIN_COMMAND,
    }
}

fn encode_membership(membership: &Membership<NodeId, BasicNode>) -> PersistedMembership {
    let configs = membership
        .get_joint_config()
        .iter()
        .map(|config| PersistedMembershipConfig {
            voters: config.iter().copied().collect(),
        })
        .collect();
    let nodes = membership
        .nodes()
        .map(|(id, node)| PersistedNode {
            id: *id,
            addr: node.addr.clone(),
        })
        .collect();
    PersistedMembership { configs, nodes }
}

fn decode_membership(
    persisted: PersistedMembership,
) -> Result<Membership<NodeId, BasicNode>, Error> {
    if persisted.configs.is_empty() {
        return Err(Error::InvalidLogPayload(
            "membership entry has no voter config".to_owned(),
        ));
    }
    let configs = persisted
        .configs
        .into_iter()
        .map(|config| {
            if config.voters.is_empty() {
                return Err(Error::InvalidLogPayload(
                    "membership voter config is empty".to_owned(),
                ));
            }
            Ok(config.voters.into_iter().collect::<BTreeSet<_>>())
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let nodes = persisted
        .nodes
        .into_iter()
        .map(|node| (node.id, BasicNode::new(node.addr)))
        .collect::<BTreeMap<_, _>>();
    Ok(Membership::new(configs, nodes))
}

pub(crate) fn encode_membership_payload(
    membership: &Membership<NodeId, BasicNode>,
) -> Result<Vec<u8>, Error> {
    let persisted = encode_membership(membership);
    let mut payload = Vec::with_capacity(persisted.encoded_len());
    persisted.encode(&mut payload)?;
    Ok(payload)
}

pub(crate) fn decode_membership_payload(
    payload: &[u8],
) -> Result<Membership<NodeId, BasicNode>, Error> {
    decode_membership(PersistedMembership::decode(payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RaftStoreConfig;
    use nokv_proto::nokv::kv::v1 as kvpb;
    use nokv_proto::nokv::meta::v1 as metapb;
    use nokv_proto::nokv::raft::v1 as raftpb;
    use nokv_raftlog::SegmentedRaftLog;

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn get_command(region_id: RegionId) -> raftpb::RaftCmdRequest {
        raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id,
                request_id: 44,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdGet as i32,
                cmd: Some(raftpb::request::Cmd::Get(kvpb::GetRequest {
                    key: b"k".to_vec(),
                    version: 9,
                })),
            }],
        }
    }

    #[test]
    fn normal_entry_round_trips_through_raftlog_record() {
        let proposal = Proposal::from_raft_command(&get_command(7)).unwrap();
        let entry = OpenRaftEntry {
            log_id: log_id(3, 11),
            payload: EntryPayload::Normal(proposal.clone()),
        };

        let record = encode_log_entry(7, &entry).unwrap();
        assert_eq!(record.region_id, 7);
        assert_eq!(record.index, 11);
        assert_eq!(record.term, 3);

        let decoded = decode_log_entry(&record).unwrap();
        assert_eq!(decoded.log_id.index, 11);
        assert_eq!(decoded.log_id.leader_id.term, 3);
        assert_eq!(decoded.log_id.leader_id.node_id, 1);
        assert_eq!(
            decoded.payload,
            EntryPayload::<RaftStoreConfig>::Normal(proposal)
        );
    }

    #[test]
    fn region_descriptor_entry_round_trips_through_raftlog_record() {
        let descriptor = metapb::RegionDescriptor {
            region_id: 7,
            epoch: Some(metapb::RegionEpoch {
                version: 1,
                conf_version: 2,
            }),
            peers: vec![
                metapb::RegionPeer {
                    store_id: 1,
                    peer_id: 1,
                },
                metapb::RegionPeer {
                    store_id: 2,
                    peer_id: 2,
                },
            ],
            ..Default::default()
        };
        let proposal = Proposal::from_region_descriptor(&descriptor).unwrap();
        let entry = OpenRaftEntry {
            log_id: log_id(3, 12),
            payload: EntryPayload::Normal(proposal.clone()),
        };

        let decoded = decode_log_entry(&encode_log_entry(7, &entry).unwrap()).unwrap();
        assert_eq!(decoded.log_id.index, 12);
        assert_eq!(
            decoded.payload,
            EntryPayload::<RaftStoreConfig>::Normal(proposal)
        );
    }

    #[test]
    fn admin_command_entry_round_trips_through_raftlog_record() {
        let command = raftpb::AdminCommand {
            r#type: raftpb::admin_command::Type::Split as i32,
            split: Some(raftpb::SplitCommand {
                parent_region_id: 7,
                split_key: b"m".to_vec(),
                child: Some(metapb::RegionDescriptor {
                    region_id: 8,
                    start_key: b"m".to_vec(),
                    peers: vec![metapb::RegionPeer {
                        store_id: 1,
                        peer_id: 8,
                    }],
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };
        let proposal = Proposal::from_admin_command(7, &command).unwrap();
        let entry = OpenRaftEntry {
            log_id: log_id(3, 13),
            payload: EntryPayload::Normal(proposal.clone()),
        };

        let decoded = decode_log_entry(&encode_log_entry(7, &entry).unwrap()).unwrap();

        assert_eq!(decoded.log_id.index, 13);
        assert_eq!(
            decoded.payload,
            EntryPayload::<RaftStoreConfig>::Normal(proposal)
        );
    }

    #[test]
    fn normal_entry_round_trips_through_segmented_raftlog() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
        let proposal = Proposal::from_raft_command(&get_command(7)).unwrap();
        let entry = OpenRaftEntry {
            log_id: log_id(3, 11),
            payload: EntryPayload::Normal(proposal.clone()),
        };

        log.append(&[encode_log_entry(7, &entry).unwrap()]).unwrap();
        log.sync().unwrap();
        let recovered = log.recover().unwrap();

        assert_eq!(recovered.len(), 1);
        let decoded = decode_log_entry(&recovered[0]).unwrap();
        assert_eq!(
            decoded.payload,
            EntryPayload::<RaftStoreConfig>::Normal(proposal)
        );
    }

    #[test]
    fn blank_entry_round_trips_through_raftlog_record() {
        let entry = OpenRaftEntry {
            log_id: log_id(4, 1),
            payload: EntryPayload::Blank,
        };

        let decoded = decode_log_entry(&encode_log_entry(9, &entry).unwrap()).unwrap();
        assert_eq!(decoded.log_id.index, 1);
        assert_eq!(decoded.payload, EntryPayload::<RaftStoreConfig>::Blank);
    }

    #[test]
    fn membership_entry_round_trips_through_raftlog_record() {
        let configs = vec![BTreeSet::from([1, 2]), BTreeSet::from([2, 3])];
        let nodes = BTreeMap::from([
            (1, BasicNode::new("store-1")),
            (2, BasicNode::new("store-2")),
            (3, BasicNode::new("store-3")),
            (4, BasicNode::new("learner-4")),
        ]);
        let membership = Membership::new(configs, nodes);
        let entry = OpenRaftEntry {
            log_id: log_id(5, 17),
            payload: EntryPayload::Membership(membership.clone()),
        };

        let decoded = decode_log_entry(&encode_log_entry(12, &entry).unwrap()).unwrap();
        assert_eq!(
            decoded.payload,
            EntryPayload::<RaftStoreConfig>::Membership(membership)
        );
    }

    #[test]
    fn normal_entry_rejects_record_region_mismatch() {
        let proposal = Proposal::from_raft_command(&get_command(7)).unwrap();
        let entry = OpenRaftEntry {
            log_id: log_id(3, 11),
            payload: EntryPayload::Normal(proposal),
        };

        let err = encode_log_entry(8, &entry).unwrap_err();
        assert!(matches!(err, Error::LogRegionMismatch { .. }));
    }

    #[test]
    fn normal_entry_decode_rejects_command_header_region_mismatch() {
        let proposal = Proposal::from_raft_command(&get_command(7)).unwrap();
        let entry = OpenRaftEntry {
            log_id: log_id(3, 11),
            payload: EntryPayload::Normal(proposal),
        };
        let mut record = encode_log_entry(7, &entry).unwrap();
        record.region_id = 8;

        let err = decode_log_entry(&record).unwrap_err();
        assert!(matches!(err, Error::RegionMismatch { .. }));
    }
}
