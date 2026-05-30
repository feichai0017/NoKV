use nokv_raftlog::LogEntry;
use openraft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    BasicNode, CommittedLeaderId, LogId, SnapshotMeta, StoredMembership, Vote,
};
use prost::Message;

use crate::log_codec::{
    decode_log_entry, decode_membership_payload, encode_log_entry, encode_membership_payload,
};
use crate::{Error, NodeId, RaftStoreConfig, RegionId};

const TRANSPORT_CODEC_VERSION: u32 = 1;

const APPEND_RESPONSE_SUCCESS: u32 = 1;
const APPEND_RESPONSE_PARTIAL_SUCCESS: u32 = 2;
const APPEND_RESPONSE_CONFLICT: u32 = 3;
const APPEND_RESPONSE_HIGHER_VOTE: u32 = 4;

#[derive(Clone, PartialEq, Message)]
struct PersistedVote {
    #[prost(uint64, tag = "1")]
    term: u64,
    #[prost(uint64, tag = "2")]
    node_id: NodeId,
    #[prost(bool, tag = "3")]
    committed: bool,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedLogId {
    #[prost(uint64, tag = "1")]
    term: u64,
    #[prost(uint64, tag = "2")]
    node_id: NodeId,
    #[prost(uint64, tag = "3")]
    index: u64,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedTransportLogEntry {
    #[prost(uint64, tag = "1")]
    region_id: RegionId,
    #[prost(uint64, tag = "2")]
    index: u64,
    #[prost(uint64, tag = "3")]
    term: u64,
    #[prost(bytes, tag = "4")]
    payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedStoredMembership {
    #[prost(message, optional, tag = "1")]
    log_id: Option<PersistedLogId>,
    #[prost(bytes, tag = "2")]
    membership_payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedSnapshotMeta {
    #[prost(message, optional, tag = "1")]
    last_log_id: Option<PersistedLogId>,
    #[prost(message, optional, tag = "2")]
    last_membership: Option<PersistedStoredMembership>,
    #[prost(string, tag = "3")]
    snapshot_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedAppendEntriesRequest {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    region_id: RegionId,
    #[prost(message, optional, tag = "3")]
    vote: Option<PersistedVote>,
    #[prost(message, optional, tag = "4")]
    prev_log_id: Option<PersistedLogId>,
    #[prost(message, repeated, tag = "5")]
    entries: Vec<PersistedTransportLogEntry>,
    #[prost(message, optional, tag = "6")]
    leader_commit: Option<PersistedLogId>,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedAppendEntriesResponse {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint32, tag = "2")]
    response_kind: u32,
    #[prost(message, optional, tag = "3")]
    partial_match: Option<PersistedLogId>,
    #[prost(message, optional, tag = "4")]
    higher_vote: Option<PersistedVote>,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedVoteRequest {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(message, optional, tag = "2")]
    vote: Option<PersistedVote>,
    #[prost(message, optional, tag = "3")]
    last_log_id: Option<PersistedLogId>,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedVoteResponse {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(message, optional, tag = "2")]
    vote: Option<PersistedVote>,
    #[prost(bool, tag = "3")]
    vote_granted: bool,
    #[prost(message, optional, tag = "4")]
    last_log_id: Option<PersistedLogId>,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedInstallSnapshotRequest {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    region_id: RegionId,
    #[prost(message, optional, tag = "3")]
    vote: Option<PersistedVote>,
    #[prost(message, optional, tag = "4")]
    meta: Option<PersistedSnapshotMeta>,
    #[prost(uint64, tag = "5")]
    offset: u64,
    #[prost(bytes, tag = "6")]
    data: Vec<u8>,
    #[prost(bool, tag = "7")]
    done: bool,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedInstallSnapshotResponse {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(message, optional, tag = "2")]
    vote: Option<PersistedVote>,
}

pub fn encode_append_entries_request(
    region_id: RegionId,
    request: &AppendEntriesRequest<RaftStoreConfig>,
) -> Result<Vec<u8>, Error> {
    let persisted = PersistedAppendEntriesRequest {
        version: TRANSPORT_CODEC_VERSION,
        region_id,
        vote: Some(encode_vote(request.vote)),
        prev_log_id: request.prev_log_id.map(encode_log_id),
        entries: request
            .entries
            .iter()
            .map(|entry| encode_transport_log_entry(region_id, entry))
            .collect::<Result<Vec<_>, _>>()?,
        leader_commit: request.leader_commit.map(encode_log_id),
    };
    encode_message(&persisted)
}

pub fn decode_append_entries_request(
    payload: &[u8],
) -> Result<(RegionId, AppendEntriesRequest<RaftStoreConfig>), Error> {
    let persisted = PersistedAppendEntriesRequest::decode(payload)?;
    check_version("append entries request", persisted.version)?;
    let region_id = persisted.region_id;
    let entries = persisted
        .entries
        .into_iter()
        .map(|entry| decode_transport_log_entry(region_id, entry))
        .collect::<Result<Vec<_>, _>>()?;
    let request = AppendEntriesRequest {
        vote: decode_required_vote(persisted.vote, "append entries request")?,
        prev_log_id: persisted.prev_log_id.map(decode_log_id),
        entries,
        leader_commit: persisted.leader_commit.map(decode_log_id),
    };
    Ok((region_id, request))
}

pub fn encode_append_entries_response(
    response: &AppendEntriesResponse<NodeId>,
) -> Result<Vec<u8>, Error> {
    let persisted = match response {
        AppendEntriesResponse::Success => PersistedAppendEntriesResponse {
            version: TRANSPORT_CODEC_VERSION,
            response_kind: APPEND_RESPONSE_SUCCESS,
            partial_match: None,
            higher_vote: None,
        },
        AppendEntriesResponse::PartialSuccess(log_id) => PersistedAppendEntriesResponse {
            version: TRANSPORT_CODEC_VERSION,
            response_kind: APPEND_RESPONSE_PARTIAL_SUCCESS,
            partial_match: log_id.map(encode_log_id),
            higher_vote: None,
        },
        AppendEntriesResponse::Conflict => PersistedAppendEntriesResponse {
            version: TRANSPORT_CODEC_VERSION,
            response_kind: APPEND_RESPONSE_CONFLICT,
            partial_match: None,
            higher_vote: None,
        },
        AppendEntriesResponse::HigherVote(vote) => PersistedAppendEntriesResponse {
            version: TRANSPORT_CODEC_VERSION,
            response_kind: APPEND_RESPONSE_HIGHER_VOTE,
            partial_match: None,
            higher_vote: Some(encode_vote(*vote)),
        },
    };
    encode_message(&persisted)
}

pub fn decode_append_entries_response(
    payload: &[u8],
) -> Result<AppendEntriesResponse<NodeId>, Error> {
    let persisted = PersistedAppendEntriesResponse::decode(payload)?;
    check_version("append entries response", persisted.version)?;
    match persisted.response_kind {
        APPEND_RESPONSE_SUCCESS => Ok(AppendEntriesResponse::Success),
        APPEND_RESPONSE_PARTIAL_SUCCESS => Ok(AppendEntriesResponse::PartialSuccess(
            persisted.partial_match.map(decode_log_id),
        )),
        APPEND_RESPONSE_CONFLICT => Ok(AppendEntriesResponse::Conflict),
        APPEND_RESPONSE_HIGHER_VOTE => Ok(AppendEntriesResponse::HigherVote(decode_required_vote(
            persisted.higher_vote,
            "append entries higher vote",
        )?)),
        other => Err(Error::InvalidTransportPayload(format!(
            "unsupported append entries response kind {other}",
        ))),
    }
}

pub fn encode_vote_request(request: &VoteRequest<NodeId>) -> Result<Vec<u8>, Error> {
    let persisted = PersistedVoteRequest {
        version: TRANSPORT_CODEC_VERSION,
        vote: Some(encode_vote(request.vote)),
        last_log_id: request.last_log_id.map(encode_log_id),
    };
    encode_message(&persisted)
}

pub fn decode_vote_request(payload: &[u8]) -> Result<VoteRequest<NodeId>, Error> {
    let persisted = PersistedVoteRequest::decode(payload)?;
    check_version("vote request", persisted.version)?;
    Ok(VoteRequest {
        vote: decode_required_vote(persisted.vote, "vote request")?,
        last_log_id: persisted.last_log_id.map(decode_log_id),
    })
}

pub fn encode_vote_response(response: &VoteResponse<NodeId>) -> Result<Vec<u8>, Error> {
    let persisted = PersistedVoteResponse {
        version: TRANSPORT_CODEC_VERSION,
        vote: Some(encode_vote(response.vote)),
        vote_granted: response.vote_granted,
        last_log_id: response.last_log_id.map(encode_log_id),
    };
    encode_message(&persisted)
}

pub fn decode_vote_response(payload: &[u8]) -> Result<VoteResponse<NodeId>, Error> {
    let persisted = PersistedVoteResponse::decode(payload)?;
    check_version("vote response", persisted.version)?;
    Ok(VoteResponse {
        vote: decode_required_vote(persisted.vote, "vote response")?,
        vote_granted: persisted.vote_granted,
        last_log_id: persisted.last_log_id.map(decode_log_id),
    })
}

pub fn encode_install_snapshot_request(
    region_id: RegionId,
    request: &InstallSnapshotRequest<RaftStoreConfig>,
) -> Result<Vec<u8>, Error> {
    let persisted = PersistedInstallSnapshotRequest {
        version: TRANSPORT_CODEC_VERSION,
        region_id,
        vote: Some(encode_vote(request.vote)),
        meta: Some(encode_snapshot_meta(&request.meta)?),
        offset: request.offset,
        data: request.data.clone(),
        done: request.done,
    };
    encode_message(&persisted)
}

pub fn decode_install_snapshot_request(
    payload: &[u8],
) -> Result<(RegionId, InstallSnapshotRequest<RaftStoreConfig>), Error> {
    let persisted = PersistedInstallSnapshotRequest::decode(payload)?;
    check_version("install snapshot request", persisted.version)?;
    let region_id = persisted.region_id;
    let request = InstallSnapshotRequest {
        vote: decode_required_vote(persisted.vote, "install snapshot request")?,
        meta: decode_required_snapshot_meta(persisted.meta)?,
        offset: persisted.offset,
        data: persisted.data,
        done: persisted.done,
    };
    Ok((region_id, request))
}

pub fn encode_install_snapshot_response(
    response: &InstallSnapshotResponse<NodeId>,
) -> Result<Vec<u8>, Error> {
    let persisted = PersistedInstallSnapshotResponse {
        version: TRANSPORT_CODEC_VERSION,
        vote: Some(encode_vote(response.vote)),
    };
    encode_message(&persisted)
}

pub fn decode_install_snapshot_response(
    payload: &[u8],
) -> Result<InstallSnapshotResponse<NodeId>, Error> {
    let persisted = PersistedInstallSnapshotResponse::decode(payload)?;
    check_version("install snapshot response", persisted.version)?;
    Ok(InstallSnapshotResponse {
        vote: decode_required_vote(persisted.vote, "install snapshot response")?,
    })
}

fn encode_transport_log_entry(
    region_id: RegionId,
    entry: &crate::OpenRaftEntry,
) -> Result<PersistedTransportLogEntry, Error> {
    let record = encode_log_entry(region_id, entry)?;
    Ok(PersistedTransportLogEntry {
        region_id: record.region_id,
        index: record.index,
        term: record.term,
        payload: record.payload,
    })
}

fn decode_transport_log_entry(
    region_id: RegionId,
    entry: PersistedTransportLogEntry,
) -> Result<crate::OpenRaftEntry, Error> {
    if entry.region_id != region_id {
        return Err(Error::InvalidTransportPayload(format!(
            "transport log entry region {} does not match envelope region {}",
            entry.region_id, region_id
        )));
    }
    decode_log_entry(&LogEntry {
        region_id: entry.region_id,
        index: entry.index,
        term: entry.term,
        payload: entry.payload,
    })
}

fn encode_snapshot_meta(
    meta: &SnapshotMeta<NodeId, BasicNode>,
) -> Result<PersistedSnapshotMeta, Error> {
    Ok(PersistedSnapshotMeta {
        last_log_id: meta.last_log_id.map(encode_log_id),
        last_membership: Some(encode_stored_membership(&meta.last_membership)?),
        snapshot_id: meta.snapshot_id.clone(),
    })
}

fn decode_required_snapshot_meta(
    meta: Option<PersistedSnapshotMeta>,
) -> Result<SnapshotMeta<NodeId, BasicNode>, Error> {
    let meta = meta.ok_or_else(|| {
        Error::InvalidTransportPayload("install snapshot request missing snapshot meta".to_owned())
    })?;
    let last_membership = meta.last_membership.ok_or_else(|| {
        Error::InvalidTransportPayload("snapshot meta missing stored membership".to_owned())
    })?;
    Ok(SnapshotMeta {
        last_log_id: meta.last_log_id.map(decode_log_id),
        last_membership: decode_stored_membership(last_membership)?,
        snapshot_id: meta.snapshot_id,
    })
}

fn encode_stored_membership(
    membership: &StoredMembership<NodeId, BasicNode>,
) -> Result<PersistedStoredMembership, Error> {
    Ok(PersistedStoredMembership {
        log_id: membership.log_id().map(encode_log_id),
        membership_payload: encode_membership_payload(membership.membership())?,
    })
}

fn decode_stored_membership(
    membership: PersistedStoredMembership,
) -> Result<StoredMembership<NodeId, BasicNode>, Error> {
    Ok(StoredMembership::new(
        membership.log_id.map(decode_log_id),
        decode_membership_payload(&membership.membership_payload)?,
    ))
}

fn encode_vote(vote: Vote<NodeId>) -> PersistedVote {
    PersistedVote {
        term: vote.leader_id.term,
        node_id: vote.leader_id.node_id,
        committed: vote.committed,
    }
}

fn decode_required_vote(vote: Option<PersistedVote>, owner: &str) -> Result<Vote<NodeId>, Error> {
    vote.map(decode_vote)
        .ok_or_else(|| Error::InvalidTransportPayload(format!("{owner} missing vote")))
}

fn decode_vote(vote: PersistedVote) -> Vote<NodeId> {
    let mut decoded = Vote::new(vote.term, vote.node_id);
    if vote.committed {
        decoded.commit();
    }
    decoded
}

fn encode_log_id(log_id: LogId<NodeId>) -> PersistedLogId {
    PersistedLogId {
        term: log_id.leader_id.term,
        node_id: log_id.leader_id.node_id,
        index: log_id.index,
    }
}

fn decode_log_id(log_id: PersistedLogId) -> LogId<NodeId> {
    LogId::new(
        CommittedLeaderId::new(log_id.term, log_id.node_id),
        log_id.index,
    )
}

fn check_version(owner: &str, version: u32) -> Result<(), Error> {
    if version != TRANSPORT_CODEC_VERSION {
        return Err(Error::InvalidTransportPayload(format!(
            "{owner} has unsupported codec version {version}",
        )));
    }
    Ok(())
}

fn encode_message<M>(message: &M) -> Result<Vec<u8>, Error>
where
    M: Message,
{
    let mut payload = Vec::with_capacity(message.encoded_len());
    message.encode(&mut payload)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use nokv_proto::nokv::kv::v1 as kvpb;
    use nokv_proto::nokv::raft::v1 as raftpb;
    use openraft::{EntryPayload, LeaderId, Membership};

    use super::*;
    use crate::{OpenRaftEntry, Proposal};

    fn log_id(term: u64, node_id: NodeId, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, node_id), index)
    }

    fn get_command(region_id: RegionId) -> raftpb::RaftCmdRequest {
        raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id,
                request_id: 77,
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

    fn membership() -> Membership<NodeId, BasicNode> {
        Membership::new(
            vec![BTreeSet::from([1, 2]), BTreeSet::from([2, 3])],
            BTreeMap::from([
                (1, BasicNode::new("store-1")),
                (2, BasicNode::new("store-2")),
                (3, BasicNode::new("store-3")),
            ]),
        )
    }

    #[test]
    fn append_entries_request_round_trips_entries_and_membership() {
        let proposal = Proposal::from_raft_command(&get_command(9)).unwrap();
        let request = AppendEntriesRequest::<RaftStoreConfig> {
            vote: Vote::new_committed(7, 1),
            prev_log_id: Some(log_id(6, 1, 41)),
            entries: vec![
                OpenRaftEntry {
                    log_id: log_id(7, 1, 42),
                    payload: EntryPayload::Normal(proposal.clone()),
                },
                OpenRaftEntry {
                    log_id: log_id(7, 1, 43),
                    payload: EntryPayload::Membership(membership()),
                },
            ],
            leader_commit: Some(log_id(7, 1, 42)),
        };

        let payload = encode_append_entries_request(9, &request).unwrap();
        let (region_id, decoded) = decode_append_entries_request(&payload).unwrap();

        assert_eq!(region_id, 9);
        assert_eq!(decoded.vote, request.vote);
        assert_eq!(decoded.prev_log_id, request.prev_log_id);
        assert_eq!(decoded.entries, request.entries);
        assert_eq!(decoded.leader_commit, request.leader_commit);
    }

    #[test]
    fn append_entries_response_round_trips_all_variants() {
        let responses = [
            AppendEntriesResponse::Success,
            AppendEntriesResponse::PartialSuccess(Some(log_id(2, 1, 8))),
            AppendEntriesResponse::PartialSuccess(None),
            AppendEntriesResponse::Conflict,
            AppendEntriesResponse::HigherVote(Vote::new(9, 3)),
        ];

        for response in responses {
            let payload = encode_append_entries_response(&response).unwrap();
            let decoded = decode_append_entries_response(&payload).unwrap();
            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn vote_request_and_response_round_trip() {
        let request = VoteRequest::new(Vote::new(3, 2), Some(log_id(2, 1, 11)));
        let response = VoteResponse::new(&Vote::new_committed(3, 2), Some(log_id(3, 2, 12)), true);

        assert_eq!(
            decode_vote_request(&encode_vote_request(&request).unwrap()).unwrap(),
            request
        );
        assert_eq!(
            decode_vote_response(&encode_vote_response(&response).unwrap()).unwrap(),
            response
        );
    }

    #[test]
    fn install_snapshot_request_round_trips_payload_and_membership() {
        let request = InstallSnapshotRequest::<RaftStoreConfig> {
            vote: Vote::new_committed(5, 1),
            meta: SnapshotMeta {
                last_log_id: Some(log_id(5, 1, 99)),
                last_membership: StoredMembership::new(Some(log_id(4, 1, 88)), membership()),
                snapshot_id: "region-9-snapshot-99".to_owned(),
            },
            offset: 4096,
            data: b"snapshot-bytes".to_vec(),
            done: true,
        };

        let payload = encode_install_snapshot_request(9, &request).unwrap();
        let (region_id, decoded) = decode_install_snapshot_request(&payload).unwrap();

        assert_eq!(region_id, 9);
        assert_eq!(decoded, request);
        assert_eq!(
            decode_install_snapshot_response(
                &encode_install_snapshot_response(&InstallSnapshotResponse {
                    vote: Vote::new(6, 2)
                })
                .unwrap()
            )
            .unwrap(),
            InstallSnapshotResponse {
                vote: Vote::new(6, 2)
            }
        );
    }

    #[test]
    fn append_entries_request_rejects_entry_region_mismatch() {
        let proposal = Proposal::from_raft_command(&get_command(7)).unwrap();
        let request = AppendEntriesRequest::<RaftStoreConfig> {
            vote: Vote::new_committed(7, 1),
            prev_log_id: None,
            entries: vec![OpenRaftEntry {
                log_id: log_id(7, 1, 42),
                payload: EntryPayload::Normal(proposal),
            }],
            leader_commit: None,
        };

        let err = encode_append_entries_request(8, &request).unwrap_err();
        assert!(matches!(err, Error::LogRegionMismatch { .. }));
    }

    #[test]
    fn append_entries_request_rejects_envelope_entry_region_mismatch() {
        let proposal = Proposal::from_raft_command(&get_command(7)).unwrap();
        let request = AppendEntriesRequest::<RaftStoreConfig> {
            vote: Vote::new_committed(7, 1),
            prev_log_id: None,
            entries: vec![OpenRaftEntry {
                log_id: log_id(7, 1, 42),
                payload: EntryPayload::Normal(proposal),
            }],
            leader_commit: None,
        };
        let mut persisted = PersistedAppendEntriesRequest::decode(
            encode_append_entries_request(7, &request)
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        persisted.entries[0].region_id = 8;
        let payload = encode_message(&persisted).unwrap();

        let err = decode_append_entries_request(&payload).unwrap_err();
        assert!(matches!(err, Error::InvalidTransportPayload(_)));
    }

    #[test]
    fn codec_rejects_unknown_version() {
        let persisted = PersistedVoteRequest {
            version: TRANSPORT_CODEC_VERSION + 1,
            vote: Some(PersistedVote {
                term: 1,
                node_id: 1,
                committed: false,
            }),
            last_log_id: None,
        };

        let err = decode_vote_request(&encode_message(&persisted).unwrap()).unwrap_err();
        assert!(matches!(err, Error::InvalidTransportPayload(_)));
    }

    #[test]
    fn vote_round_trip_preserves_uncommitted_leader_id() {
        let vote = Vote {
            leader_id: LeaderId::new(11, 4),
            committed: false,
        };

        assert_eq!(decode_vote(encode_vote(vote)), vote);
    }
}
