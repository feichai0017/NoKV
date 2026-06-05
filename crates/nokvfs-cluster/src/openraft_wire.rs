//! Conversion between storage-neutral wire DTOs and OpenRaft metadata RPCs.
//!
//! `nokvfs-protocol` owns the DTO shape. This module owns the only OpenRaft
//! conversion layer so OpenRaft types do not leak into server/client crates.

use nokvfs_protocol::{
    WireMetadataRaftAppendEntriesRequest, WireMetadataRaftAppendEntriesResponse,
    WireMetadataRaftEntry, WireMetadataRaftInstallSnapshotRequest,
    WireMetadataRaftInstallSnapshotResponse, WireMetadataRaftLeaderId, WireMetadataRaftLogId,
    WireMetadataRaftMembership, WireMetadataRaftNode, WireMetadataRaftSnapshotMeta,
    WireMetadataRaftVote, WireMetadataRaftVoteRequest, WireMetadataRaftVoteResponse,
};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, LeaderId, LogId, Membership, SnapshotMeta, StoredMembership, Vote};
use std::collections::{BTreeMap, BTreeSet};

use crate::openraft_file_log::{decode_metadata_raft_entry, encode_metadata_raft_entry};
use crate::openraft_log::{MetadataRaftConfig, MetadataRaftEntry};
use crate::SharedLogError;

pub fn wire_vote_request(request: &VoteRequest<u64>) -> WireMetadataRaftVoteRequest {
    WireMetadataRaftVoteRequest {
        vote: wire_vote(request.vote),
        last_log_id: request.last_log_id.map(wire_log_id),
    }
}

pub fn vote_request(
    request: WireMetadataRaftVoteRequest,
) -> Result<VoteRequest<u64>, SharedLogError> {
    Ok(VoteRequest {
        vote: vote(request.vote)?,
        last_log_id: request.last_log_id.map(log_id).transpose()?,
    })
}

pub fn wire_vote_response(response: &VoteResponse<u64>) -> WireMetadataRaftVoteResponse {
    WireMetadataRaftVoteResponse {
        vote: wire_vote(response.vote),
        vote_granted: response.vote_granted,
        last_log_id: response.last_log_id.map(wire_log_id),
    }
}

pub fn vote_response(
    response: WireMetadataRaftVoteResponse,
) -> Result<VoteResponse<u64>, SharedLogError> {
    Ok(VoteResponse {
        vote: vote(response.vote)?,
        vote_granted: response.vote_granted,
        last_log_id: response.last_log_id.map(log_id).transpose()?,
    })
}

pub fn wire_append_entries_request(
    request: &AppendEntriesRequest<MetadataRaftConfig>,
) -> Result<WireMetadataRaftAppendEntriesRequest, SharedLogError> {
    Ok(WireMetadataRaftAppendEntriesRequest {
        vote: wire_vote(request.vote),
        prev_log_id: request.prev_log_id.map(wire_log_id),
        entries: request
            .entries
            .iter()
            .map(wire_entry)
            .collect::<Result<Vec<_>, _>>()?,
        leader_commit: request.leader_commit.map(wire_log_id),
    })
}

pub fn append_entries_request(
    request: WireMetadataRaftAppendEntriesRequest,
) -> Result<AppendEntriesRequest<MetadataRaftConfig>, SharedLogError> {
    Ok(AppendEntriesRequest {
        vote: vote(request.vote)?,
        prev_log_id: request.prev_log_id.map(log_id).transpose()?,
        entries: request
            .entries
            .into_iter()
            .map(entry)
            .collect::<Result<Vec<_>, _>>()?,
        leader_commit: request.leader_commit.map(log_id).transpose()?,
    })
}

pub fn wire_append_entries_response(
    response: &AppendEntriesResponse<u64>,
) -> WireMetadataRaftAppendEntriesResponse {
    match response {
        AppendEntriesResponse::Success => WireMetadataRaftAppendEntriesResponse::Success,
        AppendEntriesResponse::PartialSuccess(matching) => {
            WireMetadataRaftAppendEntriesResponse::PartialSuccess {
                matching: matching.map(wire_log_id),
            }
        }
        AppendEntriesResponse::Conflict => WireMetadataRaftAppendEntriesResponse::Conflict,
        AppendEntriesResponse::HigherVote(vote) => {
            WireMetadataRaftAppendEntriesResponse::HigherVote {
                vote: wire_vote(*vote),
            }
        }
    }
}

pub fn append_entries_response(
    response: WireMetadataRaftAppendEntriesResponse,
) -> Result<AppendEntriesResponse<u64>, SharedLogError> {
    Ok(match response {
        WireMetadataRaftAppendEntriesResponse::Success => AppendEntriesResponse::Success,
        WireMetadataRaftAppendEntriesResponse::PartialSuccess { matching } => {
            AppendEntriesResponse::PartialSuccess(matching.map(log_id).transpose()?)
        }
        WireMetadataRaftAppendEntriesResponse::Conflict => AppendEntriesResponse::Conflict,
        WireMetadataRaftAppendEntriesResponse::HigherVote { vote } => {
            AppendEntriesResponse::HigherVote(self::vote(vote)?)
        }
    })
}

pub fn wire_install_snapshot_request(
    request: &InstallSnapshotRequest<MetadataRaftConfig>,
) -> WireMetadataRaftInstallSnapshotRequest {
    WireMetadataRaftInstallSnapshotRequest {
        vote: wire_vote(request.vote),
        meta: wire_snapshot_meta(&request.meta),
        offset: request.offset,
        data: request.data.clone(),
        done: request.done,
    }
}

pub fn install_snapshot_request(
    request: WireMetadataRaftInstallSnapshotRequest,
) -> Result<InstallSnapshotRequest<MetadataRaftConfig>, SharedLogError> {
    Ok(InstallSnapshotRequest {
        vote: vote(request.vote)?,
        meta: snapshot_meta(request.meta)?,
        offset: request.offset,
        data: request.data,
        done: request.done,
    })
}

pub fn wire_install_snapshot_response(
    response: &InstallSnapshotResponse<u64>,
) -> WireMetadataRaftInstallSnapshotResponse {
    WireMetadataRaftInstallSnapshotResponse {
        vote: wire_vote(response.vote),
    }
}

pub fn install_snapshot_response(
    response: WireMetadataRaftInstallSnapshotResponse,
) -> Result<InstallSnapshotResponse<u64>, SharedLogError> {
    Ok(InstallSnapshotResponse {
        vote: vote(response.vote)?,
    })
}

fn wire_entry(entry: &MetadataRaftEntry) -> Result<WireMetadataRaftEntry, SharedLogError> {
    Ok(WireMetadataRaftEntry {
        log_id: wire_log_id(entry.log_id),
        payload: encode_metadata_raft_entry(entry)?,
    })
}

fn entry(entry: WireMetadataRaftEntry) -> Result<MetadataRaftEntry, SharedLogError> {
    let decoded = decode_metadata_raft_entry(&entry.payload)?;
    let expected = wire_log_id(decoded.log_id);
    if expected != entry.log_id {
        return Err(SharedLogError::Backend(format!(
            "metadata raft entry log id mismatch: wire {:?}, payload {:?}",
            entry.log_id, expected
        )));
    }
    Ok(decoded)
}

fn wire_snapshot_meta(meta: &SnapshotMeta<u64, BasicNode>) -> WireMetadataRaftSnapshotMeta {
    WireMetadataRaftSnapshotMeta {
        last_log_id: meta.last_log_id.map(wire_log_id),
        last_membership: wire_stored_membership(&meta.last_membership),
        snapshot_id: meta.snapshot_id.clone(),
    }
}

fn snapshot_meta(
    meta: WireMetadataRaftSnapshotMeta,
) -> Result<SnapshotMeta<u64, BasicNode>, SharedLogError> {
    Ok(SnapshotMeta {
        last_log_id: meta.last_log_id.map(log_id).transpose()?,
        last_membership: stored_membership(meta.last_membership)?,
        snapshot_id: meta.snapshot_id,
    })
}

fn wire_stored_membership(
    membership: &StoredMembership<u64, BasicNode>,
) -> WireMetadataRaftMembership {
    WireMetadataRaftMembership {
        log_id: membership.log_id().map(wire_log_id),
        voter_configs: membership
            .membership()
            .get_joint_config()
            .iter()
            .map(|config| config.iter().copied().collect::<Vec<_>>())
            .collect(),
        nodes: membership
            .nodes()
            .map(|(id, node)| WireMetadataRaftNode {
                id: *id,
                address: node.addr.clone(),
            })
            .collect(),
    }
}

fn stored_membership(
    membership: WireMetadataRaftMembership,
) -> Result<StoredMembership<u64, BasicNode>, SharedLogError> {
    let configs = membership
        .voter_configs
        .into_iter()
        .map(|config| config.into_iter().collect::<BTreeSet<_>>())
        .collect::<Vec<_>>();
    let nodes = membership
        .nodes
        .into_iter()
        .map(|node| (node.id, BasicNode { addr: node.address }))
        .collect::<BTreeMap<_, _>>();
    Ok(StoredMembership::new(
        membership.log_id.map(log_id).transpose()?,
        Membership::new(configs, nodes),
    ))
}

fn wire_vote(vote: Vote<u64>) -> WireMetadataRaftVote {
    WireMetadataRaftVote {
        leader_id: WireMetadataRaftLeaderId {
            term: vote.leader_id.term,
            voted_for: vote.leader_id.voted_for(),
        },
        committed: vote.committed,
    }
}

fn vote(vote: WireMetadataRaftVote) -> Result<Vote<u64>, SharedLogError> {
    let voted_for = vote.leader_id.voted_for.ok_or_else(|| {
        SharedLogError::Backend("metadata raft vote is missing voted_for node".to_owned())
    })?;
    Ok(Vote {
        leader_id: LeaderId::new(vote.leader_id.term, voted_for),
        committed: vote.committed,
    })
}

fn wire_log_id(log_id: LogId<u64>) -> WireMetadataRaftLogId {
    WireMetadataRaftLogId {
        leader_term: log_id.leader_id.term,
        leader_node: log_id.leader_id.node_id,
        index: log_id.index,
    }
}

fn log_id(log_id: WireMetadataRaftLogId) -> Result<LogId<u64>, SharedLogError> {
    Ok(LogId::new(
        openraft::CommittedLeaderId::new(log_id.leader_term, log_id.leader_node),
        log_id.index,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_meta::command::{
        CommandKind, MetadataCommand, Mutation, MutationOp, Predicate, PredicateRef, Value,
        Version, WatchProjection,
    };
    use nokvfs_types::RecordFamily;
    use openraft::entry::EntryPayload;
    use openraft::entry::FromAppData;
    use openraft::{CommittedLeaderId, LogId};

    #[test]
    fn append_entries_wire_round_trips_metadata_entries() {
        let entry = MetadataRaftEntry {
            log_id: LogId::new(CommittedLeaderId::new(3, 2), 11),
            payload: EntryPayload::Normal(
                crate::openraft_log::MetadataRaftCommandBatch::new(vec![metadata_command(
                    b"request-1",
                    b"dentry/a",
                    2,
                )])
                .unwrap(),
            ),
        };
        let request = AppendEntriesRequest {
            vote: Vote::new_committed(3, 2),
            prev_log_id: Some(LogId::new(CommittedLeaderId::new(3, 2), 10)),
            entries: vec![entry.clone()],
            leader_commit: Some(entry.log_id),
        };

        let wire = wire_append_entries_request(&request).unwrap();
        let decoded = append_entries_request(wire).unwrap();

        assert_eq!(decoded.vote, request.vote);
        assert_eq!(decoded.prev_log_id, request.prev_log_id);
        assert_eq!(decoded.entries, request.entries);
        assert_eq!(decoded.leader_commit, request.leader_commit);
    }

    #[test]
    fn vote_wire_round_trips_vote_state() {
        let request = VoteRequest {
            vote: Vote::new(7, 4),
            last_log_id: Some(LogId::new(CommittedLeaderId::new(6, 3), 9)),
        };

        let decoded = vote_request(wire_vote_request(&request)).unwrap();

        assert_eq!(decoded.vote, request.vote);
        assert_eq!(decoded.last_log_id, request.last_log_id);
    }

    #[test]
    fn vote_response_wire_round_trips_vote_state() {
        let response = VoteResponse {
            vote: Vote::new_committed(9, 5),
            vote_granted: true,
            last_log_id: Some(LogId::new(CommittedLeaderId::new(8, 5), 13)),
        };

        let decoded = vote_response(wire_vote_response(&response)).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn append_entries_response_wire_round_trips_status() {
        let partial = AppendEntriesResponse::PartialSuccess(Some(LogId::new(
            CommittedLeaderId::new(4, 2),
            17,
        )));
        let higher_vote = AppendEntriesResponse::HigherVote(Vote::new(6, 3));

        assert_eq!(
            append_entries_response(wire_append_entries_response(&partial)).unwrap(),
            partial
        );
        assert_eq!(
            append_entries_response(wire_append_entries_response(&higher_vote)).unwrap(),
            higher_vote
        );
    }

    #[test]
    fn snapshot_wire_round_trips_metadata() {
        let membership = Membership::new(
            vec![BTreeSet::from([1, 2, 3])],
            BTreeMap::from([
                (
                    1,
                    BasicNode {
                        addr: "127.0.0.1:7001".to_owned(),
                    },
                ),
                (
                    4,
                    BasicNode {
                        addr: "127.0.0.1:7004".to_owned(),
                    },
                ),
            ]),
        );
        let request = InstallSnapshotRequest {
            vote: Vote::new_committed(8, 1),
            meta: SnapshotMeta {
                last_log_id: Some(LogId::new(CommittedLeaderId::new(8, 1), 16)),
                last_membership: StoredMembership::new(
                    Some(LogId::new(CommittedLeaderId::new(8, 1), 16)),
                    membership,
                ),
                snapshot_id: "snapshot-8-16".to_owned(),
            },
            offset: 0,
            data: b"snapshot".to_vec(),
            done: true,
        };

        let decoded = install_snapshot_request(wire_install_snapshot_request(&request)).unwrap();

        assert_eq!(decoded.vote, request.vote);
        assert_eq!(decoded.meta.last_log_id, request.meta.last_log_id);
        assert_eq!(decoded.meta.snapshot_id, request.meta.snapshot_id);
        assert_eq!(decoded.data, request.data);
        assert!(decoded.done);
    }

    #[test]
    fn snapshot_response_wire_round_trips_vote_state() {
        let response = InstallSnapshotResponse {
            vote: Vote::new_committed(8, 1),
        };

        let decoded = install_snapshot_response(wire_install_snapshot_response(&response)).unwrap();

        assert_eq!(decoded.vote, response.vote);
    }

    #[test]
    fn entry_decode_rejects_mismatched_wire_log_id() {
        let entry = MetadataRaftEntry::from_app_data(
            crate::openraft_log::MetadataRaftCommandBatch::new(vec![metadata_command(
                b"request-1",
                b"dentry/a",
                2,
            )])
            .unwrap(),
        );
        let mut wire = wire_entry(&entry).unwrap();
        wire.log_id.index += 1;

        assert!(matches!(
            self::entry(wire),
            Err(SharedLogError::Backend(message)) if message.contains("log id mismatch")
        ));
    }

    fn metadata_command(request_id: &[u8], key: &[u8], commit_version: u64) -> MetadataCommand {
        MetadataCommand {
            request_id: request_id.to_vec(),
            kind: CommandKind::CreateFile,
            read_version: Version::new(1).unwrap(),
            commit_version: Version::new(commit_version).unwrap(),
            primary_family: RecordFamily::Dentry,
            primary_key: key.to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Dentry,
                key: key.to_vec(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: key.to_vec(),
                op: MutationOp::Put,
                value: Some(Value(b"inode=2".to_vec())),
            }],
            watch: vec![WatchProjection {
                family: RecordFamily::Dentry,
                key: key.to_vec(),
                event: b"create".to_vec(),
            }],
        }
    }
}
