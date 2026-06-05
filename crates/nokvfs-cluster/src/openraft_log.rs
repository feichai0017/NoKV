//! OpenRaft type boundary for NoKV metadata replication.
//!
//! OpenRaft owns ordering and quorum. NoKV keeps filesystem semantics in
//! `MetadataCommand` batches and applies them through the metadata store.

use std::io::Cursor;

use nokvfs_meta::command::{CommitResult, MetadataCommand, MetadataError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataRaftCommandBatch {
    pub commands: Vec<MetadataCommand>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataRaftApplyBatchResult {
    pub results: Vec<Result<CommitResult, MetadataError>>,
}

openraft::declare_raft_types!(
    pub MetadataRaftConfig:
        D = MetadataRaftCommandBatch,
        R = MetadataRaftApplyBatchResult,
        NodeId = u64,
);

pub type MetadataRaftEntry = <MetadataRaftConfig as openraft::RaftTypeConfig>::Entry;

impl MetadataRaftCommandBatch {
    pub fn new(commands: Vec<MetadataCommand>) -> Result<Self, MetadataError> {
        if commands.is_empty() {
            return Err(MetadataError::Backend(
                "metadata raft batch cannot be empty".to_owned(),
            ));
        }
        Ok(Self { commands })
    }

    pub fn command_count(&self) -> usize {
        self.commands.len()
    }
}

impl MetadataRaftApplyBatchResult {
    pub fn success(results: Vec<CommitResult>) -> Self {
        Self {
            results: results.into_iter().map(Ok).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_meta::command::{
        CommandKind, Mutation, MutationOp, PredicateRef, Value, Version, WatchProjection,
    };
    use nokvfs_types::RecordFamily;
    use openraft::{entry::FromAppData, EntryPayload};

    #[test]
    fn openraft_entry_payload_is_metadata_command_batch() {
        let command = metadata_command(b"request-1", b"dentry/a", 2);
        let batch = MetadataRaftCommandBatch::new(vec![command.clone()]).unwrap();

        let entry = MetadataRaftEntry::from_app_data(batch);

        match entry.payload {
            EntryPayload::Normal(payload) => {
                assert_eq!(payload.command_count(), 1);
                assert_eq!(payload.commands[0], command);
            }
            other => panic!("metadata raft entry used non-command payload: {other:?}"),
        }
    }

    #[test]
    fn openraft_batch_rejects_empty_command_vectors() {
        assert!(matches!(
            MetadataRaftCommandBatch::new(Vec::new()),
            Err(MetadataError::Backend(message))
                if message.contains("metadata raft batch cannot be empty")
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
                predicate: nokvfs_meta::command::Predicate::NotExists,
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
