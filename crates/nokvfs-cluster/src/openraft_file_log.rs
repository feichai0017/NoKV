//! File-backed OpenRaft log storage for metadata command batches.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex};

use openraft::entry::EntryPayload;
use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    BasicNode, ErrorSubject, ErrorVerb, LeaderId, LogId, LogState, Membership, RaftLogReader,
    StorageError, Vote,
};

use crate::file::{decode_metadata_command_batch, encode_metadata_command_batch};
use crate::openraft_log::{MetadataRaftCommandBatch, MetadataRaftConfig, MetadataRaftEntry};
use crate::SharedLogError;

const FRAME_MAGIC: &[u8; 8] = b"NKFSRF01";
const RECORD_ENTRY: u8 = 1;
const RECORD_VOTE: u8 = 2;
const RECORD_COMMITTED: u8 = 3;
const RECORD_PURGE: u8 = 4;
const RECORD_TRUNCATE: u8 = 5;

#[derive(Clone, Debug)]
pub struct FileMetadataRaftLog {
    inner: Arc<Mutex<FileMetadataRaftLogState>>,
    sync: FileMetadataRaftLogSync,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileMetadataRaftLogOptions {
    pub sync: FileMetadataRaftLogSync,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FileMetadataRaftLogSync {
    #[default]
    Data,
    None,
}

#[derive(Debug)]
struct FileMetadataRaftLogState {
    file: File,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged_log_id: Option<LogId<u64>>,
    entries: BTreeMap<u64, MetadataRaftEntry>,
}

#[derive(Debug)]
struct RecoveredRaftLog {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged_log_id: Option<LogId<u64>>,
    entries: BTreeMap<u64, MetadataRaftEntry>,
}

enum DecodedRaftRecord {
    Entry(MetadataRaftEntry),
    Vote(Option<Vote<u64>>),
    Committed(Option<LogId<u64>>),
    Purge(LogId<u64>),
    Truncate(LogId<u64>),
}

enum FrameRead {
    Frame(Vec<u8>),
    Eof,
    Partial,
}

impl FileMetadataRaftLog {
    pub fn open(
        path: impl AsRef<Path>,
        options: FileMetadataRaftLogOptions,
    ) -> Result<Self, SharedLogError> {
        if let Some(parent) = path.as_ref().parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(to_backend_error)?;
            }
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(to_backend_error)?;
        let recovered = recover(&mut file)?;
        file.seek(SeekFrom::End(0)).map_err(to_backend_error)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(FileMetadataRaftLogState {
                file,
                vote: recovered.vote,
                committed: recovered.committed,
                last_purged_log_id: recovered.last_purged_log_id,
                entries: recovered.entries,
            })),
            sync: options.sync,
        })
    }

    pub fn last_log_id(&self) -> Option<LogId<u64>> {
        self.inner.lock().ok().and_then(|inner| inner.last_log_id())
    }
}

impl FileMetadataRaftLogState {
    fn last_log_id(&self) -> Option<LogId<u64>> {
        self.entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(self.last_purged_log_id)
    }
}

pub(crate) fn encode_metadata_raft_entry(
    entry: &MetadataRaftEntry,
) -> Result<Vec<u8>, SharedLogError> {
    encode_entry_record(entry)
}

pub(crate) fn decode_metadata_raft_entry(
    payload: &[u8],
) -> Result<MetadataRaftEntry, SharedLogError> {
    match decode_record(payload)? {
        DecodedRaftRecord::Entry(entry) => Ok(entry),
        _ => Err(SharedLogError::Backend(
            "metadata raft wire payload is not a log entry".to_owned(),
        )),
    }
}

impl RaftLogReader<MetadataRaftConfig> for FileMetadataRaftLog {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<MetadataRaftEntry>, StorageError<u64>> {
        let inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                "file metadata raft log mutex poisoned",
            )
        })?;
        Ok(inner
            .entries
            .iter()
            .filter(|(index, _)| range.contains(*index))
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<MetadataRaftConfig> for FileMetadataRaftLog {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<MetadataRaftConfig>, StorageError<u64>> {
        let inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                "file metadata raft log mutex poisoned",
            )
        })?;
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: inner.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Vote,
                ErrorVerb::Write,
                "file metadata raft log mutex poisoned",
            )
        })?;
        append_record(
            &mut inner.file,
            &encode_vote_record(Some(*vote)).map_err(log_storage_error)?,
            self.sync,
        )
        .map_err(log_storage_error)?;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Vote,
                ErrorVerb::Read,
                "file metadata raft log mutex poisoned",
            )
        })?;
        Ok(inner.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Write,
                "file metadata raft committed pointer mutex poisoned",
            )
        })?;
        append_record(
            &mut inner.file,
            &encode_committed_record(committed),
            self.sync,
        )
        .map_err(log_storage_error)?;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                "file metadata raft committed pointer mutex poisoned",
            )
        })?;
        Ok(inner.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<MetadataRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = MetadataRaftEntry> + Send,
        I::IntoIter: Send,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let mut inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Write,
                "file metadata raft log mutex poisoned",
            )
        })?;
        for entry in entries {
            append_record(
                &mut inner.file,
                &encode_entry_record(&entry).map_err(log_storage_error)?,
                self.sync,
            )
            .map_err(log_storage_error)?;
            inner.entries.insert(entry.log_id.index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Log(log_id),
                ErrorVerb::Delete,
                "file metadata raft log mutex poisoned",
            )
        })?;
        append_record(
            &mut inner.file,
            &encode_log_id_record(RECORD_TRUNCATE, log_id),
            self.sync,
        )
        .map_err(log_storage_error)?;
        truncate_from(&mut inner.entries, log_id);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().map_err(|_| {
            storage_error(
                ErrorSubject::Log(log_id),
                ErrorVerb::Delete,
                "file metadata raft log mutex poisoned",
            )
        })?;
        append_record(
            &mut inner.file,
            &encode_log_id_record(RECORD_PURGE, log_id),
            self.sync,
        )
        .map_err(log_storage_error)?;
        purge_through(&mut inner.entries, log_id);
        inner.last_purged_log_id = Some(log_id);
        Ok(())
    }
}

fn recover(file: &mut File) -> Result<RecoveredRaftLog, SharedLogError> {
    file.seek(SeekFrom::Start(0)).map_err(to_backend_error)?;
    let mut recovered = RecoveredRaftLog {
        vote: None,
        committed: None,
        last_purged_log_id: None,
        entries: BTreeMap::new(),
    };
    loop {
        let frame_start = file.stream_position().map_err(to_backend_error)?;
        let payload = match read_frame(file)? {
            FrameRead::Frame(payload) => payload,
            FrameRead::Eof => break,
            FrameRead::Partial => {
                file.set_len(frame_start).map_err(to_backend_error)?;
                file.seek(SeekFrom::End(0)).map_err(to_backend_error)?;
                break;
            }
        };
        match decode_record(&payload)? {
            DecodedRaftRecord::Entry(entry) => {
                if recovered
                    .last_purged_log_id
                    .is_none_or(|purged| entry.log_id.index > purged.index)
                {
                    recovered.entries.insert(entry.log_id.index, entry);
                }
            }
            DecodedRaftRecord::Vote(vote) => recovered.vote = vote,
            DecodedRaftRecord::Committed(committed) => recovered.committed = committed,
            DecodedRaftRecord::Purge(log_id) => {
                purge_through(&mut recovered.entries, log_id);
                recovered.last_purged_log_id = Some(log_id);
            }
            DecodedRaftRecord::Truncate(log_id) => truncate_from(&mut recovered.entries, log_id),
        }
    }
    Ok(recovered)
}

fn append_record(
    file: &mut File,
    payload: &[u8],
    sync: FileMetadataRaftLogSync,
) -> Result<(), SharedLogError> {
    file.seek(SeekFrom::End(0)).map_err(to_backend_error)?;
    file.write_all(FRAME_MAGIC).map_err(to_backend_error)?;
    file.write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(to_backend_error)?;
    file.write_all(payload).map_err(to_backend_error)?;
    file.flush().map_err(to_backend_error)?;
    match sync {
        FileMetadataRaftLogSync::Data => file.sync_data().map_err(to_backend_error),
        FileMetadataRaftLogSync::None => Ok(()),
    }
}

fn read_frame(file: &mut File) -> Result<FrameRead, SharedLogError> {
    let mut header = [0_u8; 12];
    let mut read = 0_usize;
    while read < header.len() {
        let bytes = file.read(&mut header[read..]).map_err(to_backend_error)?;
        if bytes == 0 {
            return if read == 0 {
                Ok(FrameRead::Eof)
            } else {
                Ok(FrameRead::Partial)
            };
        }
        read += bytes;
    }
    if &header[..8] != FRAME_MAGIC {
        return Err(SharedLogError::Backend(
            "file metadata raft log frame has invalid magic".to_owned(),
        ));
    }
    let len = u32::from_be_bytes(
        header[8..12]
            .try_into()
            .expect("frame length has fixed width"),
    ) as usize;
    let mut payload = vec![0_u8; len];
    let mut read = 0_usize;
    while read < payload.len() {
        let bytes = file.read(&mut payload[read..]).map_err(to_backend_error)?;
        if bytes == 0 {
            return Ok(FrameRead::Partial);
        }
        read += bytes;
    }
    Ok(FrameRead::Frame(payload))
}

fn encode_entry_record(entry: &MetadataRaftEntry) -> Result<Vec<u8>, SharedLogError> {
    let mut out = vec![RECORD_ENTRY];
    encode_log_id(&mut out, entry.log_id);
    match &entry.payload {
        EntryPayload::Blank => out.push(1),
        EntryPayload::Normal(batch) => {
            out.push(2);
            push_bytes(&mut out, &encode_metadata_command_batch(&batch.commands)?)?;
        }
        EntryPayload::Membership(membership) => {
            out.push(3);
            encode_membership(&mut out, membership)?;
        }
    }
    Ok(out)
}

fn encode_vote_record(vote: Option<Vote<u64>>) -> Result<Vec<u8>, SharedLogError> {
    let mut out = vec![RECORD_VOTE];
    match vote {
        Some(vote) => {
            out.push(1);
            push_u64(&mut out, vote.leader_id.term);
            push_u64(&mut out, vote.leader_id.node_id);
            out.push(u8::from(vote.committed));
        }
        None => out.push(0),
    }
    Ok(out)
}

fn encode_committed_record(committed: Option<LogId<u64>>) -> Vec<u8> {
    let mut out = vec![RECORD_COMMITTED];
    encode_optional_log_id(&mut out, committed);
    out
}

fn encode_log_id_record(tag: u8, log_id: LogId<u64>) -> Vec<u8> {
    let mut out = vec![tag];
    encode_log_id(&mut out, log_id);
    out
}

fn decode_record(payload: &[u8]) -> Result<DecodedRaftRecord, SharedLogError> {
    let mut input = Decoder::new(payload);
    let record = match input.u8()? {
        RECORD_ENTRY => {
            let log_id = decode_log_id(&mut input)?;
            let payload = match input.u8()? {
                1 => EntryPayload::Blank,
                2 => {
                    let commands = decode_metadata_command_batch(&input.bytes()?)?;
                    EntryPayload::Normal(MetadataRaftCommandBatch::new(commands).map_err(
                        |err| SharedLogError::Backend(format!("invalid raft command batch: {err}")),
                    )?)
                }
                3 => EntryPayload::Membership(decode_membership(&mut input)?),
                tag => {
                    return Err(SharedLogError::Backend(format!(
                        "unknown metadata raft entry payload tag {tag}"
                    )))
                }
            };
            DecodedRaftRecord::Entry(MetadataRaftEntry { log_id, payload })
        }
        RECORD_VOTE => DecodedRaftRecord::Vote(decode_vote(&mut input)?),
        RECORD_COMMITTED => DecodedRaftRecord::Committed(decode_optional_log_id(&mut input)?),
        RECORD_PURGE => DecodedRaftRecord::Purge(decode_log_id(&mut input)?),
        RECORD_TRUNCATE => DecodedRaftRecord::Truncate(decode_log_id(&mut input)?),
        tag => {
            return Err(SharedLogError::Backend(format!(
                "unknown metadata raft log record tag {tag}"
            )))
        }
    };
    input.finish()?;
    Ok(record)
}

fn encode_membership(
    out: &mut Vec<u8>,
    membership: &Membership<u64, BasicNode>,
) -> Result<(), SharedLogError> {
    push_len(out, membership.get_joint_config().len())?;
    for config in membership.get_joint_config() {
        push_len(out, config.len())?;
        for node in config {
            push_u64(out, *node);
        }
    }
    let nodes = membership.nodes().collect::<Vec<_>>();
    push_len(out, nodes.len())?;
    for (node, basic) in nodes {
        push_u64(out, *node);
        push_bytes(out, basic.addr.as_bytes())?;
    }
    Ok(())
}

fn decode_membership(
    input: &mut Decoder<'_>,
) -> Result<Membership<u64, BasicNode>, SharedLogError> {
    let config_count = input.len()?;
    let mut configs = Vec::with_capacity(config_count);
    for _ in 0..config_count {
        let node_count = input.len()?;
        let mut config = BTreeSet::new();
        for _ in 0..node_count {
            config.insert(input.u64()?);
        }
        configs.push(config);
    }
    let node_count = input.len()?;
    let mut nodes = BTreeMap::new();
    for _ in 0..node_count {
        let node = input.u64()?;
        let addr = String::from_utf8(input.bytes()?)
            .map_err(|err| SharedLogError::Backend(format!("invalid raft node address: {err}")))?;
        nodes.insert(node, BasicNode { addr });
    }
    Ok(Membership::new(configs, nodes))
}

fn decode_vote(input: &mut Decoder<'_>) -> Result<Option<Vote<u64>>, SharedLogError> {
    match input.u8()? {
        0 => Ok(None),
        1 => {
            let term = input.u64()?;
            let node_id = input.u64()?;
            let committed = match input.u8()? {
                0 => false,
                1 => true,
                tag => {
                    return Err(SharedLogError::Backend(format!(
                        "invalid metadata raft vote committed tag {tag}"
                    )))
                }
            };
            Ok(Some(Vote {
                leader_id: LeaderId::new(term, node_id),
                committed,
            }))
        }
        tag => Err(SharedLogError::Backend(format!(
            "invalid metadata raft vote option tag {tag}"
        ))),
    }
}

fn encode_optional_log_id(out: &mut Vec<u8>, log_id: Option<LogId<u64>>) {
    match log_id {
        Some(log_id) => {
            out.push(1);
            encode_log_id(out, log_id);
        }
        None => out.push(0),
    }
}

fn decode_optional_log_id(input: &mut Decoder<'_>) -> Result<Option<LogId<u64>>, SharedLogError> {
    match input.u8()? {
        0 => Ok(None),
        1 => Ok(Some(decode_log_id(input)?)),
        tag => Err(SharedLogError::Backend(format!(
            "invalid metadata raft optional log id tag {tag}"
        ))),
    }
}

fn encode_log_id(out: &mut Vec<u8>, log_id: LogId<u64>) {
    push_u64(out, log_id.leader_id.term);
    push_u64(out, log_id.leader_id.node_id);
    push_u64(out, log_id.index);
}

fn decode_log_id(input: &mut Decoder<'_>) -> Result<LogId<u64>, SharedLogError> {
    let term = input.u64()?;
    let node_id = input.u64()?;
    let index = input.u64()?;
    Ok(LogId::new(
        openraft::CommittedLeaderId::new(term, node_id),
        index,
    ))
}

fn purge_through(entries: &mut BTreeMap<u64, MetadataRaftEntry>, log_id: LogId<u64>) {
    let keys = entries
        .range(..=log_id.index)
        .map(|(index, _)| *index)
        .collect::<Vec<_>>();
    for index in keys {
        entries.remove(&index);
    }
}

fn truncate_from(entries: &mut BTreeMap<u64, MetadataRaftEntry>, log_id: LogId<u64>) {
    let keys = entries
        .range(log_id.index..)
        .map(|(index, _)| *index)
        .collect::<Vec<_>>();
    for index in keys {
        entries.remove(&index);
    }
}

struct Decoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn finish(&self) -> Result<(), SharedLogError> {
        if self.offset == self.input.len() {
            return Ok(());
        }
        Err(SharedLogError::Backend(
            "file metadata raft log record has trailing bytes".to_owned(),
        ))
    }

    fn u8(&mut self) -> Result<u8, SharedLogError> {
        let value = *self.input.get(self.offset).ok_or_else(|| {
            SharedLogError::Backend("file metadata raft log record is truncated".to_owned())
        })?;
        self.offset += 1;
        Ok(value)
    }

    fn u64(&mut self) -> Result<u64, SharedLogError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes(
            bytes.try_into().expect("u64 field has fixed width"),
        ))
    }

    fn len(&mut self) -> Result<usize, SharedLogError> {
        let raw = self.u64()?;
        usize::try_from(raw).map_err(|_| {
            SharedLogError::Backend("metadata raft log length overflows usize".to_owned())
        })
    }

    fn bytes(&mut self) -> Result<Vec<u8>, SharedLogError> {
        let len = self.len()?;
        Ok(self.take(len)?.to_vec())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], SharedLogError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            SharedLogError::Backend("file metadata raft log offset overflow".to_owned())
        })?;
        let bytes = self.input.get(self.offset..end).ok_or_else(|| {
            SharedLogError::Backend("file metadata raft log record is truncated".to_owned())
        })?;
        self.offset = end;
        Ok(bytes)
    }
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn push_len(out: &mut Vec<u8>, len: usize) -> Result<(), SharedLogError> {
    push_u64(
        out,
        u64::try_from(len).map_err(|_| {
            SharedLogError::Backend("metadata raft log length overflows u64".to_owned())
        })?,
    );
    Ok(())
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), SharedLogError> {
    push_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn storage_error(subject: ErrorSubject<u64>, verb: ErrorVerb, message: &str) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, std::io::Error::other(message.to_owned()))
}

fn log_storage_error(err: SharedLogError) -> StorageError<u64> {
    storage_error(ErrorSubject::Logs, ErrorVerb::Write, &err.to_string())
}

fn to_backend_error(err: impl std::error::Error) -> SharedLogError {
    SharedLogError::Backend(err.to_string())
}
