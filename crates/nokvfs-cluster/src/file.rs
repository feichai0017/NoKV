use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use nokvfs_meta::command::{
    CommandKind, MetadataCommand, Mutation, MutationOp, Predicate, PredicateRef, Value, Version,
    WatchProjection,
};
use nokvfs_types::{MountId, RecordFamily};

use crate::{
    DurableReceipt, LogIndex, LogPosition, LogTerm, MetadataLogEntry, SharedLogError,
    SharedMetadataLog,
};

const FRAME_MAGIC: &[u8; 8] = b"NKFSLG01";
const RECORD_ENTRY: u8 = 1;
const RECORD_COMPACT: u8 = 2;

#[derive(Debug)]
pub struct FileSharedLog {
    inner: Mutex<FileLogState>,
}

#[derive(Debug)]
struct FileLogState {
    file: File,
    next_index: u64,
    compacted_through: LogIndex,
    entries: VecDeque<MetadataLogEntry>,
}

impl FileSharedLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedLogError> {
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
            inner: Mutex::new(FileLogState {
                file,
                next_index: recovered.next_index,
                compacted_through: recovered.compacted_through,
                entries: recovered.entries,
            }),
        })
    }
}

impl SharedMetadataLog for FileSharedLog {
    fn append_batch(
        &self,
        term: LogTerm,
        mount: MountId,
        commands: &[MetadataCommand],
    ) -> Result<Vec<DurableReceipt>, SharedLogError> {
        if commands.is_empty() {
            return Err(SharedLogError::EmptyBatch);
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("file shared log mutex poisoned".to_owned()))?;
        let index = LogIndex::new(inner.next_index)?;
        let position = LogPosition { term, index };
        let entry = MetadataLogEntry {
            position,
            mount,
            commands: commands.to_vec(),
        };
        append_record(&mut inner.file, &encode_entry_record(&entry)?)?;
        inner.next_index = inner.next_index.saturating_add(1);
        inner.entries.push_back(entry);
        Ok(commands
            .iter()
            .enumerate()
            .map(|(batch_position, command)| DurableReceipt {
                position,
                mount,
                batch_position,
                request_id: command.request_id.clone(),
                commit_version: command.commit_version,
            })
            .collect())
    }

    fn read_from(
        &self,
        start: LogIndex,
        limit: usize,
    ) -> Result<Vec<MetadataLogEntry>, SharedLogError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("file shared log mutex poisoned".to_owned()))?;
        if start <= inner.compacted_through {
            return Err(SharedLogError::Compacted {
                requested: start,
                compacted: inner.compacted_through,
            });
        }
        let limit = if limit == 0 { usize::MAX } else { limit };
        Ok(inner
            .entries
            .iter()
            .filter(|entry| entry.position.index >= start)
            .take(limit)
            .cloned()
            .collect())
    }

    fn compact_through(&self, index: LogIndex) -> Result<(), SharedLogError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("file shared log mutex poisoned".to_owned()))?;
        let compacted_through = inner.compacted_through.max(index);
        if compacted_through == inner.compacted_through {
            return Ok(());
        }
        append_record(&mut inner.file, &encode_compact_record(compacted_through))?;
        inner.compacted_through = compacted_through;
        while inner
            .entries
            .front()
            .is_some_and(|entry| entry.position.index <= compacted_through)
        {
            inner.entries.pop_front();
        }
        Ok(())
    }

    fn committed_index(&self) -> LogIndex {
        self.inner
            .lock()
            .map(|inner| {
                LogIndex::new(inner.next_index.saturating_sub(1)).unwrap_or(LogIndex::ZERO)
            })
            .unwrap_or(LogIndex::ZERO)
    }
}

#[derive(Debug)]
struct RecoveredLog {
    next_index: u64,
    compacted_through: LogIndex,
    entries: VecDeque<MetadataLogEntry>,
}

fn recover(file: &mut File) -> Result<RecoveredLog, SharedLogError> {
    file.seek(SeekFrom::Start(0)).map_err(to_backend_error)?;
    let mut entries = VecDeque::new();
    let mut compacted_through = LogIndex::ZERO;
    let mut next_index = 1_u64;
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
            DecodedRecord::Entry(entry) => {
                next_index = next_index.max(entry.position.index.get().saturating_add(1));
                if entry.position.index > compacted_through {
                    entries.push_back(entry);
                }
            }
            DecodedRecord::Compact(index) => {
                compacted_through = compacted_through.max(index);
                while entries
                    .front()
                    .is_some_and(|entry| entry.position.index <= compacted_through)
                {
                    entries.pop_front();
                }
            }
        }
    }
    Ok(RecoveredLog {
        next_index,
        compacted_through,
        entries,
    })
}

fn append_record(file: &mut File, payload: &[u8]) -> Result<(), SharedLogError> {
    file.seek(SeekFrom::End(0)).map_err(to_backend_error)?;
    file.write_all(FRAME_MAGIC).map_err(to_backend_error)?;
    file.write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(to_backend_error)?;
    file.write_all(payload).map_err(to_backend_error)?;
    file.flush().map_err(to_backend_error)?;
    file.sync_data().map_err(to_backend_error)
}

enum FrameRead {
    Frame(Vec<u8>),
    Eof,
    Partial,
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
            "file shared log frame has invalid magic".to_owned(),
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

enum DecodedRecord {
    Entry(MetadataLogEntry),
    Compact(LogIndex),
}

fn encode_entry_record(entry: &MetadataLogEntry) -> Result<Vec<u8>, SharedLogError> {
    let mut out = vec![RECORD_ENTRY];
    push_u64(&mut out, entry.position.term.get());
    push_u64(&mut out, entry.position.index.get());
    push_u64(&mut out, entry.mount.get());
    push_len(&mut out, entry.commands.len())?;
    for command in &entry.commands {
        encode_command(&mut out, command)?;
    }
    Ok(out)
}

fn encode_compact_record(index: LogIndex) -> Vec<u8> {
    let mut out = vec![RECORD_COMPACT];
    push_u64(&mut out, index.get());
    out
}

fn decode_record(payload: &[u8]) -> Result<DecodedRecord, SharedLogError> {
    let mut input = Decoder::new(payload);
    match input.u8()? {
        RECORD_ENTRY => {
            let term = LogTerm::new(input.u64()?)?;
            let index = LogIndex::new(input.u64()?)?;
            let mount = MountId::new(input.u64()?).map_err(|err| {
                SharedLogError::Backend(format!("invalid metadata log mount id: {err}"))
            })?;
            let command_count = input.len()?;
            let mut commands = Vec::with_capacity(command_count);
            for _ in 0..command_count {
                commands.push(decode_command(&mut input)?);
            }
            input.finish()?;
            Ok(DecodedRecord::Entry(MetadataLogEntry {
                position: LogPosition { term, index },
                mount,
                commands,
            }))
        }
        RECORD_COMPACT => {
            let index = LogIndex::new(input.u64()?)?;
            input.finish()?;
            Ok(DecodedRecord::Compact(index))
        }
        tag => Err(SharedLogError::Backend(format!(
            "unknown file shared log record tag {tag}"
        ))),
    }
}

fn encode_command(out: &mut Vec<u8>, command: &MetadataCommand) -> Result<(), SharedLogError> {
    push_bytes(out, &command.request_id)?;
    out.push(command_kind_tag(command.kind));
    push_u64(out, command.read_version.get());
    push_u64(out, command.commit_version.get());
    out.push(record_family_tag(command.primary_family));
    push_bytes(out, &command.primary_key)?;
    push_len(out, command.predicates.len())?;
    for predicate in &command.predicates {
        out.push(record_family_tag(predicate.family));
        push_bytes(out, &predicate.key)?;
        encode_predicate(out, predicate.predicate);
    }
    push_len(out, command.mutations.len())?;
    for mutation in &command.mutations {
        out.push(record_family_tag(mutation.family));
        push_bytes(out, &mutation.key)?;
        out.push(mutation_op_tag(mutation.op));
        match &mutation.value {
            Some(value) => {
                out.push(1);
                push_bytes(out, &value.0)?;
            }
            None => out.push(0),
        }
    }
    push_len(out, command.watch.len())?;
    for watch in &command.watch {
        out.push(record_family_tag(watch.family));
        push_bytes(out, &watch.key)?;
        push_bytes(out, &watch.event)?;
    }
    Ok(())
}

fn decode_command(input: &mut Decoder<'_>) -> Result<MetadataCommand, SharedLogError> {
    let request_id = input.bytes()?;
    let kind = command_kind_from_tag(input.u8()?)?;
    let read_version = Version::new(input.u64()?)
        .map_err(|err| SharedLogError::Backend(format!("invalid read version: {err}")))?;
    let commit_version = Version::new(input.u64()?)
        .map_err(|err| SharedLogError::Backend(format!("invalid commit version: {err}")))?;
    let primary_family = record_family_from_tag(input.u8()?)?;
    let primary_key = input.bytes()?;
    let predicate_count = input.len()?;
    let mut predicates = Vec::with_capacity(predicate_count);
    for _ in 0..predicate_count {
        predicates.push(PredicateRef {
            family: record_family_from_tag(input.u8()?)?,
            key: input.bytes()?,
            predicate: decode_predicate(input)?,
        });
    }
    let mutation_count = input.len()?;
    let mut mutations = Vec::with_capacity(mutation_count);
    for _ in 0..mutation_count {
        let family = record_family_from_tag(input.u8()?)?;
        let key = input.bytes()?;
        let op = mutation_op_from_tag(input.u8()?)?;
        let value = match input.u8()? {
            0 => None,
            1 => Some(Value(input.bytes()?)),
            tag => {
                return Err(SharedLogError::Backend(format!(
                    "invalid mutation value tag {tag}"
                )))
            }
        };
        mutations.push(Mutation {
            family,
            key,
            op,
            value,
        });
    }
    let watch_count = input.len()?;
    let mut watch = Vec::with_capacity(watch_count);
    for _ in 0..watch_count {
        watch.push(WatchProjection {
            family: record_family_from_tag(input.u8()?)?,
            key: input.bytes()?,
            event: input.bytes()?,
        });
    }
    let command = MetadataCommand {
        request_id,
        kind,
        read_version,
        commit_version,
        primary_family,
        primary_key,
        predicates,
        mutations,
        watch,
    };
    command
        .validate()
        .map_err(|err| SharedLogError::Backend(format!("invalid metadata command: {err}")))?;
    Ok(command)
}

fn encode_predicate(out: &mut Vec<u8>, predicate: Predicate) {
    match predicate {
        Predicate::Exists => out.push(1),
        Predicate::NotExists => out.push(2),
        Predicate::PrefixEmpty => out.push(3),
        Predicate::VersionEquals(version) => {
            out.push(4);
            push_u64(out, version.get());
        }
    }
}

fn decode_predicate(input: &mut Decoder<'_>) -> Result<Predicate, SharedLogError> {
    match input.u8()? {
        1 => Ok(Predicate::Exists),
        2 => Ok(Predicate::NotExists),
        3 => Ok(Predicate::PrefixEmpty),
        4 => Ok(Predicate::VersionEquals(
            Version::new(input.u64()?).map_err(|err| {
                SharedLogError::Backend(format!("invalid predicate version: {err}"))
            })?,
        )),
        tag => Err(SharedLogError::Backend(format!(
            "unknown predicate tag {tag}"
        ))),
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
            "file shared log record has trailing bytes".to_owned(),
        ))
    }

    fn u8(&mut self) -> Result<u8, SharedLogError> {
        let value = *self.input.get(self.offset).ok_or_else(|| {
            SharedLogError::Backend("file shared log record is truncated".to_owned())
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
            SharedLogError::Backend("file shared log length overflows usize".to_owned())
        })
    }

    fn bytes(&mut self) -> Result<Vec<u8>, SharedLogError> {
        let len = self.len()?;
        Ok(self.take(len)?.to_vec())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], SharedLogError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| SharedLogError::Backend("file shared log offset overflow".to_owned()))?;
        let bytes = self.input.get(self.offset..end).ok_or_else(|| {
            SharedLogError::Backend("file shared log record is truncated".to_owned())
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
        u64::try_from(len)
            .map_err(|_| SharedLogError::Backend("metadata log length overflows u64".to_owned()))?,
    );
    Ok(())
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), SharedLogError> {
    push_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn to_backend_error(err: impl std::error::Error) -> SharedLogError {
    SharedLogError::Backend(err.to_string())
}

fn command_kind_tag(kind: CommandKind) -> u8 {
    match kind {
        CommandKind::ReserveAllocator => 1,
        CommandKind::CreateFile => 2,
        CommandKind::CreateFiles => 3,
        CommandKind::CreateDir => 4,
        CommandKind::CreateSymlink => 5,
        CommandKind::UpdateAttr => 6,
        CommandKind::SetXattr => 7,
        CommandKind::RemoveXattr => 8,
        CommandKind::Rename => 9,
        CommandKind::RenameReplace => 10,
        CommandKind::RemoveFile => 11,
        CommandKind::RemoveEmptyDir => 12,
        CommandKind::PublishArtifact => 13,
        CommandKind::ReplaceArtifact => 14,
        CommandKind::SnapshotSubtree => 15,
        CommandKind::RetireSnapshot => 16,
        CommandKind::WatchSubtree => 17,
        CommandKind::CleanupObjects => 18,
    }
}

fn command_kind_from_tag(tag: u8) -> Result<CommandKind, SharedLogError> {
    match tag {
        1 => Ok(CommandKind::ReserveAllocator),
        2 => Ok(CommandKind::CreateFile),
        3 => Ok(CommandKind::CreateFiles),
        4 => Ok(CommandKind::CreateDir),
        5 => Ok(CommandKind::CreateSymlink),
        6 => Ok(CommandKind::UpdateAttr),
        7 => Ok(CommandKind::SetXattr),
        8 => Ok(CommandKind::RemoveXattr),
        9 => Ok(CommandKind::Rename),
        10 => Ok(CommandKind::RenameReplace),
        11 => Ok(CommandKind::RemoveFile),
        12 => Ok(CommandKind::RemoveEmptyDir),
        13 => Ok(CommandKind::PublishArtifact),
        14 => Ok(CommandKind::ReplaceArtifact),
        15 => Ok(CommandKind::SnapshotSubtree),
        16 => Ok(CommandKind::RetireSnapshot),
        17 => Ok(CommandKind::WatchSubtree),
        18 => Ok(CommandKind::CleanupObjects),
        tag => Err(SharedLogError::Backend(format!(
            "unknown command kind tag {tag}"
        ))),
    }
}

fn record_family_tag(family: RecordFamily) -> u8 {
    match family {
        RecordFamily::System => 1,
        RecordFamily::Mount => 2,
        RecordFamily::Inode => 3,
        RecordFamily::Dentry => 4,
        RecordFamily::Parent => 5,
        RecordFamily::Xattr => 6,
        RecordFamily::ChunkManifest => 7,
        RecordFamily::Session => 8,
        RecordFamily::PathIndex => 9,
        RecordFamily::Watch => 10,
        RecordFamily::Snapshot => 11,
        RecordFamily::Gc => 12,
        RecordFamily::CommandDedupe => 13,
        RecordFamily::History => 14,
    }
}

fn record_family_from_tag(tag: u8) -> Result<RecordFamily, SharedLogError> {
    match tag {
        1 => Ok(RecordFamily::System),
        2 => Ok(RecordFamily::Mount),
        3 => Ok(RecordFamily::Inode),
        4 => Ok(RecordFamily::Dentry),
        5 => Ok(RecordFamily::Parent),
        6 => Ok(RecordFamily::Xattr),
        7 => Ok(RecordFamily::ChunkManifest),
        8 => Ok(RecordFamily::Session),
        9 => Ok(RecordFamily::PathIndex),
        10 => Ok(RecordFamily::Watch),
        11 => Ok(RecordFamily::Snapshot),
        12 => Ok(RecordFamily::Gc),
        13 => Ok(RecordFamily::CommandDedupe),
        14 => Ok(RecordFamily::History),
        tag => Err(SharedLogError::Backend(format!(
            "unknown record family tag {tag}"
        ))),
    }
}

fn mutation_op_tag(op: MutationOp) -> u8 {
    match op {
        MutationOp::Put => 1,
        MutationOp::Delete => 2,
    }
}

fn mutation_op_from_tag(tag: u8) -> Result<MutationOp, SharedLogError> {
    match tag {
        1 => Ok(MutationOp::Put),
        2 => Ok(MutationOp::Delete),
        tag => Err(SharedLogError::Backend(format!(
            "unknown mutation op tag {tag}"
        ))),
    }
}
