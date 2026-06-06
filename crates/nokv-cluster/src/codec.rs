use nokv_meta::command::{
    CommandKind, MetadataCommand, Mutation, MutationOp, Predicate, PredicateRef, Value, Version,
    WatchProjection,
};
use nokv_types::RecordFamily;

use crate::MetadataRaftError;

pub(crate) fn encode_metadata_command_batch(
    commands: &[MetadataCommand],
) -> Result<Vec<u8>, MetadataRaftError> {
    if commands.is_empty() {
        return Err(MetadataRaftError::EmptyBatch);
    }
    let mut out = Vec::new();
    push_len(&mut out, commands.len())?;
    for command in commands {
        encode_command(&mut out, command)?;
    }
    Ok(out)
}

pub(crate) fn decode_metadata_command_batch(
    payload: &[u8],
) -> Result<Vec<MetadataCommand>, MetadataRaftError> {
    let mut input = Decoder::new(payload);
    let command_count = input.len()?;
    if command_count == 0 {
        return Err(MetadataRaftError::EmptyBatch);
    }
    let mut commands = Vec::with_capacity(command_count);
    for _ in 0..command_count {
        commands.push(decode_command(&mut input)?);
    }
    input.finish()?;
    Ok(commands)
}

fn encode_command(out: &mut Vec<u8>, command: &MetadataCommand) -> Result<(), MetadataRaftError> {
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

fn decode_command(input: &mut Decoder<'_>) -> Result<MetadataCommand, MetadataRaftError> {
    let request_id = input.bytes()?;
    let kind = command_kind_from_tag(input.u8()?)?;
    let read_version = Version::new(input.u64()?)
        .map_err(|err| MetadataRaftError::Backend(format!("invalid read version: {err}")))?;
    let commit_version = Version::new(input.u64()?)
        .map_err(|err| MetadataRaftError::Backend(format!("invalid commit version: {err}")))?;
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
                return Err(MetadataRaftError::Backend(format!(
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
        .map_err(|err| MetadataRaftError::Backend(format!("invalid metadata command: {err}")))?;
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

fn decode_predicate(input: &mut Decoder<'_>) -> Result<Predicate, MetadataRaftError> {
    match input.u8()? {
        1 => Ok(Predicate::Exists),
        2 => Ok(Predicate::NotExists),
        3 => Ok(Predicate::PrefixEmpty),
        4 => Ok(Predicate::VersionEquals(
            Version::new(input.u64()?).map_err(|err| {
                MetadataRaftError::Backend(format!("invalid predicate version: {err}"))
            })?,
        )),
        tag => Err(MetadataRaftError::Backend(format!(
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

    fn finish(&self) -> Result<(), MetadataRaftError> {
        if self.offset == self.input.len() {
            return Ok(());
        }
        Err(MetadataRaftError::Backend(
            "metadata command batch has trailing bytes".to_owned(),
        ))
    }

    fn u8(&mut self) -> Result<u8, MetadataRaftError> {
        let value = *self.input.get(self.offset).ok_or_else(|| {
            MetadataRaftError::Backend("metadata command batch is truncated".to_owned())
        })?;
        self.offset += 1;
        Ok(value)
    }

    fn u64(&mut self) -> Result<u64, MetadataRaftError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes(
            bytes.try_into().expect("u64 field has fixed width"),
        ))
    }

    fn len(&mut self) -> Result<usize, MetadataRaftError> {
        let raw = self.u64()?;
        usize::try_from(raw).map_err(|_| {
            MetadataRaftError::Backend("metadata command batch length overflows usize".to_owned())
        })
    }

    fn bytes(&mut self) -> Result<Vec<u8>, MetadataRaftError> {
        let len = self.len()?;
        Ok(self.take(len)?.to_vec())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], MetadataRaftError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            MetadataRaftError::Backend("metadata command batch offset overflow".to_owned())
        })?;
        let bytes = self.input.get(self.offset..end).ok_or_else(|| {
            MetadataRaftError::Backend("metadata command batch is truncated".to_owned())
        })?;
        self.offset = end;
        Ok(bytes)
    }
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn push_len(out: &mut Vec<u8>, len: usize) -> Result<(), MetadataRaftError> {
    push_u64(
        out,
        u64::try_from(len).map_err(|_| {
            MetadataRaftError::Backend("metadata command batch length overflows u64".to_owned())
        })?,
    );
    Ok(())
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), MetadataRaftError> {
    push_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
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

fn command_kind_from_tag(tag: u8) -> Result<CommandKind, MetadataRaftError> {
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
        tag => Err(MetadataRaftError::Backend(format!(
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

fn record_family_from_tag(tag: u8) -> Result<RecordFamily, MetadataRaftError> {
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
        tag => Err(MetadataRaftError::Backend(format!(
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

fn mutation_op_from_tag(tag: u8) -> Result<MutationOp, MetadataRaftError> {
    match tag {
        1 => Ok(MutationOp::Put),
        2 => Ok(MutationOp::Delete),
        tag => Err(MetadataRaftError::Backend(format!(
            "unknown mutation op tag {tag}"
        ))),
    }
}
