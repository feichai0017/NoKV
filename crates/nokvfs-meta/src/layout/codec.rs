use std::fmt;

use nokvfs_types::{
    BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName, DentryProjection, DentryRecord,
    FileType, InodeAttr, InodeId, ObjectGcRecord,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodecError {
    Truncated,
    InvalidFileType(u8),
    InvalidInodeId(u64),
    InvalidName(String),
    InvalidUtf8,
    TrailingBytes,
}

pub fn encode_allocator_state(last_commit_version: u64, next_inode: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    push_u64(&mut out, last_commit_version);
    push_u64(&mut out, next_inode);
    out
}

pub fn decode_allocator_state(bytes: &[u8]) -> Result<(u64, u64), CodecError> {
    let mut input = Decoder::new(bytes);
    let state = (input.u64()?, input.u64()?);
    input.finish()?;
    Ok(state)
}

pub fn encode_inode_attr(attr: &InodeAttr) -> Vec<u8> {
    let mut out = Vec::with_capacity(61);
    push_u64(&mut out, attr.inode.get());
    out.push(file_type_tag(attr.file_type));
    push_u32(&mut out, attr.mode);
    push_u32(&mut out, attr.uid);
    push_u32(&mut out, attr.gid);
    push_u64(&mut out, attr.size);
    push_u64(&mut out, attr.generation);
    push_u64(&mut out, attr.mtime_ms);
    push_u64(&mut out, attr.ctime_ms);
    out
}

pub fn decode_inode_attr(bytes: &[u8]) -> Result<InodeAttr, CodecError> {
    let mut input = Decoder::new(bytes);
    let attr = decode_inode_attr_from(&mut input)?;
    input.finish()?;
    Ok(attr)
}

pub fn encode_dentry_projection(projection: &DentryProjection) -> Vec<u8> {
    let mut out = Vec::new();
    push_u64(&mut out, projection.dentry.parent.get());
    put_bytes(&mut out, projection.dentry.name.as_bytes());
    push_u64(&mut out, projection.dentry.child.get());
    out.push(file_type_tag(projection.dentry.child_type));
    push_u64(&mut out, projection.dentry.attr_generation);
    put_bytes(&mut out, &encode_inode_attr(&projection.attr));
    match &projection.body {
        Some(body) => {
            out.push(1);
            put_string(&mut out, &body.producer);
            put_string(&mut out, &body.digest_uri);
            push_u64(&mut out, body.size);
            put_string(&mut out, &body.content_type);
            put_string(&mut out, &body.manifest_id);
            push_u64(&mut out, body.generation);
            push_u64(&mut out, body.chunk_size);
            push_u64(&mut out, body.block_size);
        }
        None => out.push(0),
    }
    out
}

pub fn decode_dentry_projection(bytes: &[u8]) -> Result<DentryProjection, CodecError> {
    let mut input = Decoder::new(bytes);
    let parent = inode(input.u64()?)?;
    let name = DentryName::new(input.bytes()?.to_vec())
        .map_err(|err| CodecError::InvalidName(err.to_string()))?;
    let child = inode(input.u64()?)?;
    let child_type = file_type(input.u8()?)?;
    let attr_generation = input.u64()?;
    let attr_bytes = input.bytes()?;
    let attr = decode_inode_attr(attr_bytes)?;
    let body = match input.u8()? {
        0 => None,
        1 => Some(BodyDescriptor {
            producer: input.string()?,
            digest_uri: input.string()?,
            size: input.u64()?,
            content_type: input.string()?,
            manifest_id: input.string()?,
            generation: input.u64()?,
            chunk_size: input.u64()?,
            block_size: input.u64()?,
        }),
        tag => return Err(CodecError::InvalidFileType(tag)),
    };
    input.finish()?;
    Ok(DentryProjection {
        dentry: DentryRecord {
            parent,
            name,
            child,
            child_type,
            attr_generation,
        },
        attr,
        body,
    })
}

pub fn encode_body_descriptor(body: &BodyDescriptor) -> Vec<u8> {
    let mut out = Vec::new();
    put_string(&mut out, &body.producer);
    put_string(&mut out, &body.digest_uri);
    push_u64(&mut out, body.size);
    put_string(&mut out, &body.content_type);
    put_string(&mut out, &body.manifest_id);
    push_u64(&mut out, body.generation);
    push_u64(&mut out, body.chunk_size);
    push_u64(&mut out, body.block_size);
    out
}

pub fn decode_body_descriptor(bytes: &[u8]) -> Result<BodyDescriptor, CodecError> {
    let mut input = Decoder::new(bytes);
    let body = BodyDescriptor {
        producer: input.string()?,
        digest_uri: input.string()?,
        size: input.u64()?,
        content_type: input.string()?,
        manifest_id: input.string()?,
        generation: input.u64()?,
        chunk_size: input.u64()?,
        block_size: input.u64()?,
    };
    input.finish()?;
    Ok(body)
}

pub fn encode_chunk_manifest(manifest: &ChunkManifest) -> Vec<u8> {
    let mut out = Vec::new();
    push_u64(&mut out, manifest.chunk_index);
    push_u64(&mut out, manifest.logical_offset);
    push_u64(&mut out, manifest.len);
    push_u32(&mut out, manifest.blocks.len() as u32);
    for block in &manifest.blocks {
        put_string(&mut out, &block.object_key);
        push_u64(&mut out, block.logical_offset);
        push_u64(&mut out, block.object_offset);
        push_u64(&mut out, block.len);
        put_string(&mut out, &block.digest_uri);
    }
    out
}

pub fn decode_chunk_manifest(bytes: &[u8]) -> Result<ChunkManifest, CodecError> {
    let mut input = Decoder::new(bytes);
    let chunk_index = input.u64()?;
    let logical_offset = input.u64()?;
    let len = input.u64()?;
    let block_count = input.u32()? as usize;
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        blocks.push(BlockDescriptor {
            object_key: input.string()?,
            logical_offset: input.u64()?,
            object_offset: input.u64()?,
            len: input.u64()?,
            digest_uri: input.string()?,
        });
    }
    input.finish()?;
    Ok(ChunkManifest {
        chunk_index,
        logical_offset,
        len,
        blocks,
    })
}

pub fn encode_object_gc_record(record: &ObjectGcRecord) -> Vec<u8> {
    let mut out = Vec::new();
    push_u64(&mut out, record.inode.get());
    push_u64(&mut out, record.generation);
    put_string(&mut out, &record.object_key);
    push_u64(&mut out, record.size);
    put_string(&mut out, &record.digest_uri);
    push_u64(&mut out, record.enqueue_version);
    out
}

pub fn decode_object_gc_record(bytes: &[u8]) -> Result<ObjectGcRecord, CodecError> {
    let mut input = Decoder::new(bytes);
    let record = ObjectGcRecord {
        inode: inode(input.u64()?)?,
        generation: input.u64()?,
        object_key: input.string()?,
        size: input.u64()?,
        digest_uri: input.string()?,
        enqueue_version: input.u64()?,
    };
    input.finish()?;
    Ok(record)
}

fn decode_inode_attr_from(input: &mut Decoder<'_>) -> Result<InodeAttr, CodecError> {
    Ok(InodeAttr {
        inode: inode(input.u64()?)?,
        file_type: file_type(input.u8()?)?,
        mode: input.u32()?,
        uid: input.u32()?,
        gid: input.u32()?,
        size: input.u64()?,
        generation: input.u64()?,
        mtime_ms: input.u64()?,
        ctime_ms: input.u64()?,
    })
}

fn file_type_tag(file_type: FileType) -> u8 {
    match file_type {
        FileType::File => 1,
        FileType::Directory => 2,
        FileType::Symlink => 3,
    }
}

fn file_type(tag: u8) -> Result<FileType, CodecError> {
    match tag {
        1 => Ok(FileType::File),
        2 => Ok(FileType::Directory),
        3 => Ok(FileType::Symlink),
        _ => Err(CodecError::InvalidFileType(tag)),
    }
}

fn inode(raw: u64) -> Result<InodeId, CodecError> {
    InodeId::new(raw).map_err(|_| CodecError::InvalidInodeId(raw))
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        if self.offset >= self.bytes.len() {
            return Err(CodecError::Truncated);
        }
        let value = self.bytes[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<&'a [u8], CodecError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn string(&mut self) -> Result<String, CodecError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| CodecError::InvalidUtf8)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self.offset.checked_add(len).ok_or(CodecError::Truncated)?;
        if end > self.bytes.len() {
            return Err(CodecError::Truncated);
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn finish(self) -> Result<(), CodecError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes)
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "encoded metadata value is truncated"),
            Self::InvalidFileType(tag) => write!(f, "invalid file type tag {tag}"),
            Self::InvalidInodeId(id) => write!(f, "invalid inode id {id}"),
            Self::InvalidName(err) => write!(f, "invalid dentry name: {err}"),
            Self::InvalidUtf8 => write!(f, "encoded metadata string is not UTF-8"),
            Self::TrailingBytes => write!(f, "encoded metadata value has trailing bytes"),
        }
    }
}

impl std::error::Error for CodecError {}
