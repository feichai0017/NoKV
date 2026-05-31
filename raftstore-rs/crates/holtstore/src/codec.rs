use nokv_mvcc as mvcc;
use nokv_proto::nokv::kv::v1 as kvpb;

use crate::{Error, RegionApplyState, Result};

pub(crate) fn encode_value(value: &mvcc::VersionedValue) -> Vec<u8> {
    let bytes = value.value.as_deref().unwrap_or_default();
    let mut out = Vec::with_capacity(1 + 4 + 8 + 8 + 4 + bytes.len());
    out.push(1);
    out.extend_from_slice(&(value.kind as i32).to_be_bytes());
    out.extend_from_slice(&value.start_version.to_be_bytes());
    out.extend_from_slice(&value.expires_at.to_be_bytes());
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

pub(crate) fn decode_value(bytes: &[u8]) -> mvcc::Result<mvcc::VersionedValue> {
    if bytes.len() < 25 {
        return Err(mvcc::Error::Decode("short mvcc value".to_owned()));
    }
    if bytes[0] != 1 {
        return Err(mvcc::Error::Decode(
            "unsupported mvcc value version".to_owned(),
        ));
    }
    let kind_raw = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
    let start_version = u64::from_be_bytes(bytes[5..13].try_into().unwrap());
    let expires_at = u64::from_be_bytes(bytes[13..21].try_into().unwrap());
    let len = u32::from_be_bytes(bytes[21..25].try_into().unwrap()) as usize;
    if bytes.len() != 25 + len {
        return Err(mvcc::Error::Decode("invalid mvcc value length".to_owned()));
    }
    let kind = kvpb::mutation::Op::try_from(kind_raw).unwrap_or(kvpb::mutation::Op::Put);
    Ok(mvcc::VersionedValue {
        kind,
        start_version,
        value: (kind == kvpb::mutation::Op::Put || kind == kvpb::mutation::Op::Lock)
            .then(|| bytes[25..].to_vec()),
        expires_at,
    })
}

pub(crate) fn encode_lock(lock: &mvcc::LockRecord) -> mvcc::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(4 + lock.primary.len() + 8 * 4 + 4 + 4 + lock.value.len());
    out.extend_from_slice(&(lock.primary.len() as u32).to_be_bytes());
    out.extend_from_slice(&lock.primary);
    out.extend_from_slice(&lock.start_version.to_be_bytes());
    out.extend_from_slice(&lock.start_time.to_be_bytes());
    out.extend_from_slice(&lock.ttl.to_be_bytes());
    out.extend_from_slice(&lock.min_commit_ts.to_be_bytes());
    out.extend_from_slice(&(lock.op as i32).to_be_bytes());
    out.extend_from_slice(&lock.expires_at.to_be_bytes());
    out.extend_from_slice(&(lock.value.len() as u32).to_be_bytes());
    out.extend_from_slice(&lock.value);
    Ok(out)
}

pub(crate) fn decode_lock(bytes: &[u8]) -> mvcc::Result<mvcc::LockRecord> {
    let mut cursor = Cursor::new(bytes);
    let primary = cursor.read_vec()?;
    let start_version = cursor.read_u64()?;
    let start_time = cursor.read_u64()?;
    let ttl = cursor.read_u64()?;
    let min_commit_ts = cursor.read_u64()?;
    let op_raw = cursor.read_i32()?;
    let expires_at = cursor.read_u64()?;
    let value = cursor.read_vec()?;
    if !cursor.done() {
        return Err(mvcc::Error::Decode("trailing lock bytes".to_owned()));
    }
    Ok(mvcc::LockRecord {
        primary,
        start_version,
        start_time,
        ttl,
        min_commit_ts,
        op: kvpb::mutation::Op::try_from(op_raw).unwrap_or(kvpb::mutation::Op::Put),
        value,
        expires_at,
    })
}

pub(crate) fn encode_apply_state(state: &RegionApplyState) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 * 5);
    out.push(1);
    out.extend_from_slice(&state.region_id.to_be_bytes());
    out.extend_from_slice(&state.term.to_be_bytes());
    out.extend_from_slice(&state.applied_index.to_be_bytes());
    out.extend_from_slice(&state.truncated_term.to_be_bytes());
    out.extend_from_slice(&state.truncated_index.to_be_bytes());
    out
}

pub(crate) fn decode_apply_state(bytes: &[u8]) -> Result<RegionApplyState> {
    if bytes.len() != 1 + 8 * 5 {
        return Err(Error::InvalidMetadata(
            "invalid apply state length".to_owned(),
        ));
    }
    if bytes[0] != 1 {
        return Err(Error::InvalidMetadata(
            "unsupported apply state version".to_owned(),
        ));
    }
    Ok(RegionApplyState {
        region_id: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
        term: u64::from_be_bytes(bytes[9..17].try_into().unwrap()),
        applied_index: u64::from_be_bytes(bytes[17..25].try_into().unwrap()),
        truncated_term: u64::from_be_bytes(bytes[25..33].try_into().unwrap()),
        truncated_index: u64::from_be_bytes(bytes[33..41].try_into().unwrap()),
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_vec(&mut self) -> mvcc::Result<Vec<u8>> {
        let len = self.read_u32()? as usize;
        if self.bytes.len().saturating_sub(self.offset) < len {
            return Err(mvcc::Error::Decode("short vector field".to_owned()));
        }
        let out = self.bytes[self.offset..self.offset + len].to_vec();
        self.offset += len;
        Ok(out)
    }

    fn read_u32(&mut self) -> mvcc::Result<u32> {
        if self.bytes.len().saturating_sub(self.offset) < 4 {
            return Err(mvcc::Error::Decode("short u32 field".to_owned()));
        }
        let out = u32::from_be_bytes(self.bytes[self.offset..self.offset + 4].try_into().unwrap());
        self.offset += 4;
        Ok(out)
    }

    fn read_u64(&mut self) -> mvcc::Result<u64> {
        if self.bytes.len().saturating_sub(self.offset) < 8 {
            return Err(mvcc::Error::Decode("short u64 field".to_owned()));
        }
        let out = u64::from_be_bytes(self.bytes[self.offset..self.offset + 8].try_into().unwrap());
        self.offset += 8;
        Ok(out)
    }

    fn read_i32(&mut self) -> mvcc::Result<i32> {
        if self.bytes.len().saturating_sub(self.offset) < 4 {
            return Err(mvcc::Error::Decode("short i32 field".to_owned()));
        }
        let out = i32::from_be_bytes(self.bytes[self.offset..self.offset + 4].try_into().unwrap());
        self.offset += 4;
        Ok(out)
    }

    fn done(&self) -> bool {
        self.offset == self.bytes.len()
    }
}
