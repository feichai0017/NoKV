use nokv_metadata_state as metadata_state;

use crate::{Error, RegionApplyState, Result};

pub(crate) fn encode_value(value: &metadata_state::VersionedValue) -> Vec<u8> {
    let bytes = value.value.as_deref().unwrap_or_default();
    let mut out = Vec::with_capacity(1 + 4 + 8 + 8 + 8 + 4 + bytes.len());
    out.push(2);
    out.extend_from_slice(&(value.kind as i32).to_be_bytes());
    out.extend_from_slice(&value.start_version.to_be_bytes());
    out.extend_from_slice(&value.expires_at.to_be_bytes());
    out.extend_from_slice(&value.retention_pin_version.to_be_bytes());
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

pub(crate) fn decode_value(bytes: &[u8]) -> metadata_state::Result<metadata_state::VersionedValue> {
    if bytes.len() < 25 {
        return Err(metadata_state::Error::Decode(
            "short metadata value".to_owned(),
        ));
    }
    if bytes[0] != 1 && bytes[0] != 2 {
        return Err(metadata_state::Error::Decode(
            "unsupported metadata value version".to_owned(),
        ));
    }
    let kind_raw = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
    let start_version = u64::from_be_bytes(bytes[5..13].try_into().unwrap());
    let expires_at = u64::from_be_bytes(bytes[13..21].try_into().unwrap());
    let (retention_pin_version, len_pos) = if bytes[0] == 2 {
        if bytes.len() < 33 {
            return Err(metadata_state::Error::Decode(
                "short metadata value".to_owned(),
            ));
        }
        (u64::from_be_bytes(bytes[21..29].try_into().unwrap()), 29)
    } else {
        (0, 21)
    };
    let len = u32::from_be_bytes(bytes[len_pos..len_pos + 4].try_into().unwrap()) as usize;
    if bytes.len() != len_pos + 4 + len {
        return Err(metadata_state::Error::Decode(
            "invalid metadata value length".to_owned(),
        ));
    }
    let kind = metadata_state::ValueKind::from_i32(kind_raw);
    Ok(metadata_state::VersionedValue {
        kind,
        start_version,
        value: (kind == metadata_state::ValueKind::Put).then(|| bytes[len_pos + 4..].to_vec()),
        expires_at,
        retention_pin_version,
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
