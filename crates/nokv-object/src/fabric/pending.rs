use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use crate::store::{ObjectError, ObjectKey};

pub(super) const LOCAL_HOT_INTERNAL_DIR: &str = ".nokv-internal";
const PENDING_COLD_PUT_DIR: &str = "cold-pending";

pub fn default_pending_cold_put_root(hot_root: &Path) -> PathBuf {
    hot_root
        .join(LOCAL_HOT_INTERNAL_DIR)
        .join(PENDING_COLD_PUT_DIR)
}

pub(super) fn write_pending_cold_put(root: &Path, key: &ObjectKey) -> Result<(), ObjectError> {
    let path = root.join(pending_cold_put_file_name(key));
    match open_pending_cold_put_marker(&path) {
        Ok(()) => return Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(ObjectError::from_backend(err)),
    }

    fs::create_dir_all(root).map_err(ObjectError::from_backend)?;
    open_pending_cold_put_marker(&path).map_err(ObjectError::from_backend)
}

fn open_pending_cold_put_marker(path: &Path) -> io::Result<()> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .map(|_| ())
}

pub(super) fn remove_pending_cold_put(root: &Path, key: &ObjectKey) -> Result<(), ObjectError> {
    match fs::remove_file(root.join(pending_cold_put_file_name(key))) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ObjectError::from_backend(err)),
    }
}

pub(super) fn read_pending_cold_puts(root: &Path) -> Result<Vec<ObjectKey>, ObjectError> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(ObjectError::from_backend(err)),
    };
    let mut keys = Vec::new();
    for entry in entries {
        let entry = entry.map_err(ObjectError::from_backend)?;
        let file_type = entry.file_type().map_err(ObjectError::from_backend)?;
        if !file_type.is_file() {
            continue;
        }
        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        keys.push(ObjectKey::new(hex_decode_to_string(&file_name)?)?);
    }
    Ok(keys)
}

fn pending_cold_put_file_name(key: &ObjectKey) -> String {
    hex_encode(key.as_str().as_bytes())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode_to_string(encoded: &str) -> Result<String, ObjectError> {
    if !encoded.len().is_multiple_of(2) {
        return Err(ObjectError::Backend(
            "pending cold put marker has odd hex key length".to_owned(),
        ));
    }
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    let raw = encoded.as_bytes();
    for pair in raw.chunks_exact(2) {
        let hi = hex_value(pair[0])?;
        let lo = hex_value(pair[1])?;
        bytes.push((hi << 4) | lo);
    }
    String::from_utf8(bytes).map_err(|err| {
        ObjectError::Backend(format!("pending cold put marker key is not utf8: {err}"))
    })
}

fn hex_value(byte: u8) -> Result<u8, ObjectError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ObjectError::Backend(
            "pending cold put marker contains invalid hex key".to_owned(),
        )),
    }
}
