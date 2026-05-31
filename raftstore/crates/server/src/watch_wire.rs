use crate::{
    DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE, DEFAULT_APPLY_WATCH_MAX_KEY_BYTES_PER_MESSAGE,
};

pub(crate) fn matching_apply_watch_keys(keys: &[Vec<u8>], prefix: &[u8]) -> Vec<Vec<u8>> {
    keys.iter()
        .filter(|key| prefix.is_empty() || key.starts_with(prefix))
        .cloned()
        .collect()
}

pub(crate) fn chunk_apply_watch_keys(keys: Vec<Vec<u8>>) -> Vec<Vec<Vec<u8>>> {
    if keys.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::with_capacity(
        (keys.len() + DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE - 1)
            / DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE,
    );
    let mut current = Vec::with_capacity(keys.len().min(DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE));
    let mut current_bytes = 0usize;
    for key in keys {
        let key_bytes = key.len();
        if !current.is_empty()
            && (current.len() >= DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE
                || current_bytes + key_bytes > DEFAULT_APPLY_WATCH_MAX_KEY_BYTES_PER_MESSAGE)
        {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += key_bytes;
        current.push(key);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}
