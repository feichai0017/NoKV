use holt::RangeEntry;
use nokv_proto::nokv::kv::v1 as kvpb;
use prost::Message;

use crate::trees::{watch_apply_event_key, watch_apply_region_prefix};
use crate::{HoltMvccStore, HoltStore, Result};

pub const DEFAULT_WATCH_APPLY_REPLAY_LIMIT: usize = 4096;

impl HoltStore {
    pub fn put_watch_apply_event(&self, event: &kvpb::ApplyWatchEvent) -> Result<()> {
        let mut bytes = Vec::with_capacity(event.encoded_len());
        event.encode(&mut bytes)?;
        self.watch_apply()?.put(
            &watch_apply_event_key(
                event.region_id,
                event.term,
                event.index,
                event.commit_version,
                &bytes,
            ),
            &bytes,
        )?;
        Ok(())
    }

    pub fn first_watch_apply_event(&self, region_id: u64) -> Result<Option<kvpb::ApplyWatchEvent>> {
        let prefix = watch_apply_region_prefix(region_id);
        for entry in self.watch_apply()?.range().prefix(&prefix) {
            let entry = entry?;
            let RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            return Ok(Some(kvpb::ApplyWatchEvent::decode(value.as_slice())?));
        }
        Ok(None)
    }

    pub fn watch_apply_events_after(
        &self,
        region_id: u64,
        term: u64,
        index: u64,
        key_prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<kvpb::ApplyWatchEvent>> {
        let prefix = watch_apply_region_prefix(region_id);
        let mut out = Vec::new();
        for entry in self.watch_apply()?.range().prefix(&prefix) {
            let entry = entry?;
            let RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            let event = kvpb::ApplyWatchEvent::decode(value.as_slice())?;
            if !apply_event_after_cursor(&event, term, index) {
                continue;
            }
            if !apply_event_matches_prefix(&event, key_prefix) {
                continue;
            }
            out.push(event);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}

impl HoltMvccStore {
    pub fn put_watch_apply_event(&self, event: &kvpb::ApplyWatchEvent) -> Result<()> {
        self.store.put_watch_apply_event(event)
    }

    pub fn first_watch_apply_event(&self, region_id: u64) -> Result<Option<kvpb::ApplyWatchEvent>> {
        self.store.first_watch_apply_event(region_id)
    }

    pub fn watch_apply_events_after(
        &self,
        region_id: u64,
        term: u64,
        index: u64,
        key_prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<kvpb::ApplyWatchEvent>> {
        self.store
            .watch_apply_events_after(region_id, term, index, key_prefix, limit)
    }
}

fn apply_event_matches_prefix(event: &kvpb::ApplyWatchEvent, prefix: &[u8]) -> bool {
    if prefix.is_empty() {
        return true;
    }
    event.keys.iter().any(|key| key.starts_with(prefix))
}

fn apply_event_after_cursor(event: &kvpb::ApplyWatchEvent, term: u64, index: u64) -> bool {
    if term == 0 {
        return event.index > index;
    }
    event.term > term || (event.term == term && event.index > index)
}
