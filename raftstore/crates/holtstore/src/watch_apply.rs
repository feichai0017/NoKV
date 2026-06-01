use holt::RangeEntry;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use prost::Message;

use crate::trees::{watch_apply_event_key, watch_apply_region_prefix, watch_apply_retention_key};
use crate::{HoltMetadataStore, Result};

pub const DEFAULT_WATCH_APPLY_REPLAY_LIMIT: usize = 4096;

impl HoltMetadataStore {
    pub fn put_watch_apply_event(&self, event: &metadatapb::MetadataApplyWatchEvent) -> Result<()> {
        let mut bytes = Vec::with_capacity(event.encoded_len());
        event.encode(&mut bytes)?;
        self.store.watch_apply()?.put(
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

    pub fn first_watch_apply_event(
        &self,
        region_id: u64,
    ) -> Result<Option<metadatapb::MetadataApplyWatchEvent>> {
        let prefix = watch_apply_region_prefix(region_id);
        for entry in self.store.watch_apply()?.range().prefix(&prefix) {
            let entry = entry?;
            let RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            return Ok(Some(metadatapb::MetadataApplyWatchEvent::decode(
                value.as_slice(),
            )?));
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
    ) -> Result<Vec<metadatapb::MetadataApplyWatchEvent>> {
        let prefix = watch_apply_region_prefix(region_id);
        let mut out = Vec::new();
        for entry in self.store.watch_apply()?.range().prefix(&prefix) {
            let entry = entry?;
            let RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            let event = metadatapb::MetadataApplyWatchEvent::decode(value.as_slice())?;
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

    pub fn watch_apply_retention_cursor(&self, region_id: u64) -> Result<Option<(u64, u64, u64)>> {
        let Some(bytes) = self
            .store
            .region_meta()?
            .get(&watch_apply_retention_key(region_id))?
        else {
            return Ok(None);
        };
        if bytes.len() != 24 {
            return Ok(None);
        }
        let term = u64::from_be_bytes(bytes[0..8].try_into().expect("cursor term length"));
        let index = u64::from_be_bytes(bytes[8..16].try_into().expect("cursor index length"));
        let commit_version =
            u64::from_be_bytes(bytes[16..24].try_into().expect("cursor version length"));
        Ok(Some((term, index, commit_version)))
    }
}

pub(crate) fn encode_watch_apply_retention_cursor(
    term: u64,
    index: u64,
    commit_version: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&term.to_be_bytes());
    out.extend_from_slice(&index.to_be_bytes());
    out.extend_from_slice(&commit_version.to_be_bytes());
    out
}

fn apply_event_matches_prefix(event: &metadatapb::MetadataApplyWatchEvent, prefix: &[u8]) -> bool {
    if prefix.is_empty() {
        return true;
    }
    event.keys.iter().any(|key| key.starts_with(prefix))
}

fn apply_event_after_cursor(
    event: &metadatapb::MetadataApplyWatchEvent,
    term: u64,
    index: u64,
) -> bool {
    if term == 0 {
        return event.index > index;
    }
    event.term > term || (event.term == term && event.index > index)
}
