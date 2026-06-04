use super::*;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn watch_subtree(&self, scope: InodeId) -> Result<WatchCursor, MetadError> {
        let Some(attr) = self.get_attr(scope)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        Ok(WatchCursor {
            version: self.read_version()?.get(),
            event_id: u64::MAX,
        })
    }

    pub fn replay_watch(
        &self,
        scope: InodeId,
        after: WatchCursor,
        limit: usize,
    ) -> Result<Vec<WatchRecord>, MetadError> {
        let version = self.read_version()?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Watch,
            prefix: watch_log_prefix(self.mount, scope),
            version,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        let mut out = Vec::new();
        for row in rows {
            let cursor = watch_cursor_from_key(&row.key)?;
            if cursor <= after {
                continue;
            }
            out.push(WatchRecord {
                cursor,
                event: decode_watch_event(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?,
            });
            if limit != 0 && out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    pub(super) fn watch_projection(&self, scope: InodeId, event: WatchEvent) -> WatchProjection {
        WatchProjection {
            family: RecordFamily::Watch,
            key: watch_log_prefix(self.mount, scope),
            event: encode_watch_event(&event),
        }
    }
}
