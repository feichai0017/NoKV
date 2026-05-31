use nokv_metastore::MetadataEngine;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::metadata::metadata_command_watch_keys;

use super::{
    invalid_raft_command, AppliedMetadataEngine, MetadataCommandExecutor, MetadataReadExecutor,
    PersistentAppliedMetadataEngine, RegionMetadataSink,
};

impl<E> AppliedMetadataEngine<E>
where
    E: MetadataEngine,
{
    pub(super) fn execute_metadata_command_at(
        &self,
        req: &metadatapb::MetadataCommitRequest,
        forced_status: Option<(u64, u64)>,
    ) -> nokv_metastore::Result<metadatapb::MetadataCommitResponse> {
        let command = req
            .command
            .as_ref()
            .ok_or_else(|| invalid_raft_command("metadata command is required"))?;
        if command.read_version == 0 {
            return Err(invalid_raft_command(
                "metadata command read_version is required",
            ));
        }
        let commit_version = if command.commit_version == 0 {
            command.read_version.saturating_add(1)
        } else {
            command.commit_version
        };
        if commit_version <= command.read_version {
            return Err(invalid_raft_command(
                "metadata command commit_version must be greater than read_version",
            ));
        }
        let response = {
            let engine = self.inner.engine.lock().map_err(|_| {
                nokv_metastore::Error::Backend("region apply mutex poisoned".to_owned())
            })?;
            engine.commit_metadata(command, commit_version)?
        };

        let applied_status = if let Some((term, index)) = forced_status {
            self.record_applied_status(term, index);
            Some((term, index))
        } else if !command.mutations.is_empty() {
            Some(self.advance_apply_index())
        } else {
            None
        };
        if response.error.is_none() {
            if let Some((term, index)) = applied_status {
                let watch_keys = metadata_command_watch_keys(command);
                self.publish_apply(
                    index,
                    term,
                    metadatapb::MetadataApplyWatchEventSource::Commit,
                    commit_version,
                    watch_keys,
                    true,
                );
            }
        }
        Ok(metadatapb::MetadataCommitResponse {
            result: Some(metadatapb::MetadataCommitResult {
                commit_version,
                region_id: self.inner.region_id,
                term: applied_status.map(|(term, _)| term).unwrap_or_default(),
                index: applied_status.map(|(_, index)| index).unwrap_or_default(),
                applied_mutations: if response.error.is_none() {
                    response.applied_mutations
                } else {
                    0
                },
            }),
            error: response.error,
            region_error: None,
        })
    }

    fn execute_metadata_command_inner(
        &self,
        req: &metadatapb::MetadataCommitRequest,
    ) -> nokv_metastore::Result<metadatapb::MetadataCommitResponse> {
        self.execute_metadata_command_at(req, None)
    }

    fn execute_metadata_get_inner(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> nokv_metastore::Result<metadatapb::MetadataGetResponse> {
        let response = self.read(|engine| engine.get_metadata(req))?;
        self.inner.traffic.record_read(1);
        Ok(response)
    }

    fn execute_metadata_batch_get_inner(
        &self,
        req: &metadatapb::MetadataBatchGetRequest,
    ) -> nokv_metastore::Result<metadatapb::MetadataBatchGetResponse> {
        if req.requests.is_empty() {
            return Ok(metadatapb::MetadataBatchGetResponse::default());
        }
        let response = self.read(|engine| engine.batch_get_metadata(req))?;
        self.inner.traffic.record_read(req.requests.len() as u64);
        Ok(response)
    }

    fn execute_metadata_scan_inner(
        &self,
        req: &metadatapb::MetadataScanRequest,
    ) -> nokv_metastore::Result<metadatapb::MetadataScanResponse> {
        if req.reverse {
            return Err(invalid_raft_command(
                "metadata reverse scans are not supported",
            ));
        }
        let response = self.read(|engine| engine.scan_metadata(req))?;
        self.inner.traffic.record_read(1);
        Ok(response)
    }

    fn read<T>(
        &self,
        f: impl FnOnce(&E) -> nokv_metastore::Result<T>,
    ) -> nokv_metastore::Result<T> {
        let engine = self.inner.engine.lock().map_err(|_| {
            nokv_metastore::Error::Backend("region apply mutex poisoned".to_owned())
        })?;
        f(&engine)
    }
}

impl<E> MetadataCommandExecutor for AppliedMetadataEngine<E>
where
    E: MetadataEngine,
{
    fn execute_metadata_command<'a>(
        &'a self,
        req: &'a metadatapb::MetadataCommitRequest,
    ) -> impl std::future::Future<Output = nokv_metastore::Result<metadatapb::MetadataCommitResponse>>
           + Send
           + 'a {
        async move { self.execute_metadata_command_inner(req) }
    }
}

impl<E> MetadataReadExecutor for AppliedMetadataEngine<E>
where
    E: MetadataEngine,
{
    fn execute_metadata_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataGetRequest,
    ) -> impl std::future::Future<Output = nokv_metastore::Result<metadatapb::MetadataGetResponse>>
           + Send
           + 'a {
        async move { self.execute_metadata_get_inner(req) }
    }

    fn execute_metadata_batch_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataBatchGetRequest,
    ) -> impl std::future::Future<
        Output = nokv_metastore::Result<metadatapb::MetadataBatchGetResponse>,
    > + Send
           + 'a {
        async move { self.execute_metadata_batch_get_inner(req) }
    }

    fn execute_metadata_scan<'a>(
        &'a self,
        req: &'a metadatapb::MetadataScanRequest,
    ) -> impl std::future::Future<Output = nokv_metastore::Result<metadatapb::MetadataScanResponse>>
           + Send
           + 'a {
        async move { self.execute_metadata_scan_inner(req) }
    }
}

impl<E, S> MetadataCommandExecutor for PersistentAppliedMetadataEngine<E, S>
where
    E: MetadataEngine,
    S: RegionMetadataSink,
{
    fn execute_metadata_command<'a>(
        &'a self,
        req: &'a metadatapb::MetadataCommitRequest,
    ) -> impl std::future::Future<Output = nokv_metastore::Result<metadatapb::MetadataCommitResponse>>
           + Send
           + 'a {
        async move {
            let before = self.engine.status().applied_index;
            let response = self.engine.execute_metadata_command(req).await?;
            self.persist_if_advanced(before)?;
            Ok(response)
        }
    }
}

impl<E, S> MetadataReadExecutor for PersistentAppliedMetadataEngine<E, S>
where
    E: MetadataEngine,
    S: RegionMetadataSink,
{
    fn execute_metadata_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataGetRequest,
    ) -> impl std::future::Future<Output = nokv_metastore::Result<metadatapb::MetadataGetResponse>>
           + Send
           + 'a {
        async move { self.engine.execute_metadata_get(req).await }
    }

    fn execute_metadata_batch_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataBatchGetRequest,
    ) -> impl std::future::Future<
        Output = nokv_metastore::Result<metadatapb::MetadataBatchGetResponse>,
    > + Send
           + 'a {
        async move { self.engine.execute_metadata_batch_get(req).await }
    }

    fn execute_metadata_scan<'a>(
        &'a self,
        req: &'a metadatapb::MetadataScanRequest,
    ) -> impl std::future::Future<Output = nokv_metastore::Result<metadatapb::MetadataScanResponse>>
           + Send
           + 'a {
        async move { self.engine.execute_metadata_scan(req).await }
    }
}
