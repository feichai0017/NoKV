use nokv_metadata_state::MetadataEngine;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use std::time::Instant;

use crate::metadata_wire::{metadata_command_watch_events, metadata_command_watch_keys};

use super::{
    invalid_raft_command, AppliedMetadataEngine, MetadataCommandExecutor, MetadataReadExecutor,
    PersistentAppliedMetadataEngine, RegionMetadataSink,
};
use crate::metrics;

impl<E> AppliedMetadataEngine<E>
where
    E: MetadataEngine,
{
    pub(super) fn execute_metadata_command_at(
        &self,
        req: &metadatapb::MetadataCommitRequest,
        forced_status: Option<(u64, u64)>,
    ) -> nokv_metadata_state::Result<metadatapb::MetadataCommitResponse> {
        let mut responses =
            self.execute_metadata_commands_at(std::slice::from_ref(req), forced_status)?;
        Ok(responses.remove(0))
    }

    pub(super) fn execute_metadata_commands_at(
        &self,
        reqs: &[metadatapb::MetadataCommitRequest],
        forced_status: Option<(u64, u64)>,
    ) -> nokv_metadata_state::Result<Vec<metadatapb::MetadataCommitResponse>> {
        if reqs.is_empty() {
            return Ok(Vec::new());
        }
        let mut commands = Vec::with_capacity(reqs.len());
        let mut commit_versions = Vec::with_capacity(reqs.len());
        for req in reqs {
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
            commands.push(command.clone());
            commit_versions.push(commit_version);
        }
        let apply_started = Instant::now();
        let results = {
            let engine = self.inner.engine.lock().map_err(|_| {
                nokv_metadata_state::Error::Backend("region apply mutex poisoned".to_owned())
            })?;
            engine.commit_metadata_batch(&commands, &commit_versions)?
        };
        metrics::record_metadata_apply(commands.len() as u64, apply_started.elapsed());
        if results.len() != commands.len() {
            return Err(nokv_metadata_state::Error::Backend(
                "metadata command batch response length mismatch".to_owned(),
            ));
        }

        let should_advance = forced_status.is_some()
            || commands
                .iter()
                .zip(&results)
                .any(|(command, result)| result.error.is_none() && !command.mutations.is_empty());
        let applied_status = if let Some((term, index)) = forced_status {
            self.record_applied_status(term, index);
            Some((term, index))
        } else if should_advance {
            Some(self.advance_apply_index())
        } else {
            None
        };
        let mut watch_keys = Vec::new();
        let mut watch_events = Vec::new();
        let mut watch_commit_version = 0;
        let mut responses = Vec::with_capacity(commands.len());
        for ((command, commit_version), result) in commands
            .iter()
            .zip(commit_versions.iter().copied())
            .zip(results.into_iter())
        {
            if result.error.is_none() {
                watch_commit_version = watch_commit_version.max(commit_version);
                for key in metadata_command_watch_keys(command) {
                    if !watch_keys.contains(&key) {
                        watch_keys.push(key);
                    }
                }
                for event in metadata_command_watch_events(command) {
                    if !watch_events
                        .iter()
                        .any(|existing: &metadatapb::MetadataWatchEvent| {
                            existing.key == event.key && existing.operation == event.operation
                        })
                    {
                        watch_events.push(event);
                    }
                }
            }
            responses.push(metadatapb::MetadataCommitResponse {
                result: Some(metadatapb::MetadataCommitResult {
                    commit_version,
                    region_id: self.inner.region_id,
                    term: applied_status.map(|(term, _)| term).unwrap_or_default(),
                    index: applied_status.map(|(_, index)| index).unwrap_or_default(),
                    applied_mutations: if result.error.is_none() {
                        result.applied_mutations
                    } else {
                        0
                    },
                }),
                error: result.error,
                region_error: None,
            });
        }
        if let Some((term, index)) = applied_status {
            if !watch_keys.is_empty() {
                self.publish_apply(
                    index,
                    term,
                    metadatapb::MetadataApplyWatchEventSource::Commit,
                    watch_commit_version,
                    watch_keys,
                    watch_events,
                    true,
                );
            }
        }
        Ok(responses)
    }

    fn execute_metadata_command_inner(
        &self,
        req: &metadatapb::MetadataCommitRequest,
    ) -> nokv_metadata_state::Result<metadatapb::MetadataCommitResponse> {
        self.execute_metadata_command_at(req, None)
    }

    fn execute_metadata_get_inner(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> nokv_metadata_state::Result<metadatapb::MetadataGetResponse> {
        let response = self.read(|engine| engine.get_metadata(req))?;
        self.inner.traffic.record_read(1);
        Ok(response)
    }

    fn execute_metadata_batch_get_inner(
        &self,
        req: &metadatapb::MetadataBatchGetRequest,
    ) -> nokv_metadata_state::Result<metadatapb::MetadataBatchGetResponse> {
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
    ) -> nokv_metadata_state::Result<metadatapb::MetadataScanResponse> {
        let response = self.read(|engine| engine.scan_metadata(req))?;
        self.inner.traffic.record_read(1);
        Ok(response)
    }

    fn read<T>(
        &self,
        f: impl FnOnce(&E) -> nokv_metadata_state::Result<T>,
    ) -> nokv_metadata_state::Result<T> {
        let engine = self.inner.engine.lock().map_err(|_| {
            nokv_metadata_state::Error::Backend("region apply mutex poisoned".to_owned())
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
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataCommitResponse>,
    > + Send
           + 'a {
        async move { self.execute_metadata_command_inner(req) }
    }

    fn execute_metadata_commands<'a>(
        &'a self,
        reqs: &'a [metadatapb::MetadataCommitRequest],
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<Vec<metadatapb::MetadataCommitResponse>>,
    > + Send
           + 'a {
        async move { self.execute_metadata_commands_at(reqs, None) }
    }
}

impl<E> MetadataReadExecutor for AppliedMetadataEngine<E>
where
    E: MetadataEngine,
{
    fn execute_metadata_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataGetRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataGetResponse>,
    > + Send
           + 'a {
        async move { self.execute_metadata_get_inner(req) }
    }

    fn execute_metadata_batch_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataBatchGetRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataBatchGetResponse>,
    > + Send
           + 'a {
        async move { self.execute_metadata_batch_get_inner(req) }
    }

    fn execute_metadata_scan<'a>(
        &'a self,
        req: &'a metadatapb::MetadataScanRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataScanResponse>,
    > + Send
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
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataCommitResponse>,
    > + Send
           + 'a {
        async move {
            let before = self.engine.status().applied_index;
            let response = self.engine.execute_metadata_command(req).await?;
            self.persist_if_advanced(before, false)?;
            Ok(response)
        }
    }

    fn execute_metadata_commands<'a>(
        &'a self,
        reqs: &'a [metadatapb::MetadataCommitRequest],
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<Vec<metadatapb::MetadataCommitResponse>>,
    > + Send
           + 'a {
        async move {
            let before = self.engine.status().applied_index;
            let responses = self.engine.execute_metadata_commands(reqs).await?;
            self.persist_if_advanced(before, false)?;
            Ok(responses)
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
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataGetResponse>,
    > + Send
           + 'a {
        async move { self.engine.execute_metadata_get(req).await }
    }

    fn execute_metadata_batch_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataBatchGetRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataBatchGetResponse>,
    > + Send
           + 'a {
        async move { self.engine.execute_metadata_batch_get(req).await }
    }

    fn execute_metadata_scan<'a>(
        &'a self,
        req: &'a metadatapb::MetadataScanRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataScanResponse>,
    > + Send
           + 'a {
        async move { self.engine.execute_metadata_scan(req).await }
    }
}
