use nokv_metadata_state as metadata_state;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use std::time::{Duration, Instant};

use crate::metrics;
use crate::versions::apply_committed;
use crate::HoltMetadataStore;

impl metadata_state::MetadataEngine for HoltMetadataStore {
    fn get_metadata(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> metadata_state::Result<metadatapb::MetadataGetResponse> {
        let _guard = self.lock()?;
        Ok(match self.read_committed(&req.key, req.version)? {
            Some((_commit, value)) => {
                if metadata_state::value_is_expired(value.expires_at) {
                    return Ok(metadatapb::MetadataGetResponse {
                        not_found: true,
                        ..Default::default()
                    });
                }
                let expires_at = value.expires_at;
                let bytes = value.value;
                let not_found = bytes.is_none();
                metadatapb::MetadataGetResponse {
                    kv: bytes.map(|value| metadatapb::MetadataKv {
                        value,
                        expires_at,
                        ..Default::default()
                    }),
                    not_found,
                    ..Default::default()
                }
            }
            None => metadatapb::MetadataGetResponse {
                not_found: true,
                ..Default::default()
            },
        })
    }

    fn batch_get_metadata(
        &self,
        req: &metadatapb::MetadataBatchGetRequest,
    ) -> metadata_state::Result<metadatapb::MetadataBatchGetResponse> {
        if req.requests.is_empty() {
            return Ok(metadatapb::MetadataBatchGetResponse::default());
        }
        let mut responses = Vec::with_capacity(req.requests.len());
        for request in &req.requests {
            responses.push(self.get_metadata(request)?);
        }
        Ok(metadatapb::MetadataBatchGetResponse {
            responses,
            region_error: None,
        })
    }

    fn scan_metadata(
        &self,
        req: &metadatapb::MetadataScanRequest,
    ) -> metadata_state::Result<metadatapb::MetadataScanResponse> {
        let _guard = self.lock()?;
        let read_version = metadata_state::scan_read_version(req.version);
        let limit = metadata_state::scan_limit(req.limit);
        let mut kvs = Vec::new();
        let mut keys = self.scan_write_user_keys()?;
        if req.reverse {
            keys.reverse();
        }
        for key in keys {
            if !metadata_state::scan_key_matches_start(
                &key,
                &req.start_key,
                req.include_start,
                req.reverse,
            ) {
                continue;
            }
            if let Some((_commit_version, value)) = self.read_committed(&key, read_version)? {
                if metadata_state::value_is_expired(value.expires_at) {
                    continue;
                }
                let expires_at = value.expires_at;
                if let Some(bytes) = value.value {
                    kvs.push(metadatapb::MetadataKv {
                        key,
                        value: bytes,
                        version: read_version,
                        expires_at,
                    });
                    if kvs.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(metadatapb::MetadataScanResponse {
            kvs,
            ..Default::default()
        })
    }

    fn commit_metadata(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
    ) -> metadata_state::Result<metadata_state::MetadataApplyResult> {
        let mut results =
            self.commit_metadata_batch(std::slice::from_ref(command), &[commit_version])?;
        Ok(results.remove(0))
    }

    fn commit_metadata_batch(
        &self,
        commands: &[metadatapb::MetadataCommand],
        commit_versions: &[u64],
    ) -> metadata_state::Result<Vec<metadata_state::MetadataApplyResult>> {
        if commands.len() != commit_versions.len() {
            return Err(metadata_state::Error::Backend(
                "metadata command batch length mismatch".to_owned(),
            ));
        }
        let started = Instant::now();
        let _guard = self.lock()?;
        let mut results = Vec::with_capacity(commands.len());
        let mut pending = Vec::new();
        let prepare_started = Instant::now();
        for (command, commit_version) in commands.iter().zip(commit_versions) {
            let result = self.prepare_metadata_commit(command, *commit_version, &mut pending)?;
            results.push(result);
        }
        let prepare_duration = prepare_started.elapsed();
        let pending_writes = pending.len() as u64;
        let mut atomic_duration = Duration::ZERO;
        if !pending.is_empty() {
            let atomic_started = Instant::now();
            self.atomic(|batch| {
                for (key, commit_version, value) in &pending {
                    apply_committed(batch, key, *commit_version, value);
                }
            })?;
            atomic_duration = atomic_started.elapsed();
        }
        metrics::record_metadata_commit(
            commands.len() as u64,
            pending_writes,
            prepare_duration,
            atomic_duration,
            started.elapsed(),
        );
        Ok(results)
    }
}

impl HoltMetadataStore {
    fn prepare_metadata_commit(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
        pending: &mut Vec<(Vec<u8>, u64, metadata_state::VersionedValue)>,
    ) -> metadata_state::Result<metadata_state::MetadataApplyResult> {
        if let Some(error) = metadata_state::validation::metadata_commit_version(
            command.read_version,
            commit_version,
        ) {
            return Ok(metadata_apply_error(commit_version, error));
        }
        if self.metadata_already_applied(command, commit_version)? {
            return Ok(metadata_state::MetadataApplyResult {
                commit_version,
                applied_mutations: command.mutations.len() as u64,
                error: None,
            });
        }
        for predicate in &command.predicates {
            let read_version = if predicate.read_version == 0 {
                command.read_version
            } else {
                predicate.read_version
            };
            let observed = self
                .read_committed_with_pending(&predicate.key, read_version, pending)?
                .and_then(|(_, value)| value.value);
            if let Some(error) = metadata_state::validation::metadata_command_predicate_observation(
                predicate,
                observed.as_deref(),
            ) {
                return Ok(metadata_apply_error(commit_version, error));
            }
        }
        for mutation in &command.mutations {
            if let Some(error) = metadata_state::validation::metadata_command_mutation(mutation) {
                return Ok(metadata_apply_error(commit_version, error));
            }
            if let Some((commit_ts, _)) = self.first_write_after_or_at_with_pending(
                &mutation.key,
                command.read_version,
                pending,
            )? {
                return Ok(metadata_apply_error(
                    commit_version,
                    metadata_state::errors::metadata_revision_conflict(
                        &mutation.key,
                        commit_ts,
                        command.read_version,
                    ),
                ));
            }
            if mutation.assertion_not_exist
                && self
                    .read_committed_with_pending(&mutation.key, command.read_version, pending)?
                    .and_then(|(_, value)| value.value)
                    .is_some()
            {
                return Ok(metadata_apply_error(
                    commit_version,
                    metadata_state::errors::metadata_already_exists(&mutation.key),
                ));
            }
        }
        pending.extend(command.mutations.iter().map(|mutation| {
            (
                mutation.key.clone(),
                commit_version,
                metadata_state::metadata_mutation_value(mutation, command.read_version),
            )
        }));
        Ok(metadata_state::MetadataApplyResult {
            commit_version,
            applied_mutations: command.mutations.len() as u64,
            error: None,
        })
    }

    fn read_committed_with_pending(
        &self,
        key: &[u8],
        version: u64,
        pending: &[(Vec<u8>, u64, metadata_state::VersionedValue)],
    ) -> metadata_state::Result<Option<(u64, metadata_state::VersionedValue)>> {
        let mut best = self.read_committed(key, version)?;
        for (pending_key, commit_version, value) in pending {
            if pending_key.as_slice() == key
                && *commit_version <= version
                && best.as_ref().is_none_or(|(ts, _)| *commit_version > *ts)
            {
                best = Some((*commit_version, value.clone()));
            }
        }
        Ok(best)
    }

    fn first_write_after_or_at_with_pending(
        &self,
        key: &[u8],
        version: u64,
        pending: &[(Vec<u8>, u64, metadata_state::VersionedValue)],
    ) -> metadata_state::Result<Option<(u64, metadata_state::VersionedValue)>> {
        let mut best = self.first_write_after_or_at(key, version)?;
        for (pending_key, commit_version, value) in pending {
            if pending_key.as_slice() == key
                && *commit_version >= version
                && best.as_ref().is_none_or(|(ts, _)| *commit_version < *ts)
            {
                best = Some((*commit_version, value.clone()));
            }
        }
        Ok(best)
    }

    fn metadata_already_applied(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
    ) -> metadata_state::Result<bool> {
        let mut any_present = false;
        let mut all_present = true;
        for mutation in &command.mutations {
            let Some((existing_commit, value)) =
                self.write_by_start_version(&mutation.key, command.read_version)?
            else {
                all_present = false;
                continue;
            };
            any_present = true;
            if existing_commit != commit_version
                || !metadata_state::metadata_mutation_matches_value(mutation, &value)
            {
                return Ok(false);
            }
        }
        Ok(any_present && all_present)
    }
}

fn metadata_apply_error(
    commit_version: u64,
    error: metadatapb::MetadataKeyError,
) -> metadata_state::MetadataApplyResult {
    metadata_state::MetadataApplyResult {
        commit_version,
        applied_mutations: 0,
        error: Some(error),
    }
}
