use nokv_metastore as metastore;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::versions::apply_committed;
use crate::HoltMetadataStore;

impl metastore::MetadataEngine for HoltMetadataStore {
    fn get_metadata(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> metastore::Result<metadatapb::MetadataGetResponse> {
        let _guard = self.lock()?;
        Ok(match self.read_committed(&req.key, req.version)? {
            Some((_commit, value)) => {
                if metastore::value_is_expired(value.expires_at) {
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
    ) -> metastore::Result<metadatapb::MetadataBatchGetResponse> {
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
    ) -> metastore::Result<metadatapb::MetadataScanResponse> {
        let _guard = self.lock()?;
        let read_version = metastore::scan_read_version(req.version);
        let limit = metastore::scan_limit(req.limit);
        let mut kvs = Vec::new();
        let mut keys = self.scan_write_user_keys()?;
        if req.reverse {
            keys.reverse();
        }
        for key in keys {
            if !metastore::scan_key_matches_start(
                &key,
                &req.start_key,
                req.include_start,
                req.reverse,
            ) {
                continue;
            }
            if let Some((_commit_version, value)) = self.read_committed(&key, read_version)? {
                if metastore::value_is_expired(value.expires_at) {
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
    ) -> metastore::Result<metastore::MetadataApplyResult> {
        let _guard = self.lock()?;
        if let Some(error) =
            metastore::validation::metadata_commit_version(command.read_version, commit_version)
        {
            return Ok(metadata_apply_error(commit_version, error));
        }
        if self.metadata_already_applied(command, commit_version)? {
            return Ok(metastore::MetadataApplyResult {
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
                .read_committed(&predicate.key, read_version)?
                .and_then(|(_, value)| value.value);
            if let Some(error) = metastore::validation::metadata_command_predicate_observation(
                predicate,
                observed.as_deref(),
            ) {
                return Ok(metadata_apply_error(commit_version, error));
            }
        }
        for mutation in &command.mutations {
            if let Some(error) = metastore::validation::metadata_command_mutation(mutation) {
                return Ok(metadata_apply_error(commit_version, error));
            }
            if let Some((commit_ts, _)) =
                self.first_write_after_or_at(&mutation.key, command.read_version)?
            {
                return Ok(metadata_apply_error(
                    commit_version,
                    metastore::errors::metadata_revision_conflict(
                        &mutation.key,
                        commit_ts,
                        command.read_version,
                    ),
                ));
            }
            if mutation.assertion_not_exist
                && self
                    .read_committed(&mutation.key, command.read_version)?
                    .and_then(|(_, value)| value.value)
                    .is_some()
            {
                return Ok(metadata_apply_error(
                    commit_version,
                    metastore::errors::metadata_already_exists(&mutation.key),
                ));
            }
        }
        let values = command
            .mutations
            .iter()
            .map(|mutation| {
                (
                    mutation.key.clone(),
                    metastore::metadata_mutation_value(mutation, command.read_version),
                )
            })
            .collect::<Vec<_>>();
        self.atomic(|batch| {
            for (key, value) in &values {
                apply_committed(batch, key, commit_version, value);
            }
        })?;
        Ok(metastore::MetadataApplyResult {
            commit_version,
            applied_mutations: command.mutations.len() as u64,
            error: None,
        })
    }
}

impl HoltMetadataStore {
    fn metadata_already_applied(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
    ) -> metastore::Result<bool> {
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
                || !metastore::metadata_mutation_matches_value(mutation, &value)
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
) -> metastore::MetadataApplyResult {
    metastore::MetadataApplyResult {
        commit_version,
        applied_mutations: 0,
        error: Some(error),
    }
}
