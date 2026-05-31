use nokv_mvcc as mvcc;
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::mvcc_engine::apply_committed;
use crate::HoltMvccStore;

impl mvcc::MetadataEngine for HoltMvccStore {
    fn get_metadata(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> mvcc::Result<metadatapb::MetadataGetResponse> {
        Ok(mvcc::metadata_get_response_from_kv(mvcc::KvEngine::get(
            self,
            &kvpb::GetRequest {
                key: req.key.clone(),
                version: req.version,
            },
        )?))
    }

    fn batch_get_metadata(
        &self,
        req: &metadatapb::MetadataBatchGetRequest,
    ) -> mvcc::Result<metadatapb::MetadataBatchGetResponse> {
        if req.requests.is_empty() {
            return Ok(metadatapb::MetadataBatchGetResponse::default());
        }
        let response = mvcc::KvEngine::batch_get(
            self,
            &kvpb::BatchGetRequest {
                requests: req
                    .requests
                    .iter()
                    .map(|request| kvpb::GetRequest {
                        key: request.key.clone(),
                        version: request.version,
                    })
                    .collect(),
            },
        )?;
        Ok(metadatapb::MetadataBatchGetResponse {
            responses: response
                .responses
                .into_iter()
                .map(mvcc::metadata_get_response_from_kv)
                .collect(),
            region_error: None,
        })
    }

    fn scan_metadata(
        &self,
        req: &metadatapb::MetadataScanRequest,
    ) -> mvcc::Result<metadatapb::MetadataScanResponse> {
        if req.reverse {
            return Err(mvcc::Error::Backend(
                "metadata reverse scans are not supported".to_owned(),
            ));
        }
        Ok(mvcc::metadata_scan_response_from_kv(mvcc::KvEngine::scan(
            self,
            &kvpb::ScanRequest {
                start_key: req.start_key.clone(),
                limit: req.limit,
                version: req.version,
                include_start: req.include_start,
                reverse: false,
            },
        )?))
    }

    fn commit_metadata(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
    ) -> mvcc::Result<mvcc::MetadataApplyResult> {
        let _guard = self.lock()?;
        if let Some(error) = mvcc::validation::commit_version(command.read_version, commit_version)
        {
            return Ok(metadata_apply_error(commit_version, error));
        }
        if self.metadata_already_applied(command, commit_version)? {
            return Ok(mvcc::MetadataApplyResult {
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
            if let Some(lock) = self.get_lock(&predicate.key)? {
                if lock.start_version <= read_version {
                    return Ok(metadata_apply_error(
                        commit_version,
                        mvcc::errors::locked(&predicate.key, &lock),
                    ));
                }
            }
            let observed = self
                .read_committed(&predicate.key, read_version)?
                .and_then(|(_, value)| value.value);
            if let Some(error) =
                mvcc::validation::metadata_predicate_observation(predicate, observed.as_deref())
            {
                return Ok(metadata_apply_error(commit_version, error));
            }
        }
        let primary = command
            .mutations
            .first()
            .map(|mutation| mutation.key.as_slice())
            .unwrap_or_default();
        for mutation in &command.mutations {
            if let Some(error) = mvcc::validation::metadata_mutation(mutation) {
                return Ok(metadata_apply_error(commit_version, error));
            }
            if let Some(lock) = self.get_lock(&mutation.key)? {
                return Ok(metadata_apply_error(
                    commit_version,
                    mvcc::errors::locked(&mutation.key, &lock),
                ));
            }
            if let Some((commit_ts, value)) =
                self.first_write_after_or_at(&mutation.key, command.read_version)?
            {
                return Ok(metadata_apply_error(
                    commit_version,
                    mvcc::errors::write_conflict(
                        &mutation.key,
                        primary,
                        commit_ts,
                        value.start_version,
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
                    mvcc::errors::already_exists(&mutation.key),
                ));
            }
        }
        let values = command
            .mutations
            .iter()
            .map(|mutation| {
                (
                    mutation.key.clone(),
                    mvcc::metadata_mutation_value(mutation, command.read_version),
                )
            })
            .collect::<Vec<_>>();
        self.atomic(|batch| {
            for (key, value) in &values {
                apply_committed(batch, key, commit_version, value);
            }
        })?;
        Ok(mvcc::MetadataApplyResult {
            commit_version,
            applied_mutations: command.mutations.len() as u64,
            error: None,
        })
    }
}

impl HoltMvccStore {
    fn metadata_already_applied(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
    ) -> mvcc::Result<bool> {
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
                || !mvcc::metadata_mutation_matches_value(mutation, &value)
            {
                return Ok(false);
            }
        }
        Ok(any_present && all_present)
    }
}

fn metadata_apply_error(commit_version: u64, error: kvpb::KeyError) -> mvcc::MetadataApplyResult {
    mvcc::MetadataApplyResult {
        commit_version,
        applied_mutations: 0,
        error: Some(error),
    }
}
