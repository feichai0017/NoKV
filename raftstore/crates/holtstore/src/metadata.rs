use holt::RangeEntry;
use nokv_metadata_state as metadata_state;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use prost::Message;
use std::time::{Duration, Instant};

use crate::metrics;
use crate::store::to_backend_error;
use crate::trees::{family_from_i32, METADATA_HISTORY_ACTIVE_KEY, REGION_META_TREE};
use crate::versions::{apply_committed, apply_committed_current_only, decode_current_value};
use crate::HoltMetadataStore;

#[derive(Clone)]
struct PendingMetadataWrite {
    family: metadatapb::MetadataFamily,
    key: Vec<u8>,
    commit_version: u64,
    value: metadata_state::VersionedValue,
    write_history: bool,
}

impl metadata_state::MetadataEngine for HoltMetadataStore {
    fn get_metadata(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> metadata_state::Result<metadatapb::MetadataGetResponse> {
        let family = family_from_i32(req.key_family);
        Ok(match self.read_committed(family, &req.key, req.version)? {
            Some((commit, value)) => {
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
                        key: req.key.clone(),
                        key_family: family as i32,
                        value,
                        version: commit,
                        expires_at,
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
            let family = family_from_i32(request.key_family);
            responses.push(
                match self.read_committed(family, &request.key, request.version)? {
                    Some((commit, value)) => {
                        if metadata_state::value_is_expired(value.expires_at) {
                            metadatapb::MetadataGetResponse {
                                not_found: true,
                                ..Default::default()
                            }
                        } else {
                            let expires_at = value.expires_at;
                            let bytes = value.value;
                            let not_found = bytes.is_none();
                            metadatapb::MetadataGetResponse {
                                kv: bytes.map(|value| metadatapb::MetadataKv {
                                    key: request.key.clone(),
                                    key_family: family as i32,
                                    value,
                                    version: commit,
                                    expires_at,
                                }),
                                not_found,
                                ..Default::default()
                            }
                        }
                    }
                    None => metadatapb::MetadataGetResponse {
                        not_found: true,
                        ..Default::default()
                    },
                },
            );
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
        let family = family_from_i32(req.key_family);
        let read_version = metadata_state::scan_read_version(req.version);
        let limit = metadata_state::scan_limit(req.limit);
        let mut kvs = Vec::new();
        let current = self.store.current(family).map_err(to_backend_error)?;
        if !req.reverse {
            let mut visited = 0_u64;
            if req.prefix_key.is_empty() {
                for entry in current.range() {
                    let entry = entry.map_err(to_backend_error)?;
                    let RangeEntry::Key { key, .. } = entry else {
                        continue;
                    };
                    if self.scan_one_key(
                        req,
                        family,
                        key,
                        read_version,
                        limit,
                        &mut visited,
                        &mut kvs,
                    )? {
                        break;
                    }
                }
            } else {
                for entry in current.range().prefix(&req.prefix_key) {
                    let entry = entry.map_err(to_backend_error)?;
                    let RangeEntry::Key { key, .. } = entry else {
                        continue;
                    };
                    if self.scan_one_key(
                        req,
                        family,
                        key,
                        read_version,
                        limit,
                        &mut visited,
                        &mut kvs,
                    )? {
                        break;
                    }
                }
            }
            metrics::record_metadata_scan(visited, kvs.len() as u64);
            return Ok(metadatapb::MetadataScanResponse {
                kvs,
                ..Default::default()
            });
        }

        let mut keys = Vec::new();
        if req.prefix_key.is_empty() {
            for entry in current.range() {
                let entry = entry.map_err(to_backend_error)?;
                let RangeEntry::Key { key, .. } = entry else {
                    continue;
                };
                keys.push(key);
            }
        } else {
            for entry in current.range().prefix(&req.prefix_key) {
                let entry = entry.map_err(to_backend_error)?;
                let RangeEntry::Key { key, .. } = entry else {
                    continue;
                };
                keys.push(key);
            }
        }
        keys.reverse();
        let mut visited = 0_u64;
        for key in keys {
            if self.scan_one_key(
                req,
                family,
                key,
                read_version,
                limit,
                &mut visited,
                &mut kvs,
            )? {
                break;
            }
        }
        metrics::record_metadata_scan(visited, kvs.len() as u64);
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
        let mut pending: Vec<PendingMetadataWrite> = Vec::new();
        let history_active = self.metadata_history_active_locked()?;
        let mut history_required = history_active;
        let prepare_started = Instant::now();
        for (command, commit_version) in commands.iter().zip(commit_versions) {
            let command_history_required =
                history_required || metadata_command_activates_history(command);
            let result = self.prepare_metadata_commit(
                command,
                *commit_version,
                command_history_required,
                &mut pending,
            )?;
            if command_history_required {
                history_required = true;
            }
            results.push(result);
        }
        let prepare_duration = prepare_started.elapsed();
        let pending_writes = pending.len() as u64;
        let history_writes = pending.iter().filter(|write| write.write_history).count() as u64;
        let mut atomic_duration = Duration::ZERO;
        if !pending.is_empty() {
            let atomic_started = Instant::now();
            self.atomic(|batch| {
                if history_required && !history_active {
                    batch.put(REGION_META_TREE, METADATA_HISTORY_ACTIVE_KEY, b"1");
                }
                for write in &pending {
                    if write.write_history {
                        apply_committed(
                            batch,
                            write.family,
                            &write.key,
                            write.commit_version,
                            &write.value,
                        );
                    } else {
                        apply_committed_current_only(
                            batch,
                            write.family,
                            &write.key,
                            write.commit_version,
                            &write.value,
                        );
                    }
                }
            })?;
            atomic_duration = atomic_started.elapsed();
        }
        metrics::record_metadata_commit(
            commands.len() as u64,
            pending_writes,
            pending_writes,
            history_writes,
            prepare_duration,
            atomic_duration,
            started.elapsed(),
        );
        Ok(results)
    }
}

impl HoltMetadataStore {
    fn scan_one_key(
        &self,
        req: &metadatapb::MetadataScanRequest,
        family: metadatapb::MetadataFamily,
        key: Vec<u8>,
        read_version: u64,
        limit: usize,
        visited: &mut u64,
        kvs: &mut Vec<metadatapb::MetadataKv>,
    ) -> metadata_state::Result<bool> {
        if !metadata_state::scan_key_matches_start(
            &key,
            &req.start_key,
            req.include_start,
            req.reverse,
        ) {
            return Ok(false);
        }
        *visited += 1;
        if let Some((commit_version, value)) = self.read_committed(family, &key, read_version)? {
            if metadata_state::value_is_expired(value.expires_at) {
                return Ok(false);
            }
            let expires_at = value.expires_at;
            if let Some(bytes) = value.value {
                kvs.push(metadatapb::MetadataKv {
                    key,
                    key_family: family as i32,
                    value: bytes,
                    version: commit_version,
                    expires_at,
                });
            }
        }
        Ok(kvs.len() >= limit)
    }

    fn metadata_history_active_locked(&self) -> metadata_state::Result<bool> {
        Ok(self
            .store
            .region_meta()
            .map_err(to_backend_error)?
            .get(METADATA_HISTORY_ACTIVE_KEY)
            .map_err(to_backend_error)?
            .is_some())
    }

    fn prepare_metadata_commit(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
        history_required: bool,
        pending: &mut Vec<PendingMetadataWrite>,
    ) -> metadata_state::Result<metadata_state::MetadataApplyResult> {
        if let Some(error) = metadata_state::validation::metadata_commit_version(
            command.read_version,
            commit_version,
        ) {
            return Ok(metadata_apply_error(commit_version, error));
        }
        if let Some(result) = self.metadata_dedupe_result(command, pending)? {
            return Ok(result);
        }
        for predicate in &command.predicates {
            let read_version = if predicate.read_version == 0 {
                command.read_version
            } else {
                predicate.read_version
            };
            if metadatapb::MetadataPredicateKind::try_from(predicate.kind)
                == Ok(metadatapb::MetadataPredicateKind::PrefixEmpty)
            {
                if !self.prefix_empty_with_pending(
                    family_from_i32(predicate.key_family),
                    &predicate.key,
                    read_version,
                    pending,
                )? {
                    return Ok(metadata_apply_error(
                        commit_version,
                        metadata_state::errors::metadata_prefix_not_empty(),
                    ));
                }
                continue;
            }
            let observed = self
                .read_committed_with_pending(
                    family_from_i32(predicate.key_family),
                    &predicate.key,
                    read_version,
                    pending,
                )?
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
                family_from_i32(mutation.key_family),
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
                    .read_committed_with_pending(
                        family_from_i32(mutation.key_family),
                        &mutation.key,
                        command.read_version,
                        pending,
                    )?
                    .and_then(|(_, value)| value.value)
                    .is_some()
            {
                return Ok(metadata_apply_error(
                    commit_version,
                    metadata_state::errors::metadata_already_exists(&mutation.key),
                ));
            }
        }
        if history_required {
            for mutation in &command.mutations {
                self.preserve_current_for_history(
                    family_from_i32(mutation.key_family),
                    &mutation.key,
                    pending,
                )?;
            }
        }
        pending.extend(
            command
                .mutations
                .iter()
                .map(|mutation| PendingMetadataWrite {
                    family: family_from_i32(mutation.key_family),
                    key: mutation.key.clone(),
                    commit_version,
                    value: metadata_state::metadata_mutation_value(mutation, command.read_version),
                    write_history: history_required,
                }),
        );
        if !command.request_id.is_empty() {
            pending.push(PendingMetadataWrite {
                family: metadatapb::MetadataFamily::CommandDedupe,
                key: command.request_id.clone(),
                commit_version,
                value: metadata_state::VersionedValue {
                    kind: metadata_state::ValueKind::Put,
                    start_version: command.read_version,
                    value: Some(encode_metadata_dedupe_record(
                        command,
                        commit_version,
                        command.mutations.len() as u64,
                    )),
                    expires_at: 0,
                    retention_pin_version: 0,
                },
                write_history: false,
            });
        }
        Ok(metadata_state::MetadataApplyResult {
            commit_version,
            applied_mutations: command.mutations.len() as u64,
            error: None,
        })
    }

    fn preserve_current_for_history(
        &self,
        family: metadatapb::MetadataFamily,
        key: &[u8],
        pending: &mut Vec<PendingMetadataWrite>,
    ) -> metadata_state::Result<()> {
        let Some(raw) = self
            .store
            .current(family)
            .map_err(to_backend_error)?
            .get(key)
            .map_err(to_backend_error)?
        else {
            return Ok(());
        };
        let (commit_version, value) = decode_current_value(&raw)?;
        if pending.iter().any(|write| {
            write.family == family
                && write.key.as_slice() == key
                && write.commit_version == commit_version
        }) {
            return Ok(());
        }
        pending.push(PendingMetadataWrite {
            family,
            key: key.to_vec(),
            commit_version,
            value,
            write_history: true,
        });
        Ok(())
    }

    fn read_committed_with_pending(
        &self,
        family: metadatapb::MetadataFamily,
        key: &[u8],
        version: u64,
        pending: &[PendingMetadataWrite],
    ) -> metadata_state::Result<Option<(u64, metadata_state::VersionedValue)>> {
        let mut best = self.read_committed(family, key, version)?;
        for write in pending {
            if write.family == family
                && write.key.as_slice() == key
                && write.commit_version <= version
                && best
                    .as_ref()
                    .is_none_or(|(ts, _)| write.commit_version > *ts)
            {
                best = Some((write.commit_version, write.value.clone()));
            }
        }
        Ok(best)
    }

    fn prefix_empty_with_pending(
        &self,
        family: metadatapb::MetadataFamily,
        prefix: &[u8],
        version: u64,
        pending: &[PendingMetadataWrite],
    ) -> metadata_state::Result<bool> {
        for entry in self
            .store
            .current(family)
            .map_err(to_backend_error)?
            .range()
            .prefix(prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, .. } = entry else {
                continue;
            };
            if self
                .read_committed_with_pending(family, &key, version, pending)?
                .and_then(|(_, value)| value.value)
                .is_some()
            {
                return Ok(false);
            }
        }
        for write in pending {
            if write.family == family
                && write.key.starts_with(prefix)
                && self
                    .read_committed_with_pending(family, &write.key, version, pending)?
                    .and_then(|(_, value)| value.value)
                    .is_some()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn first_write_after_or_at_with_pending(
        &self,
        family: metadatapb::MetadataFamily,
        key: &[u8],
        version: u64,
        pending: &[PendingMetadataWrite],
    ) -> metadata_state::Result<Option<(u64, metadata_state::VersionedValue)>> {
        let mut best = self.first_write_after_or_at(family, key, version)?;
        for write in pending {
            if write.family == family
                && write.key.as_slice() == key
                && write.commit_version >= version
                && best
                    .as_ref()
                    .is_none_or(|(ts, _)| write.commit_version < *ts)
            {
                best = Some((write.commit_version, write.value.clone()));
            }
        }
        Ok(best)
    }

    fn metadata_dedupe_result(
        &self,
        command: &metadatapb::MetadataCommand,
        pending: &[PendingMetadataWrite],
    ) -> metadata_state::Result<Option<metadata_state::MetadataApplyResult>> {
        if command.request_id.is_empty() {
            return Ok(None);
        }
        for write in pending.iter().rev() {
            if write.family == metadatapb::MetadataFamily::CommandDedupe
                && write.key.as_slice() == command.request_id.as_slice()
            {
                let Some(raw) = write.value.value.as_deref() else {
                    return Ok(None);
                };
                return decode_metadata_dedupe_record(command, raw).map(Some);
            }
        }
        let Some(raw) = self
            .store
            .current(metadatapb::MetadataFamily::CommandDedupe)
            .map_err(to_backend_error)?
            .get(&command.request_id)
            .map_err(to_backend_error)?
        else {
            return Ok(None);
        };
        let (_commit_ts, value) = decode_current_value(&raw)?;
        let Some(record) = value.value else {
            return Ok(None);
        };
        decode_metadata_dedupe_record(command, &record).map(Some)
    }
}

fn metadata_command_activates_history(command: &metadatapb::MetadataCommand) -> bool {
    command
        .mutations
        .iter()
        .any(|mutation| mutation.retention_pin_version != 0)
}

fn encode_metadata_dedupe_record(
    command: &metadatapb::MetadataCommand,
    commit_version: u64,
    applied_mutations: u64,
) -> Vec<u8> {
    let command_bytes = command.encode_to_vec();
    let mut out = Vec::with_capacity(20 + command_bytes.len());
    out.extend_from_slice(&commit_version.to_be_bytes());
    out.extend_from_slice(&applied_mutations.to_be_bytes());
    out.extend_from_slice(&(command_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&command_bytes);
    out
}

fn decode_metadata_dedupe_record(
    command: &metadatapb::MetadataCommand,
    raw: &[u8],
) -> metadata_state::Result<metadata_state::MetadataApplyResult> {
    if raw.len() < 20 {
        return Err(metadata_state::Error::Decode(
            "metadata command dedupe record too short".to_owned(),
        ));
    }
    let commit_version = u64::from_be_bytes(raw[0..8].try_into().unwrap());
    let applied_mutations = u64::from_be_bytes(raw[8..16].try_into().unwrap());
    let command_len = u32::from_be_bytes(raw[16..20].try_into().unwrap()) as usize;
    if raw.len() != 20 + command_len {
        return Err(metadata_state::Error::Decode(
            "metadata command dedupe record length mismatch".to_owned(),
        ));
    }
    let current = command.encode_to_vec();
    if current.as_slice() != &raw[20..] {
        return Err(metadata_state::Error::Backend(
            "metadata command request id reused with different command".to_owned(),
        ));
    }
    Ok(metadata_state::MetadataApplyResult {
        commit_version,
        applied_mutations,
        error: None,
    })
}

#[cfg(test)]
mod metadata_dedupe_tests {
    use super::*;

    #[test]
    fn metadata_dedupe_record_rejects_reused_request_id_with_different_command() {
        let command = metadatapb::MetadataCommand {
            request_id: b"rid".to_vec(),
            read_version: 10,
            mutations: vec![metadatapb::MetadataMutation {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let encoded = encode_metadata_dedupe_record(&command, 11, 1);
        let changed = metadatapb::MetadataCommand {
            request_id: b"rid".to_vec(),
            read_version: 10,
            mutations: vec![metadatapb::MetadataMutation {
                key: b"k1".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(decode_metadata_dedupe_record(&changed, &encoded).is_err());
        let decoded = decode_metadata_dedupe_record(&command, &encoded).unwrap();
        assert_eq!(decoded.commit_version, 11);
        assert_eq!(decoded.applied_mutations, 1);
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
