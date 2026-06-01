use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RaftNodeMetricsSnapshot {
    pub proposals_total: u64,
    pub proposal_commands_total: u64,
    pub proposal_commands_max: u64,
    pub proposal_ns_total: u64,
    pub proposal_ns_max: u64,
    pub log_append_calls_total: u64,
    pub log_entries_total: u64,
    pub log_entries_max: u64,
    pub log_append_ns_total: u64,
    pub log_append_ns_max: u64,
    pub log_sync_ns_total: u64,
    pub log_sync_ns_max: u64,
    pub state_machine_apply_calls_total: u64,
    pub state_machine_apply_entries_total: u64,
    pub state_machine_apply_entries_max: u64,
    pub state_machine_apply_ns_total: u64,
    pub state_machine_apply_ns_max: u64,
    pub metadata_apply_batches_total: u64,
    pub metadata_apply_commands_total: u64,
    pub metadata_apply_commands_max: u64,
    pub metadata_apply_ns_total: u64,
    pub metadata_apply_ns_max: u64,
    pub append_entries_client_calls_total: u64,
    pub append_entries_client_entries_total: u64,
    pub append_entries_client_entries_max: u64,
    pub append_entries_client_ns_total: u64,
    pub append_entries_client_ns_max: u64,
    pub append_entries_client_empty_calls_total: u64,
    pub append_entries_client_empty_ns_total: u64,
    pub append_entries_client_data_calls_total: u64,
    pub append_entries_client_data_ns_total: u64,
    pub append_entries_client_error_calls_total: u64,
    pub append_entries_client_error_ns_total: u64,
    pub append_entries_server_calls_total: u64,
    pub append_entries_server_entries_total: u64,
    pub append_entries_server_entries_max: u64,
    pub append_entries_server_ns_total: u64,
    pub append_entries_server_ns_max: u64,
    pub append_entries_server_empty_calls_total: u64,
    pub append_entries_server_empty_ns_total: u64,
    pub append_entries_server_data_calls_total: u64,
    pub append_entries_server_data_ns_total: u64,
}

pub(crate) struct RaftNodeMetrics {
    proposals_total: AtomicU64,
    proposal_commands_total: AtomicU64,
    proposal_commands_max: AtomicU64,
    proposal_ns_total: AtomicU64,
    proposal_ns_max: AtomicU64,
    log_append_calls_total: AtomicU64,
    log_entries_total: AtomicU64,
    log_entries_max: AtomicU64,
    log_append_ns_total: AtomicU64,
    log_append_ns_max: AtomicU64,
    log_sync_ns_total: AtomicU64,
    log_sync_ns_max: AtomicU64,
    state_machine_apply_calls_total: AtomicU64,
    state_machine_apply_entries_total: AtomicU64,
    state_machine_apply_entries_max: AtomicU64,
    state_machine_apply_ns_total: AtomicU64,
    state_machine_apply_ns_max: AtomicU64,
    metadata_apply_batches_total: AtomicU64,
    metadata_apply_commands_total: AtomicU64,
    metadata_apply_commands_max: AtomicU64,
    metadata_apply_ns_total: AtomicU64,
    metadata_apply_ns_max: AtomicU64,
    append_entries_client_calls_total: AtomicU64,
    append_entries_client_entries_total: AtomicU64,
    append_entries_client_entries_max: AtomicU64,
    append_entries_client_ns_total: AtomicU64,
    append_entries_client_ns_max: AtomicU64,
    append_entries_client_empty_calls_total: AtomicU64,
    append_entries_client_empty_ns_total: AtomicU64,
    append_entries_client_data_calls_total: AtomicU64,
    append_entries_client_data_ns_total: AtomicU64,
    append_entries_client_error_calls_total: AtomicU64,
    append_entries_client_error_ns_total: AtomicU64,
    append_entries_server_calls_total: AtomicU64,
    append_entries_server_entries_total: AtomicU64,
    append_entries_server_entries_max: AtomicU64,
    append_entries_server_ns_total: AtomicU64,
    append_entries_server_ns_max: AtomicU64,
    append_entries_server_empty_calls_total: AtomicU64,
    append_entries_server_empty_ns_total: AtomicU64,
    append_entries_server_data_calls_total: AtomicU64,
    append_entries_server_data_ns_total: AtomicU64,
}

pub fn raftnode_metrics_snapshot() -> RaftNodeMetricsSnapshot {
    let metrics = raftnode_metrics();
    RaftNodeMetricsSnapshot {
        proposals_total: load(&metrics.proposals_total),
        proposal_commands_total: load(&metrics.proposal_commands_total),
        proposal_commands_max: load(&metrics.proposal_commands_max),
        proposal_ns_total: load(&metrics.proposal_ns_total),
        proposal_ns_max: load(&metrics.proposal_ns_max),
        log_append_calls_total: load(&metrics.log_append_calls_total),
        log_entries_total: load(&metrics.log_entries_total),
        log_entries_max: load(&metrics.log_entries_max),
        log_append_ns_total: load(&metrics.log_append_ns_total),
        log_append_ns_max: load(&metrics.log_append_ns_max),
        log_sync_ns_total: load(&metrics.log_sync_ns_total),
        log_sync_ns_max: load(&metrics.log_sync_ns_max),
        state_machine_apply_calls_total: load(&metrics.state_machine_apply_calls_total),
        state_machine_apply_entries_total: load(&metrics.state_machine_apply_entries_total),
        state_machine_apply_entries_max: load(&metrics.state_machine_apply_entries_max),
        state_machine_apply_ns_total: load(&metrics.state_machine_apply_ns_total),
        state_machine_apply_ns_max: load(&metrics.state_machine_apply_ns_max),
        metadata_apply_batches_total: load(&metrics.metadata_apply_batches_total),
        metadata_apply_commands_total: load(&metrics.metadata_apply_commands_total),
        metadata_apply_commands_max: load(&metrics.metadata_apply_commands_max),
        metadata_apply_ns_total: load(&metrics.metadata_apply_ns_total),
        metadata_apply_ns_max: load(&metrics.metadata_apply_ns_max),
        append_entries_client_calls_total: load(&metrics.append_entries_client_calls_total),
        append_entries_client_entries_total: load(&metrics.append_entries_client_entries_total),
        append_entries_client_entries_max: load(&metrics.append_entries_client_entries_max),
        append_entries_client_ns_total: load(&metrics.append_entries_client_ns_total),
        append_entries_client_ns_max: load(&metrics.append_entries_client_ns_max),
        append_entries_client_empty_calls_total: load(
            &metrics.append_entries_client_empty_calls_total,
        ),
        append_entries_client_empty_ns_total: load(&metrics.append_entries_client_empty_ns_total),
        append_entries_client_data_calls_total: load(
            &metrics.append_entries_client_data_calls_total,
        ),
        append_entries_client_data_ns_total: load(&metrics.append_entries_client_data_ns_total),
        append_entries_client_error_calls_total: load(
            &metrics.append_entries_client_error_calls_total,
        ),
        append_entries_client_error_ns_total: load(&metrics.append_entries_client_error_ns_total),
        append_entries_server_calls_total: load(&metrics.append_entries_server_calls_total),
        append_entries_server_entries_total: load(&metrics.append_entries_server_entries_total),
        append_entries_server_entries_max: load(&metrics.append_entries_server_entries_max),
        append_entries_server_ns_total: load(&metrics.append_entries_server_ns_total),
        append_entries_server_ns_max: load(&metrics.append_entries_server_ns_max),
        append_entries_server_empty_calls_total: load(
            &metrics.append_entries_server_empty_calls_total,
        ),
        append_entries_server_empty_ns_total: load(&metrics.append_entries_server_empty_ns_total),
        append_entries_server_data_calls_total: load(
            &metrics.append_entries_server_data_calls_total,
        ),
        append_entries_server_data_ns_total: load(&metrics.append_entries_server_data_ns_total),
    }
}

pub(crate) fn record_proposal(commands: u64, duration: Duration) {
    let metrics = raftnode_metrics();
    metrics.proposals_total.fetch_add(1, Ordering::Relaxed);
    metrics
        .proposal_commands_total
        .fetch_add(commands, Ordering::Relaxed);
    record_max(&metrics.proposal_commands_max, commands);
    record_duration(
        &metrics.proposal_ns_total,
        &metrics.proposal_ns_max,
        duration,
    );
}

pub(crate) fn record_log_append(entries: u64, append_duration: Duration, sync_duration: Duration) {
    let metrics = raftnode_metrics();
    metrics
        .log_append_calls_total
        .fetch_add(1, Ordering::Relaxed);
    metrics
        .log_entries_total
        .fetch_add(entries, Ordering::Relaxed);
    record_max(&metrics.log_entries_max, entries);
    record_duration(
        &metrics.log_append_ns_total,
        &metrics.log_append_ns_max,
        append_duration,
    );
    record_duration(
        &metrics.log_sync_ns_total,
        &metrics.log_sync_ns_max,
        sync_duration,
    );
}

pub(crate) fn record_state_machine_apply(entries: u64, duration: Duration) {
    let metrics = raftnode_metrics();
    metrics
        .state_machine_apply_calls_total
        .fetch_add(1, Ordering::Relaxed);
    metrics
        .state_machine_apply_entries_total
        .fetch_add(entries, Ordering::Relaxed);
    record_max(&metrics.state_machine_apply_entries_max, entries);
    record_duration(
        &metrics.state_machine_apply_ns_total,
        &metrics.state_machine_apply_ns_max,
        duration,
    );
}

pub(crate) fn record_metadata_apply(commands: u64, duration: Duration) {
    let metrics = raftnode_metrics();
    metrics
        .metadata_apply_batches_total
        .fetch_add(1, Ordering::Relaxed);
    metrics
        .metadata_apply_commands_total
        .fetch_add(commands, Ordering::Relaxed);
    record_max(&metrics.metadata_apply_commands_max, commands);
    record_duration(
        &metrics.metadata_apply_ns_total,
        &metrics.metadata_apply_ns_max,
        duration,
    );
}

pub(crate) fn record_append_entries_client(entries: u64, duration: Duration, ok: bool) {
    let metrics = raftnode_metrics();
    metrics
        .append_entries_client_calls_total
        .fetch_add(1, Ordering::Relaxed);
    metrics
        .append_entries_client_entries_total
        .fetch_add(entries, Ordering::Relaxed);
    record_max(&metrics.append_entries_client_entries_max, entries);
    record_duration(
        &metrics.append_entries_client_ns_total,
        &metrics.append_entries_client_ns_max,
        duration,
    );
    let ns = duration_ns(duration);
    if !ok {
        metrics
            .append_entries_client_error_calls_total
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .append_entries_client_error_ns_total
            .fetch_add(ns, Ordering::Relaxed);
    } else if entries == 0 {
        metrics
            .append_entries_client_empty_calls_total
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .append_entries_client_empty_ns_total
            .fetch_add(ns, Ordering::Relaxed);
    } else {
        metrics
            .append_entries_client_data_calls_total
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .append_entries_client_data_ns_total
            .fetch_add(ns, Ordering::Relaxed);
    }
}

pub(crate) fn record_append_entries_server(entries: u64, duration: Duration) {
    let metrics = raftnode_metrics();
    metrics
        .append_entries_server_calls_total
        .fetch_add(1, Ordering::Relaxed);
    metrics
        .append_entries_server_entries_total
        .fetch_add(entries, Ordering::Relaxed);
    record_max(&metrics.append_entries_server_entries_max, entries);
    record_duration(
        &metrics.append_entries_server_ns_total,
        &metrics.append_entries_server_ns_max,
        duration,
    );
    let ns = duration_ns(duration);
    if entries == 0 {
        metrics
            .append_entries_server_empty_calls_total
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .append_entries_server_empty_ns_total
            .fetch_add(ns, Ordering::Relaxed);
    } else {
        metrics
            .append_entries_server_data_calls_total
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .append_entries_server_data_ns_total
            .fetch_add(ns, Ordering::Relaxed);
    }
}

fn raftnode_metrics() -> &'static RaftNodeMetrics {
    static METRICS: OnceLock<RaftNodeMetrics> = OnceLock::new();
    METRICS.get_or_init(RaftNodeMetrics::default)
}

impl Default for RaftNodeMetrics {
    fn default() -> Self {
        Self {
            proposals_total: AtomicU64::new(0),
            proposal_commands_total: AtomicU64::new(0),
            proposal_commands_max: AtomicU64::new(0),
            proposal_ns_total: AtomicU64::new(0),
            proposal_ns_max: AtomicU64::new(0),
            log_append_calls_total: AtomicU64::new(0),
            log_entries_total: AtomicU64::new(0),
            log_entries_max: AtomicU64::new(0),
            log_append_ns_total: AtomicU64::new(0),
            log_append_ns_max: AtomicU64::new(0),
            log_sync_ns_total: AtomicU64::new(0),
            log_sync_ns_max: AtomicU64::new(0),
            state_machine_apply_calls_total: AtomicU64::new(0),
            state_machine_apply_entries_total: AtomicU64::new(0),
            state_machine_apply_entries_max: AtomicU64::new(0),
            state_machine_apply_ns_total: AtomicU64::new(0),
            state_machine_apply_ns_max: AtomicU64::new(0),
            metadata_apply_batches_total: AtomicU64::new(0),
            metadata_apply_commands_total: AtomicU64::new(0),
            metadata_apply_commands_max: AtomicU64::new(0),
            metadata_apply_ns_total: AtomicU64::new(0),
            metadata_apply_ns_max: AtomicU64::new(0),
            append_entries_client_calls_total: AtomicU64::new(0),
            append_entries_client_entries_total: AtomicU64::new(0),
            append_entries_client_entries_max: AtomicU64::new(0),
            append_entries_client_ns_total: AtomicU64::new(0),
            append_entries_client_ns_max: AtomicU64::new(0),
            append_entries_client_empty_calls_total: AtomicU64::new(0),
            append_entries_client_empty_ns_total: AtomicU64::new(0),
            append_entries_client_data_calls_total: AtomicU64::new(0),
            append_entries_client_data_ns_total: AtomicU64::new(0),
            append_entries_client_error_calls_total: AtomicU64::new(0),
            append_entries_client_error_ns_total: AtomicU64::new(0),
            append_entries_server_calls_total: AtomicU64::new(0),
            append_entries_server_entries_total: AtomicU64::new(0),
            append_entries_server_entries_max: AtomicU64::new(0),
            append_entries_server_ns_total: AtomicU64::new(0),
            append_entries_server_ns_max: AtomicU64::new(0),
            append_entries_server_empty_calls_total: AtomicU64::new(0),
            append_entries_server_empty_ns_total: AtomicU64::new(0),
            append_entries_server_data_calls_total: AtomicU64::new(0),
            append_entries_server_data_ns_total: AtomicU64::new(0),
        }
    }
}

pub(crate) fn record_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

pub(crate) fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn record_duration(total: &AtomicU64, max: &AtomicU64, duration: Duration) {
    let ns = duration_ns(duration);
    total.fetch_add(ns, Ordering::Relaxed);
    record_max(max, ns);
}

fn load(value: &AtomicU64) -> u64 {
    value.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_max_keeps_largest_value() {
        let value = AtomicU64::new(5);
        record_max(&value, 3);
        assert_eq!(value.load(Ordering::Relaxed), 5);
        record_max(&value, 9);
        assert_eq!(value.load(Ordering::Relaxed), 9);
    }
}
