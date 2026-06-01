use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HoltMetadataMetricsSnapshot {
    pub commit_batches_total: u64,
    pub commit_commands_total: u64,
    pub commit_commands_max: u64,
    pub commit_writes_total: u64,
    pub commit_writes_max: u64,
    pub prepare_ns_total: u64,
    pub prepare_ns_max: u64,
    pub atomic_ns_total: u64,
    pub atomic_ns_max: u64,
    pub total_ns_total: u64,
    pub total_ns_max: u64,
    pub scan_keys_visited_total: u64,
    pub scan_keys_returned_total: u64,
    pub current_hit_total: u64,
    pub history_lookup_total: u64,
}

struct HoltMetadataMetrics {
    commit_batches_total: AtomicU64,
    commit_commands_total: AtomicU64,
    commit_commands_max: AtomicU64,
    commit_writes_total: AtomicU64,
    commit_writes_max: AtomicU64,
    prepare_ns_total: AtomicU64,
    prepare_ns_max: AtomicU64,
    atomic_ns_total: AtomicU64,
    atomic_ns_max: AtomicU64,
    total_ns_total: AtomicU64,
    total_ns_max: AtomicU64,
    scan_keys_visited_total: AtomicU64,
    scan_keys_returned_total: AtomicU64,
    current_hit_total: AtomicU64,
    history_lookup_total: AtomicU64,
}

pub fn holt_metadata_metrics_snapshot() -> HoltMetadataMetricsSnapshot {
    let metrics = holt_metadata_metrics();
    HoltMetadataMetricsSnapshot {
        commit_batches_total: load(&metrics.commit_batches_total),
        commit_commands_total: load(&metrics.commit_commands_total),
        commit_commands_max: load(&metrics.commit_commands_max),
        commit_writes_total: load(&metrics.commit_writes_total),
        commit_writes_max: load(&metrics.commit_writes_max),
        prepare_ns_total: load(&metrics.prepare_ns_total),
        prepare_ns_max: load(&metrics.prepare_ns_max),
        atomic_ns_total: load(&metrics.atomic_ns_total),
        atomic_ns_max: load(&metrics.atomic_ns_max),
        total_ns_total: load(&metrics.total_ns_total),
        total_ns_max: load(&metrics.total_ns_max),
        scan_keys_visited_total: load(&metrics.scan_keys_visited_total),
        scan_keys_returned_total: load(&metrics.scan_keys_returned_total),
        current_hit_total: load(&metrics.current_hit_total),
        history_lookup_total: load(&metrics.history_lookup_total),
    }
}

pub(crate) fn record_metadata_commit(
    commands: u64,
    writes: u64,
    prepare_duration: Duration,
    atomic_duration: Duration,
    total_duration: Duration,
) {
    let metrics = holt_metadata_metrics();
    metrics.commit_batches_total.fetch_add(1, Ordering::Relaxed);
    metrics
        .commit_commands_total
        .fetch_add(commands, Ordering::Relaxed);
    record_max(&metrics.commit_commands_max, commands);
    metrics
        .commit_writes_total
        .fetch_add(writes, Ordering::Relaxed);
    record_max(&metrics.commit_writes_max, writes);
    record_duration(
        &metrics.prepare_ns_total,
        &metrics.prepare_ns_max,
        prepare_duration,
    );
    record_duration(
        &metrics.atomic_ns_total,
        &metrics.atomic_ns_max,
        atomic_duration,
    );
    record_duration(
        &metrics.total_ns_total,
        &metrics.total_ns_max,
        total_duration,
    );
}

pub(crate) fn record_metadata_scan(keys_visited: u64, keys_returned: u64) {
    let metrics = holt_metadata_metrics();
    metrics
        .scan_keys_visited_total
        .fetch_add(keys_visited, Ordering::Relaxed);
    metrics
        .scan_keys_returned_total
        .fetch_add(keys_returned, Ordering::Relaxed);
}

pub(crate) fn record_current_hit() {
    holt_metadata_metrics()
        .current_hit_total
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_history_lookup() {
    holt_metadata_metrics()
        .history_lookup_total
        .fetch_add(1, Ordering::Relaxed);
}

fn holt_metadata_metrics() -> &'static HoltMetadataMetrics {
    static METRICS: OnceLock<HoltMetadataMetrics> = OnceLock::new();
    METRICS.get_or_init(HoltMetadataMetrics::default)
}

impl Default for HoltMetadataMetrics {
    fn default() -> Self {
        Self {
            commit_batches_total: AtomicU64::new(0),
            commit_commands_total: AtomicU64::new(0),
            commit_commands_max: AtomicU64::new(0),
            commit_writes_total: AtomicU64::new(0),
            commit_writes_max: AtomicU64::new(0),
            prepare_ns_total: AtomicU64::new(0),
            prepare_ns_max: AtomicU64::new(0),
            atomic_ns_total: AtomicU64::new(0),
            atomic_ns_max: AtomicU64::new(0),
            total_ns_total: AtomicU64::new(0),
            total_ns_max: AtomicU64::new(0),
            scan_keys_visited_total: AtomicU64::new(0),
            scan_keys_returned_total: AtomicU64::new(0),
            current_hit_total: AtomicU64::new(0),
            history_lookup_total: AtomicU64::new(0),
        }
    }
}

fn record_duration(total: &AtomicU64, max: &AtomicU64, duration: Duration) {
    let ns = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
    total.fetch_add(ns, Ordering::Relaxed);
    record_max(max, ns);
}

fn record_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

fn load(value: &AtomicU64) -> u64 {
    value.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_max_keeps_largest_value() {
        let value = AtomicU64::new(2);
        record_max(&value, 1);
        assert_eq!(value.load(Ordering::Relaxed), 2);
        record_max(&value, 3);
        assert_eq!(value.load(Ordering::Relaxed), 3);
    }
}
