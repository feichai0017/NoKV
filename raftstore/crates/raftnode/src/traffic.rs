use std::sync::Mutex;
use std::time::Instant;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegionTrafficSnapshot {
    pub read_ops: u64,
    pub write_ops: u64,
    pub write_bytes: u64,
    pub atomic_ops: u64,
    pub elapsed_secs: u64,
}

pub trait RegionTrafficProvider: Clone + Send + Sync + 'static {
    fn traffic_snapshot(&self) -> RegionTrafficSnapshot;
}

#[derive(Debug)]
pub(crate) struct RegionTrafficStats {
    inner: Mutex<RegionTrafficInner>,
}

#[derive(Debug)]
struct RegionTrafficInner {
    last: Instant,
    read_ops: u64,
    write_ops: u64,
    write_bytes: u64,
    atomic_ops: u64,
}

impl Default for RegionTrafficStats {
    fn default() -> Self {
        Self {
            inner: Mutex::new(RegionTrafficInner {
                last: Instant::now(),
                read_ops: 0,
                write_ops: 0,
                write_bytes: 0,
                atomic_ops: 0,
            }),
        }
    }
}

impl RegionTrafficStats {
    pub(crate) fn record_read(&self, ops: u64) {
        if ops == 0 {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.read_ops = inner.read_ops.saturating_add(ops);
    }

    pub(crate) fn record_apply(&self, keys: &[Vec<u8>], atomic: bool) {
        if keys.is_empty() {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.write_ops = inner.write_ops.saturating_add(1);
        inner.write_bytes = inner
            .write_bytes
            .saturating_add(keys.iter().map(|key| key.len() as u64).sum::<u64>());
        if atomic {
            inner.atomic_ops = inner.atomic_ops.saturating_add(1);
        }
    }

    pub(crate) fn snapshot(&self) -> RegionTrafficSnapshot {
        let now = Instant::now();
        let Ok(mut inner) = self.inner.lock() else {
            return RegionTrafficSnapshot {
                elapsed_secs: 1,
                ..Default::default()
            };
        };
        let elapsed_secs = now.duration_since(inner.last).as_secs().max(1);
        let snapshot = RegionTrafficSnapshot {
            read_ops: inner.read_ops,
            write_ops: inner.write_ops,
            write_bytes: inner.write_bytes,
            atomic_ops: inner.atomic_ops,
            elapsed_secs,
        };
        inner.last = now;
        inner.read_ops = 0;
        inner.write_ops = 0;
        inner.write_bytes = 0;
        inner.atomic_ops = 0;
        snapshot
    }
}
