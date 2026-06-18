use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Instant;

use crate::chunk::ObjectReadBlock;
use crate::store::{
    ObjectBytes, ObjectCapabilities, ObjectError, ObjectGetRequest, ObjectInfo, ObjectKey,
    ObjectRange, ObjectStore,
};

use super::local::LocalObjectStoreStats;
use super::pending::{read_pending_cold_puts, remove_pending_cold_put, write_pending_cold_put};
use super::placement::{resolve_block_placements, BlockPlacement};
use super::timing::duration_ns;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredObjectStoreOptions {
    pub put_policy: TieredPutPolicy,
    pub populate_hot_on_get: bool,
    pub hot_fill_mode: HotFillMode,
    pub pending_cold_put_root: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TieredPutPolicy {
    ColdOnly,
    ColdThenHot,
    HotThenBackgroundCold,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HotFillMode {
    Inline,
    Background,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TieredObjectStoreStats {
    pub hot_gets: u64,
    pub hot_hits: u64,
    pub hot_misses: u64,
    pub hot_errors: u64,
    pub cold_gets: u64,
    pub cold_get_bytes: u64,
    pub cold_puts: u64,
    pub cold_put_errors: u64,
    pub hot_puts: u64,
    pub hot_put_errors: u64,
    pub hot_fills: u64,
    pub hot_fill_enqueued: u64,
    pub hot_fill_coalesced: u64,
    pub hot_fill_errors: u64,
    pub cold_deletes: u64,
    pub hot_deletes: u64,
    pub hot_delete_errors: u64,
    pub hot_put_ns: u64,
    pub pending_cold_put_ns: u64,
    pub cold_put_enqueue_ns: u64,
}

#[derive(Clone, Debug)]
pub struct TieredObjectStore<Hot, Cold> {
    hot: Hot,
    cold: Cold,
    options: TieredObjectStoreOptions,
    stats: Arc<Mutex<TieredObjectStoreStats>>,
    inflight_hot_fills: Arc<Mutex<BTreeSet<String>>>,
    cold_put_tombstones: Arc<Mutex<BTreeSet<String>>>,
    cold_put_worker: Arc<Mutex<Option<mpsc::Sender<ColdPutJob>>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HotFillPurpose {
    Put,
    ColdRead,
}

#[derive(Clone, Debug)]
struct ColdPutJob {
    key: ObjectKey,
    bytes: Option<ObjectBytes>,
}

impl Default for TieredObjectStoreOptions {
    fn default() -> Self {
        Self {
            put_policy: TieredPutPolicy::ColdThenHot,
            populate_hot_on_get: true,
            hot_fill_mode: HotFillMode::Inline,
            pending_cold_put_root: None,
        }
    }
}
impl<Hot, Cold> TieredObjectStore<Hot, Cold> {
    pub fn new(hot: Hot, cold: Cold, options: TieredObjectStoreOptions) -> Self {
        Self {
            hot,
            cold,
            options,
            stats: Arc::new(Mutex::new(TieredObjectStoreStats::default())),
            inflight_hot_fills: Arc::new(Mutex::new(BTreeSet::new())),
            cold_put_tombstones: Arc::new(Mutex::new(BTreeSet::new())),
            cold_put_worker: Arc::new(Mutex::new(None)),
        }
    }

    pub fn hot(&self) -> &Hot {
        &self.hot
    }

    pub fn cold(&self) -> &Cold {
        &self.cold
    }

    pub fn stats(&self) -> Result<TieredObjectStoreStats, ObjectError> {
        self.stats
            .lock()
            .map(|stats| *stats)
            .map_err(ObjectError::from_poisoned_lock)
    }

    fn record(&self, update: impl FnOnce(&mut TieredObjectStoreStats)) -> Result<(), ObjectError> {
        let mut stats = self.stats.lock().map_err(ObjectError::from_poisoned_lock)?;
        update(&mut stats);
        Ok(())
    }

    fn record_shared(
        stats: &Arc<Mutex<TieredObjectStoreStats>>,
        update: impl FnOnce(&mut TieredObjectStoreStats),
    ) -> Result<(), ObjectError> {
        let mut stats = stats.lock().map_err(ObjectError::from_poisoned_lock)?;
        update(&mut stats);
        Ok(())
    }

    fn clear_cold_put_tombstone(&self, key: &ObjectKey) -> Result<(), ObjectError> {
        let mut tombstones = self
            .cold_put_tombstones
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?;
        tombstones.remove(key.as_str());
        Ok(())
    }

    fn mark_cold_put_tombstone(&self, key: &ObjectKey) -> Result<(), ObjectError> {
        let mut tombstones = self
            .cold_put_tombstones
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?;
        tombstones.insert(key.as_str().to_owned());
        Ok(())
    }

    fn write_pending_cold_put(&self, key: &ObjectKey) -> Result<(), ObjectError> {
        let Some(root) = &self.options.pending_cold_put_root else {
            return Ok(());
        };
        write_pending_cold_put(root, key)
    }

    fn remove_pending_cold_put(&self, key: &ObjectKey) -> Result<(), ObjectError> {
        let Some(root) = &self.options.pending_cold_put_root else {
            return Ok(());
        };
        remove_pending_cold_put(root, key)
    }
}

impl<Hot, Cold> TieredObjectStore<Hot, Cold>
where
    Hot: ObjectStore + Clone + Send + 'static,
    Cold: ObjectStore + Clone + Send + 'static,
{
    fn put_hot_then_background_cold(
        &self,
        key: &ObjectKey,
        bytes: ObjectBytes,
    ) -> Result<ObjectInfo, ObjectError> {
        let cold_bytes = bytes.clone();
        let hot_put_start = Instant::now();
        let info = match self.hot.put(key, bytes) {
            Ok(info) => {
                let hot_put_ns = duration_ns(hot_put_start.elapsed());
                self.record(|stats| {
                    stats.hot_puts = stats.hot_puts.saturating_add(1);
                    stats.hot_put_ns = stats.hot_put_ns.saturating_add(hot_put_ns);
                })?;
                info
            }
            Err(err) => {
                let hot_put_ns = duration_ns(hot_put_start.elapsed());
                self.record(|stats| {
                    stats.hot_put_errors = stats.hot_put_errors.saturating_add(1);
                    stats.hot_put_ns = stats.hot_put_ns.saturating_add(hot_put_ns);
                })?;
                return Err(err);
            }
        };

        let pending_start = Instant::now();
        self.write_pending_cold_put(key)?;
        let pending_ns = duration_ns(pending_start.elapsed());
        self.record(|stats| {
            stats.pending_cold_put_ns = stats.pending_cold_put_ns.saturating_add(pending_ns);
        })?;

        let enqueue_start = Instant::now();
        self.enqueue_cold_put(key.clone(), Some(cold_bytes))?;
        let enqueue_ns = duration_ns(enqueue_start.elapsed());
        self.record(|stats| {
            stats.cold_put_enqueue_ns = stats.cold_put_enqueue_ns.saturating_add(enqueue_ns);
        })?;
        Ok(info)
    }

    pub fn recover_pending_cold_puts(&self) -> Result<usize, ObjectError> {
        let Some(root) = &self.options.pending_cold_put_root else {
            return Ok(0);
        };
        let keys = read_pending_cold_puts(root)?;
        let count = keys.len();
        for key in keys {
            self.enqueue_cold_put(key, None)?;
        }
        Ok(count)
    }

    fn enqueue_cold_put(
        &self,
        key: ObjectKey,
        bytes: Option<ObjectBytes>,
    ) -> Result<(), ObjectError> {
        let sender = self.cold_put_sender()?;
        sender
            .send(ColdPutJob { key, bytes })
            .map_err(|_| ObjectError::Backend("background cold put worker stopped".to_owned()))
    }

    fn cold_put_sender(&self) -> Result<mpsc::Sender<ColdPutJob>, ObjectError> {
        let mut worker = self
            .cold_put_worker
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?;
        if let Some(sender) = worker.as_ref() {
            return Ok(sender.clone());
        }

        let (sender, receiver) = mpsc::channel::<ColdPutJob>();
        let hot = self.hot.clone();
        let cold = self.cold.clone();
        let stats = self.stats.clone();
        let tombstones = self.cold_put_tombstones.clone();
        let pending_cold_put_root = self.options.pending_cold_put_root.clone();
        thread::spawn(move || {
            for job in receiver {
                let ColdPutJob { key, bytes } = job;
                let key_string = key.as_str().to_owned();
                let result = match bytes {
                    Some(bytes) => cold.put(&key, bytes),
                    None => hot.get(&key, None).and_then(|bytes| cold.put(&key, bytes)),
                }
                .and_then(|_| {
                    let tombstoned = tombstones
                        .lock()
                        .map(|tombstones| tombstones.contains(&key_string))
                        .unwrap_or(true);
                    if tombstoned {
                        cold.delete(&key)?;
                    }
                    if let Some(root) = &pending_cold_put_root {
                        remove_pending_cold_put(root, &key)?;
                    }
                    Ok(())
                });
                let _ = Self::record_shared(&stats, |stats| match result {
                    Ok(_) => {
                        stats.cold_puts = stats.cold_puts.saturating_add(1);
                    }
                    Err(_) => {
                        stats.cold_put_errors = stats.cold_put_errors.saturating_add(1);
                    }
                });
            }
        });
        *worker = Some(sender.clone());
        Ok(sender)
    }

    fn fill_hot(
        &self,
        key: ObjectKey,
        bytes: impl Into<ObjectBytes>,
        purpose: HotFillPurpose,
    ) -> Result<(), ObjectError> {
        let bytes = bytes.into();
        match (purpose, self.options.hot_fill_mode) {
            (HotFillPurpose::Put, _) | (HotFillPurpose::ColdRead, HotFillMode::Inline) => {
                self.fill_hot_inline(key, bytes, purpose)
            }
            (HotFillPurpose::ColdRead, HotFillMode::Background) => {
                self.enqueue_hot_fill(key, bytes)
            }
        }
    }

    fn fill_hot_inline(
        &self,
        key: ObjectKey,
        bytes: ObjectBytes,
        purpose: HotFillPurpose,
    ) -> Result<(), ObjectError> {
        match self.hot.put(&key, bytes) {
            Ok(_) => {
                self.record(|stats| match purpose {
                    HotFillPurpose::Put => stats.hot_puts = stats.hot_puts.saturating_add(1),
                    HotFillPurpose::ColdRead => {
                        stats.hot_fills = stats.hot_fills.saturating_add(1);
                    }
                })?;
            }
            Err(_) => {
                self.record(|stats| match purpose {
                    HotFillPurpose::Put => {
                        stats.hot_put_errors = stats.hot_put_errors.saturating_add(1);
                    }
                    HotFillPurpose::ColdRead => {
                        stats.hot_fill_errors = stats.hot_fill_errors.saturating_add(1);
                    }
                })?;
            }
        }
        Ok(())
    }

    fn enqueue_hot_fill(&self, key: ObjectKey, bytes: ObjectBytes) -> Result<(), ObjectError> {
        let fill_key = key.as_str().to_owned();
        {
            let mut inflight = self
                .inflight_hot_fills
                .lock()
                .map_err(ObjectError::from_poisoned_lock)?;
            if !inflight.insert(fill_key.clone()) {
                self.record(|stats| {
                    stats.hot_fill_coalesced = stats.hot_fill_coalesced.saturating_add(1);
                })?;
                return Ok(());
            }
        }
        self.record(|stats| {
            stats.hot_fill_enqueued = stats.hot_fill_enqueued.saturating_add(1);
        })?;
        let hot = self.hot.clone();
        let stats = self.stats.clone();
        let inflight = self.inflight_hot_fills.clone();
        thread::spawn(move || {
            let result = hot.put(&key, bytes);
            let _ = Self::record_shared(&stats, |stats| match result {
                Ok(_) => {
                    stats.hot_fills = stats.hot_fills.saturating_add(1);
                }
                Err(_) => {
                    stats.hot_fill_errors = stats.hot_fill_errors.saturating_add(1);
                }
            });
            if let Ok(mut inflight) = inflight.lock() {
                inflight.remove(&fill_key);
            }
        });
        Ok(())
    }
}

impl<Hot, Cold> ObjectStore for TieredObjectStore<Hot, Cold>
where
    Hot: ObjectStore + Clone + Send + 'static,
    Cold: ObjectStore + Clone + Send + 'static,
{
    fn capabilities(&self) -> ObjectCapabilities {
        self.cold.capabilities()
    }

    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        let bytes = bytes.into();
        self.clear_cold_put_tombstone(key)?;
        match self.options.put_policy {
            TieredPutPolicy::ColdOnly => {
                let info = self.cold.put(key, bytes)?;
                self.record(|stats| stats.cold_puts = stats.cold_puts.saturating_add(1))?;
                self.remove_pending_cold_put(key)?;
                Ok(info)
            }
            TieredPutPolicy::ColdThenHot => {
                let hot_bytes = bytes.clone();
                let info = self.cold.put(key, bytes)?;
                self.record(|stats| stats.cold_puts = stats.cold_puts.saturating_add(1))?;
                self.fill_hot(key.clone(), hot_bytes, HotFillPurpose::Put)?;
                self.remove_pending_cold_put(key)?;
                Ok(info)
            }
            TieredPutPolicy::HotThenBackgroundCold => self.put_hot_then_background_cold(key, bytes),
        }
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.record(|stats| stats.hot_gets = stats.hot_gets.saturating_add(1))?;
        match self.hot.get_if_present(key, range) {
            Ok(Some(bytes)) => {
                self.record(|stats| stats.hot_hits = stats.hot_hits.saturating_add(1))?;
                return Ok(bytes);
            }
            Ok(None) => {
                self.record(|stats| stats.hot_misses = stats.hot_misses.saturating_add(1))?;
            }
            Err(_) => {
                self.record(|stats| stats.hot_errors = stats.hot_errors.saturating_add(1))?;
            }
        }

        let bytes = self.cold.get(key, range)?;
        self.record(|stats| {
            stats.cold_gets = stats.cold_gets.saturating_add(1);
            stats.cold_get_bytes = stats.cold_get_bytes.saturating_add(bytes.len() as u64);
        })?;
        if self.options.populate_hot_on_get && self.should_fill_hot(key, range, bytes.len()) {
            self.fill_hot(key.clone(), bytes.clone(), HotFillPurpose::ColdRead)?;
        }
        Ok(bytes)
    }

    fn get_many(&self, requests: &[ObjectGetRequest]) -> Result<Vec<Vec<u8>>, ObjectError> {
        let mut results = vec![None; requests.len()];
        let mut cold_indexes = Vec::new();
        let mut cold_requests = Vec::new();

        if !requests.is_empty() {
            let request_count = requests.len() as u64;
            self.record(|stats| stats.hot_gets = stats.hot_gets.saturating_add(request_count))?;
        }

        match self.hot.get_many_if_present(requests) {
            Ok(hot_results) if hot_results.len() == requests.len() => {
                for (index, (request, maybe_bytes)) in requests.iter().zip(hot_results).enumerate()
                {
                    match maybe_bytes {
                        Some(bytes) => {
                            self.record(|stats| {
                                stats.hot_hits = stats.hot_hits.saturating_add(1);
                            })?;
                            results[index] = Some(bytes);
                        }
                        None => {
                            self.record(|stats| {
                                stats.hot_misses = stats.hot_misses.saturating_add(1);
                            })?;
                            cold_indexes.push(index);
                            cold_requests.push(request.clone());
                        }
                    }
                }
            }
            Ok(_) | Err(_) => {
                self.record(|stats| {
                    stats.hot_errors = stats.hot_errors.saturating_add(requests.len() as u64);
                })?;
                cold_indexes.extend(0..requests.len());
                cold_requests.extend(requests.iter().cloned());
            }
        }

        if !cold_requests.is_empty() {
            let cold_request_count = cold_requests.len() as u64;
            let cold_bytes = self.cold.get_many(&cold_requests)?;
            if cold_bytes.len() != cold_requests.len() {
                return Err(ObjectError::Backend(
                    "cold object store returned wrong batch length".to_owned(),
                ));
            }
            let mut cold_get_bytes = 0_u64;
            for ((index, request), bytes) in
                cold_indexes.into_iter().zip(cold_requests).zip(cold_bytes)
            {
                cold_get_bytes = cold_get_bytes.saturating_add(bytes.len() as u64);
                if self.options.populate_hot_on_get
                    && self.should_fill_hot(&request.key, request.range, bytes.len())
                {
                    self.fill_hot(request.key.clone(), bytes.clone(), HotFillPurpose::ColdRead)?;
                }
                results[index] = Some(bytes);
            }
            self.record(|stats| {
                stats.cold_gets = stats.cold_gets.saturating_add(cold_request_count);
                stats.cold_get_bytes = stats.cold_get_bytes.saturating_add(cold_get_bytes);
            })?;
        }

        results
            .into_iter()
            .map(|bytes| {
                bytes.ok_or_else(|| {
                    ObjectError::Backend("tiered object store lost batch result".to_owned())
                })
            })
            .collect()
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        match self.hot.head(key) {
            Ok(Some(info)) => Ok(Some(info)),
            Ok(None) | Err(_) => self.cold.head(key),
        }
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.mark_cold_put_tombstone(key)?;
        self.remove_pending_cold_put(key)?;
        let cold_existed = self.cold.delete(key)?;
        self.record(|stats| stats.cold_deletes = stats.cold_deletes.saturating_add(1))?;
        let hot_existed = match self.hot.delete(key) {
            Ok(existed) => {
                if existed {
                    self.record(|stats| stats.hot_deletes = stats.hot_deletes.saturating_add(1))?;
                }
                existed
            }
            Err(_) => {
                self.record(|stats| {
                    stats.hot_delete_errors = stats.hot_delete_errors.saturating_add(1);
                })?;
                false
            }
        };
        Ok(cold_existed || hot_existed)
    }

    fn resolve_read_placements(
        &self,
        blocks: &[ObjectReadBlock],
    ) -> Result<Vec<BlockPlacement>, ObjectError> {
        resolve_block_placements(&self.hot, blocks)
    }

    fn tiered_stats(&self) -> Result<Option<TieredObjectStoreStats>, ObjectError> {
        self.stats().map(Some)
    }

    fn local_hot_stats(&self) -> Result<Option<LocalObjectStoreStats>, ObjectError> {
        self.hot.local_hot_stats()
    }
}

impl<Hot, Cold> TieredObjectStore<Hot, Cold>
where
    Cold: ObjectStore,
{
    fn should_fill_hot(&self, key: &ObjectKey, range: Option<ObjectRange>, len: usize) -> bool {
        match range {
            None => true,
            Some(range) if range.offset == 0 && range.len == len => {
                matches!(self.cold.head(key), Ok(Some(info)) if info.size == len as u64)
            }
            Some(_) => false,
        }
    }
}
