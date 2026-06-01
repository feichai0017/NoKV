use std::path::Path;
use std::sync::{Arc, Mutex};

use holt::{Tree, TreeConfig, DB};
use nokv_metadata_state as metadata_state;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::trees::{
    current_tree_for_family, APPLY_STATE_TREE, DEFAULT_CURRENT_TREE, HISTORY_TREE,
    REGION_META_TREE, REQUIRED_TREES, WATCH_APPLY_TREE,
};
use crate::Result;

#[derive(Clone)]
pub struct HoltStore {
    db: DB,
}

impl HoltStore {
    pub fn open_memory() -> Result<Self> {
        Self::open(TreeConfig::memory())
    }

    pub fn open_file(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(TreeConfig::new(path.as_ref()))
    }

    pub fn open(cfg: TreeConfig) -> Result<Self> {
        let db = DB::open(cfg)?;
        let store = Self { db };
        store.ensure_required_trees()?;
        Ok(store)
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.db.checkpoint()?;
        Ok(())
    }

    pub fn atomic<F>(&self, build: F) -> Result<bool>
    where
        F: FnOnce(&mut holt::DBAtomicBatch),
    {
        Ok(self.db.atomic(build)?)
    }

    pub fn default_current(&self) -> Result<Tree> {
        self.tree(DEFAULT_CURRENT_TREE)
    }

    pub fn current(&self, family: metadatapb::MetadataFamily) -> Result<Tree> {
        self.tree(current_tree_for_family(family))
    }

    pub fn history(&self) -> Result<Tree> {
        self.tree(HISTORY_TREE)
    }

    pub fn region_meta(&self) -> Result<Tree> {
        self.tree(REGION_META_TREE)
    }

    pub fn apply_state(&self) -> Result<Tree> {
        self.tree(APPLY_STATE_TREE)
    }

    pub fn watch_apply(&self) -> Result<Tree> {
        self.tree(WATCH_APPLY_TREE)
    }

    pub fn put_data(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.default_current()?.put(key, value)?;
        Ok(())
    }

    pub fn get_data(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.default_current()?.get(key)?)
    }

    pub(crate) fn tree(&self, name: &str) -> Result<Tree> {
        Ok(self.db.open_tree(name)?)
    }

    fn ensure_required_trees(&self) -> Result<()> {
        for name in REQUIRED_TREES {
            self.db.open_or_create_tree(name)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct HoltMetadataStore {
    pub(crate) store: HoltStore,
    pub(crate) gate: Arc<Mutex<()>>,
}

impl HoltMetadataStore {
    pub fn open_memory() -> Result<Self> {
        Ok(Self::new(HoltStore::open_memory()?))
    }

    pub fn open_file(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(HoltStore::open_file(path)?))
    }

    pub fn new(store: HoltStore) -> Self {
        Self {
            store,
            gate: Arc::new(Mutex::new(())),
        }
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.store.checkpoint()
    }

    pub(crate) fn lock(&self) -> metadata_state::Result<std::sync::MutexGuard<'_, ()>> {
        self.gate.lock().map_err(|_| {
            metadata_state::Error::Backend("holt metadata store mutex poisoned".to_owned())
        })
    }

    pub(crate) fn atomic<F>(&self, build: F) -> metadata_state::Result<bool>
    where
        F: FnOnce(&mut holt::DBAtomicBatch),
    {
        self.store
            .atomic(build)
            .map_err(|err| metadata_state::Error::Backend(err.to_string()))
    }
}

pub(crate) fn to_backend_error(err: impl std::fmt::Display) -> metadata_state::Error {
    metadata_state::Error::Backend(err.to_string())
}
