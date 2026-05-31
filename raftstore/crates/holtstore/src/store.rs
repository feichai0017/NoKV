use std::path::Path;
use std::sync::{Arc, Mutex};

use holt::{Tree, TreeConfig, DB};
use nokv_mvcc as mvcc;

use crate::trees::{
    APPLY_STATE_TREE, DATA_TREE, REGION_META_TREE, REQUIRED_TREES, WATCH_APPLY_TREE, WRITE_TREE,
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

    pub fn data(&self) -> Result<Tree> {
        self.tree(DATA_TREE)
    }

    pub fn write(&self) -> Result<Tree> {
        self.tree(WRITE_TREE)
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
        self.data()?.put(key, value)?;
        Ok(())
    }

    pub fn get_data(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data()?.get(key)?)
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
pub struct HoltMvccStore {
    pub(crate) store: HoltStore,
    pub(crate) gate: Arc<Mutex<()>>,
}

impl HoltMvccStore {
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

    pub(crate) fn lock(&self) -> mvcc::Result<std::sync::MutexGuard<'_, ()>> {
        self.gate
            .lock()
            .map_err(|_| mvcc::Error::Backend("holt mvcc mutex poisoned".to_owned()))
    }

    pub(crate) fn atomic<F>(&self, build: F) -> mvcc::Result<bool>
    where
        F: FnOnce(&mut holt::DBAtomicBatch),
    {
        self.store
            .atomic(build)
            .map_err(|err| mvcc::Error::Backend(err.to_string()))
    }
}

pub(crate) fn to_backend_error(err: impl std::fmt::Display) -> mvcc::Error {
    mvcc::Error::Backend(err.to_string())
}
