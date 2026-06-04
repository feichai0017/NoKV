use std::collections::BTreeMap;
use std::sync::Mutex;

use nokvfs_types::MountId;

use crate::{CheckpointManifest, SharedLogError};

pub trait CheckpointCatalog {
    fn publish(&self, manifest: CheckpointManifest) -> Result<(), SharedLogError>;
    fn latest_for_mount(
        &self,
        mount: MountId,
    ) -> Result<Option<CheckpointManifest>, SharedLogError>;
}

#[derive(Debug, Default)]
pub struct MemoryCheckpointCatalog {
    manifests: Mutex<BTreeMap<MountId, CheckpointManifest>>,
}

impl MemoryCheckpointCatalog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CheckpointCatalog for MemoryCheckpointCatalog {
    fn publish(&self, manifest: CheckpointManifest) -> Result<(), SharedLogError> {
        if manifest.id.is_empty() {
            return Err(SharedLogError::EmptyCheckpointId);
        }
        let mut manifests = self
            .manifests
            .lock()
            .map_err(|_| SharedLogError::Backend("checkpoint catalog mutex poisoned".to_owned()))?;
        let replace = manifests
            .get(&manifest.mount)
            .map(|current| checkpoint_is_newer(&manifest, current))
            .unwrap_or(true);
        if replace {
            manifests.insert(manifest.mount, manifest);
        }
        Ok(())
    }

    fn latest_for_mount(
        &self,
        mount: MountId,
    ) -> Result<Option<CheckpointManifest>, SharedLogError> {
        self.manifests
            .lock()
            .map(|manifests| manifests.get(&mount).cloned())
            .map_err(|_| SharedLogError::Backend("checkpoint catalog mutex poisoned".to_owned()))
    }
}

fn checkpoint_is_newer(next: &CheckpointManifest, current: &CheckpointManifest) -> bool {
    (
        next.frontier.applied_position.term,
        next.frontier.applied_position.index,
    ) >= (
        current.frontier.applied_position.term,
        current.frontier.applied_position.index,
    )
}
