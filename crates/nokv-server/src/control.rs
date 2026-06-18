use std::sync::Arc;
use std::time::Duration;

use nokv_control::{
    CheckpointRef, ControlStore, LogRef, NodeId, ShardId, ShardLease, ShardRecord, ShardState,
};
use nokv_meta::{MetadataStore, NoKvFs};
use nokv_object::ObjectStore;

use crate::server::ServerError;

const DEFAULT_SHARD_OWNER_RENEWAL_INTERVAL: Duration = Duration::from_secs(5);
/// Default lease TTL the owner self-fences against. Must be `<=` the control
/// plane's own lease TTL so the local deadline never outlives the control
/// plane's expiry (matches the etcd backend's default TTL).
const DEFAULT_SHARD_LEASE_TTL: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerShardOwnerOptions {
    pub shard_id: ShardId,
    pub node_id: NodeId,
    pub acquisition: ServerShardAcquisition,
    pub renewal: Option<ServerShardOwnerRenewalOptions>,
    pub shared_log: Option<ServerSharedLogOptions>,
    /// Stable shard index to register for this shard before acquiring, when this
    /// owner is responsible for declaring its identity. `None` adopts whatever
    /// index the control record already carries (the in-process fleet path, where
    /// a separate `register_shard` step seeds identity; and the single default
    /// shard, which is index 0). A multi-process etcd fleet sets this so each
    /// process declares its own non-default shard index. The shard's path prefix
    /// is derived from `shard_id` by the control store.
    pub shard_index: Option<u16>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerShardAcquisition {
    Fresh,
    Failover { previous_epoch: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerShardOwnerRenewalOptions {
    pub interval: Duration,
    pub run_immediately: bool,
    /// TTL used to arm the owner's wall-clock self-fence. The deadline is
    /// refreshed to `renew_start + lease_ttl` on every successful renewal, so an
    /// owner that loses contact with the control plane stops committing here.
    pub lease_ttl: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerSharedLogOptions {
    pub archive_prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerShardOwnerState {
    pub shard_id: ShardId,
    pub node_id: NodeId,
    pub epoch: u64,
    pub lease_id: u64,
    pub state: ShardState,
    pub checkpoint: Option<CheckpointRef>,
    pub log: Option<LogRef>,
    pub durable_lsn: u64,
}

#[derive(Clone)]
pub(crate) struct ServerShardOwner {
    store: Arc<dyn ControlStore>,
    lease: ShardLease,
    /// Lease TTL (ms) for the wall-clock self-fence; `None` when auto-renewal is
    /// disabled (manual/test owners and single-node dev, which keep the fence
    /// off and rely on the epoch fence alone).
    lease_ttl_ms: Option<u64>,
}

impl Default for ServerShardOwnerRenewalOptions {
    fn default() -> Self {
        Self {
            interval: DEFAULT_SHARD_OWNER_RENEWAL_INTERVAL,
            run_immediately: false,
            lease_ttl: DEFAULT_SHARD_LEASE_TTL,
        }
    }
}

impl ServerShardOwnerRenewalOptions {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            run_immediately: false,
            lease_ttl: DEFAULT_SHARD_LEASE_TTL,
        }
    }
}

impl ServerShardOwnerOptions {
    pub fn fresh(shard_id: impl Into<String>, node_id: impl Into<String>) -> Self {
        Self {
            shard_id: ShardId::new(shard_id),
            node_id: NodeId::new(node_id),
            acquisition: ServerShardAcquisition::Fresh,
            renewal: Some(ServerShardOwnerRenewalOptions::default()),
            shared_log: None,
            shard_index: None,
        }
    }

    pub fn failover(
        shard_id: impl Into<String>,
        node_id: impl Into<String>,
        previous_epoch: u64,
    ) -> Self {
        Self {
            shard_id: ShardId::new(shard_id),
            node_id: NodeId::new(node_id),
            acquisition: ServerShardAcquisition::Failover { previous_epoch },
            renewal: Some(ServerShardOwnerRenewalOptions::default()),
            shared_log: None,
            shard_index: None,
        }
    }

    pub fn with_renewal(mut self, renewal: Option<ServerShardOwnerRenewalOptions>) -> Self {
        self.renewal = renewal;
        self
    }

    /// Declare the stable shard index this owner registers before acquiring. Used
    /// by a multi-process fleet so each process seeds its own shard identity.
    pub fn with_shard_index(mut self, shard_index: Option<u16>) -> Self {
        self.shard_index = shard_index;
        self
    }

    pub fn with_shared_log(mut self, shared_log: Option<ServerSharedLogOptions>) -> Self {
        self.shared_log = shared_log;
        self
    }
}

impl ServerSharedLogOptions {
    pub fn new(archive_prefix: impl Into<String>) -> Self {
        Self {
            archive_prefix: archive_prefix.into(),
        }
    }
}

impl ServerShardOwner {
    pub(crate) fn acquire<M, O>(
        store: Arc<dyn ControlStore>,
        options: ServerShardOwnerOptions,
        service: &NoKvFs<M, O>,
    ) -> Result<Self, ServerError>
    where
        M: MetadataStore,
        O: ObjectStore,
    {
        // Only arm the wall-clock self-fence when auto-renewal is on; manual/test
        // owners (renewal = None) keep it disabled and rely on the epoch fence.
        let lease_ttl_ms = options
            .renewal
            .map(|renewal| renewal.lease_ttl.as_millis() as u64)
            .filter(|ms| *ms > 0);
        let basis_ms = service.now_ms();
        let lease = match options.acquisition {
            ServerShardAcquisition::Fresh => {
                store.acquire_unassigned(options.shard_id, options.node_id)?
            }
            ServerShardAcquisition::Failover { previous_epoch } => {
                store.acquire_after_failure(options.shard_id, options.node_id, previous_epoch)?
            }
        };
        service.install_owner_epoch(lease.epoch)?;
        if let Some(ttl) = lease_ttl_ms {
            service.set_lease_deadline(basis_ms.saturating_add(ttl));
        }
        Ok(Self {
            store,
            lease,
            lease_ttl_ms,
        })
    }

    pub(crate) fn mark_serving<M, O>(
        &self,
        service: &NoKvFs<M, O>,
    ) -> Result<ServerShardOwnerState, ServerError>
    where
        M: MetadataStore,
        O: ObjectStore,
    {
        self.mark_serving_with_recovery_refs(service, None, None, 0)
    }

    pub(crate) fn mark_serving_with_recovery_refs<M, O>(
        &self,
        service: &NoKvFs<M, O>,
        checkpoint: Option<CheckpointRef>,
        log: Option<LogRef>,
        durable_lsn: u64,
    ) -> Result<ServerShardOwnerState, ServerError>
    where
        M: MetadataStore,
        O: ObjectStore,
    {
        let basis_ms = service.now_ms();
        let record = self
            .store
            .mark_serving(&self.lease, checkpoint, log, durable_lsn)?;
        service.install_owner_epoch(record.epoch)?;
        if let Some(ttl) = self.lease_ttl_ms {
            service.set_lease_deadline(basis_ms.saturating_add(ttl));
        }
        Ok(owner_state(&self.lease, &record))
    }

    pub(crate) fn renew<M, O>(
        &self,
        service: &NoKvFs<M, O>,
    ) -> Result<ServerShardOwnerState, ServerError>
    where
        M: MetadataStore,
        O: ObjectStore,
    {
        // Capture the deadline basis BEFORE the round-trip so a slow renew never
        // pushes the local deadline past the control plane's real lease expiry.
        let basis_ms = service.now_ms();
        match self.store.renew(&self.lease) {
            Ok(record) => {
                service.install_owner_epoch(record.epoch)?;
                if let Some(ttl) = self.lease_ttl_ms {
                    service.set_lease_deadline(basis_ms.saturating_add(ttl));
                }
                Ok(owner_state(&self.lease, &record))
            }
            Err(err) => {
                // Best-effort: observe a bumped epoch if the control plane is
                // reachable. If it is NOT reachable, the lease deadline armed on
                // the last successful renew still fences this owner once it
                // passes, so a partitioned owner cannot keep committing.
                if let Ok(record) = self.store.get_shard(&self.lease.shard_id) {
                    service.observe_required_owner_epoch(record.epoch)?;
                }
                Err(err.into())
            }
        }
    }

    pub(crate) fn state(&self) -> Result<ServerShardOwnerState, ServerError> {
        let record = self.store.get_shard(&self.lease.shard_id)?;
        Ok(owner_state(&self.lease, &record))
    }

    /// Relinquish ownership so a standby can acquire immediately instead of
    /// waiting out the lease TTL. Used on graceful shutdown.
    pub(crate) fn release(&self) -> Result<(), ServerError> {
        self.store.release(&self.lease)?;
        Ok(())
    }
}

fn owner_state(lease: &ShardLease, record: &ShardRecord) -> ServerShardOwnerState {
    ServerShardOwnerState {
        shard_id: lease.shard_id.clone(),
        node_id: lease.owner.clone(),
        epoch: lease.epoch,
        lease_id: lease.lease_id,
        state: record.state,
        checkpoint: record.checkpoint.clone(),
        log: record.log.clone(),
        durable_lsn: record.durable_lsn,
    }
}
