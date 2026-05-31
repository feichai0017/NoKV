//! Multi-region MetadataPlane/RaftAdmin routing for a single Rust raftstore process.
//!
//! The main server path routes metadata-native requests and admin calls.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_raftnode::{ApplyWatchProvider, MetadataCommandExecutor, MetadataReadExecutor};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status};

use crate::{
    push_missing_topology_status, AppliedRegionDescriptorProvider, EmptyRegionDescriptorSink,
    EmptyRestartDiagnostics, MetadataPlaneService, RaftAdminService, RaftMembershipAdmin,
    RaftRuntimeStatusProvider, RegionDescriptorSink, RestartDiagnosticsProvider,
    DEFAULT_APPLY_WATCH_BUFFER,
};

enum RegionServiceLookup<T> {
    Hosted(T),
    Missing(errorpb::RegionError),
}

#[derive(Clone)]
struct RegionServiceRegistry<T> {
    regions: Arc<RwLock<BTreeMap<u64, T>>>,
}

impl<T> RegionServiceRegistry<T> {
    fn new(regions: impl IntoIterator<Item = (u64, T)>) -> Result<Self, Status> {
        let registry = Self {
            regions: Arc::new(RwLock::new(BTreeMap::new())),
        };
        for (region_id, service) in regions {
            registry.insert_region(region_id, service)?;
        }
        Ok(registry)
    }

    fn insert_region(&self, region_id: u64, service: T) -> Result<(), Status> {
        validate_region_service_id(region_id)?;
        let mut regions = self
            .regions
            .write()
            .map_err(|_| region_service_registry_poisoned())?;
        if regions.insert(region_id, service).is_some() {
            return Err(Status::invalid_argument(format!(
                "duplicate region_id {region_id}"
            )));
        }
        Ok(())
    }

    fn remove_region(&self, region_id: u64) -> Result<Option<T>, Status> {
        validate_region_service_id(region_id)?;
        self.regions
            .write()
            .map_err(|_| region_service_registry_poisoned())
            .map(|mut regions| regions.remove(&region_id))
    }

    fn get_region(&self, region_id: u64) -> Result<Option<T>, Status>
    where
        T: Clone,
    {
        self.regions
            .read()
            .map_err(|_| region_service_registry_poisoned())
            .map(|regions| regions.get(&region_id).cloned())
    }

    fn values(&self) -> Result<Vec<T>, Status>
    where
        T: Clone,
    {
        self.regions
            .read()
            .map_err(|_| region_service_registry_poisoned())
            .map(|regions| regions.values().cloned().collect())
    }

    fn is_empty(&self) -> Result<bool, Status> {
        self.regions
            .read()
            .map_err(|_| region_service_registry_poisoned())
            .map(|regions| regions.is_empty())
    }
}

fn region_service_registry_poisoned() -> Status {
    Status::internal("region service registry lock poisoned")
}

fn metadata_region_lookup<T>(
    regions: &RegionServiceRegistry<T>,
    context: Option<&metadatapb::MetadataContext>,
) -> Result<RegionServiceLookup<T>, Status>
where
    T: Clone,
{
    let context = context.ok_or_else(|| Status::invalid_argument("context is required"))?;
    if context.region_id == 0 {
        return Err(Status::invalid_argument("region id is required"));
    }
    match regions.get_region(context.region_id)? {
        Some(region) => Ok(RegionServiceLookup::Hosted(region)),
        None => Ok(RegionServiceLookup::Missing(region_not_found_error(
            context.region_id,
        ))),
    }
}

fn admin_region_lookup<T>(regions: &RegionServiceRegistry<T>, region_id: u64) -> Result<T, Status>
where
    T: Clone,
{
    regions.get_region(region_id)?.ok_or_else(|| {
        Status::failed_precondition(format!(
            "region {region_id} is not hosted by this raft admin"
        ))
    })
}

fn region_not_found_error(region_id: u64) -> errorpb::RegionError {
    errorpb::RegionError {
        region_not_found: Some(errorpb::RegionNotFound { region_id }),
        ..Default::default()
    }
}

fn validate_region_service_id(region_id: u64) -> Result<(), Status> {
    if region_id == 0 {
        return Err(Status::invalid_argument("region_id is required"));
    }
    Ok(())
}

#[derive(Clone)]
pub struct MultiRegionMetadataPlaneService<E> {
    regions: RegionServiceRegistry<MetadataPlaneService<E>>,
}

impl<E> MultiRegionMetadataPlaneService<E> {
    pub fn new(
        regions: impl IntoIterator<Item = (u64, MetadataPlaneService<E>)>,
    ) -> Result<Self, Status> {
        Ok(Self {
            regions: RegionServiceRegistry::new(regions)?,
        })
    }

    pub fn insert_region(
        &self,
        region_id: u64,
        service: MetadataPlaneService<E>,
    ) -> Result<(), Status> {
        self.regions.insert_region(region_id, service)
    }

    pub fn remove_region(&self, region_id: u64) -> Result<Option<MetadataPlaneService<E>>, Status> {
        self.regions.remove_region(region_id)
    }

    fn service_for_context(
        &self,
        context: Option<&metadatapb::MetadataContext>,
    ) -> Result<RegionServiceLookup<MetadataPlaneService<E>>, Status>
    where
        MetadataPlaneService<E>: Clone,
    {
        metadata_region_lookup(&self.regions, context)
    }
}

macro_rules! delegate_metadata_request {
    ($self:expr, $request:expr, $method:ident, $response_type:ident) => {{
        let request = $request.into_inner();
        match $self.service_for_context(request.context.as_ref())? {
            RegionServiceLookup::Hosted(service) => service.$method(Request::new(request)).await,
            RegionServiceLookup::Missing(region_error) => {
                Ok(Response::new(metadatapb::$response_type {
                    region_error: Some(region_error),
                    ..Default::default()
                }))
            }
        }
    }};
}

#[tonic::async_trait]
impl<E> metadatapb::metadata_plane_server::MetadataPlane for MultiRegionMetadataPlaneService<E>
where
    E: AppliedRegionDescriptorProvider
        + ApplyWatchProvider
        + MetadataCommandExecutor
        + MetadataReadExecutor
        + RaftRuntimeStatusProvider,
{
    async fn get(
        &self,
        request: Request<metadatapb::MetadataGetRequest>,
    ) -> Result<Response<metadatapb::MetadataGetResponse>, Status> {
        delegate_metadata_request!(self, request, get, MetadataGetResponse)
    }

    async fn batch_get(
        &self,
        request: Request<metadatapb::MetadataBatchGetRequest>,
    ) -> Result<Response<metadatapb::MetadataBatchGetResponse>, Status> {
        delegate_metadata_request!(self, request, batch_get, MetadataBatchGetResponse)
    }

    async fn scan(
        &self,
        request: Request<metadatapb::MetadataScanRequest>,
    ) -> Result<Response<metadatapb::MetadataScanResponse>, Status> {
        delegate_metadata_request!(self, request, scan, MetadataScanResponse)
    }

    async fn commit_metadata(
        &self,
        request: Request<metadatapb::MetadataCommitRequest>,
    ) -> Result<Response<metadatapb::MetadataCommitResponse>, Status> {
        delegate_metadata_request!(self, request, commit_metadata, MetadataCommitResponse)
    }

    type WatchApplyStream = ReceiverStream<Result<metadatapb::MetadataWatchApplyResponse, Status>>;

    async fn watch_apply(
        &self,
        request: Request<metadatapb::MetadataWatchApplyRequest>,
    ) -> Result<Response<Self::WatchApplyStream>, Status> {
        let request = request.into_inner();
        let buffer = if request.buffer == 0 {
            DEFAULT_APPLY_WATCH_BUFFER
        } else {
            request.buffer as usize
        };
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        for service in self.regions.values()? {
            let tx = tx.clone();
            let request = request.clone();
            tokio::spawn(async move {
                let response = service.watch_apply(Request::new(request)).await;
                let mut stream = match response {
                    Ok(response) => response.into_inner(),
                    Err(err) => {
                        let _ = tx.send(Err(err)).await;
                        return;
                    }
                };
                while let Some(item) = stream.next().await {
                    if tx.send(item).await.is_err() {
                        return;
                    }
                }
            });
        }
        drop(tx);
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// RaftAdmin service router for a process that hosts more than one region.
///
/// Region-scoped admin requests are routed by the request `region_id`.
/// Process-scoped diagnostics are aggregated across hosted regions so the
/// existing `ExecutionStatus` response can describe multi-region readiness
/// without changing the protobuf contract.
#[derive(Clone)]
pub struct MultiRegionRaftAdminService<S, D = EmptyRegionDescriptorSink> {
    regions: RegionServiceRegistry<RaftAdminService<S, D>>,
    restart_diagnostics: Arc<dyn RestartDiagnosticsProvider>,
}

impl<S, D> MultiRegionRaftAdminService<S, D> {
    pub fn new(
        regions: impl IntoIterator<Item = (u64, RaftAdminService<S, D>)>,
    ) -> Result<Self, Status> {
        Ok(Self {
            regions: RegionServiceRegistry::new(regions)?,
            restart_diagnostics: Arc::new(EmptyRestartDiagnostics),
        })
    }

    pub fn insert_region(
        &self,
        region_id: u64,
        service: RaftAdminService<S, D>,
    ) -> Result<(), Status> {
        self.regions.insert_region(region_id, service)
    }

    pub fn remove_region(&self, region_id: u64) -> Result<Option<RaftAdminService<S, D>>, Status> {
        self.regions.remove_region(region_id)
    }

    pub fn with_restart_diagnostics(
        mut self,
        restart_diagnostics: Arc<dyn RestartDiagnosticsProvider>,
    ) -> Self {
        self.restart_diagnostics = restart_diagnostics;
        self
    }

    fn service_for_region(&self, region_id: u64) -> Result<RaftAdminService<S, D>, Status>
    where
        RaftAdminService<S, D>: Clone,
    {
        admin_region_lookup(&self.regions, region_id)
    }
}

#[tonic::async_trait]
impl<S, D> adminpb::raft_admin_server::RaftAdmin for MultiRegionRaftAdminService<S, D>
where
    S: AppliedRegionDescriptorProvider + RaftMembershipAdmin + RaftRuntimeStatusProvider,
    D: RegionDescriptorSink,
{
    async fn add_peer(
        &self,
        request: Request<adminpb::AddPeerRequest>,
    ) -> Result<Response<adminpb::AddPeerResponse>, Status> {
        let request = request.into_inner();
        if request.region_id == 0 || request.store_id == 0 || request.peer_id == 0 {
            return Err(Status::invalid_argument(
                "region_id, store_id, and peer_id are required",
            ));
        }
        self.service_for_region(request.region_id)?
            .add_peer(Request::new(request))
            .await
    }

    async fn remove_peer(
        &self,
        request: Request<adminpb::RemovePeerRequest>,
    ) -> Result<Response<adminpb::RemovePeerResponse>, Status> {
        let request = request.into_inner();
        if request.region_id == 0 || request.peer_id == 0 {
            return Err(Status::invalid_argument(
                "region_id and peer_id are required",
            ));
        }
        self.service_for_region(request.region_id)?
            .remove_peer(Request::new(request))
            .await
    }

    async fn transfer_leader(
        &self,
        request: Request<adminpb::TransferLeaderRequest>,
    ) -> Result<Response<adminpb::TransferLeaderResponse>, Status> {
        let request = request.into_inner();
        if request.region_id == 0 || request.peer_id == 0 {
            return Err(Status::invalid_argument(
                "region_id and peer_id are required",
            ));
        }
        self.service_for_region(request.region_id)?
            .transfer_leader(Request::new(request))
            .await
    }

    async fn region_runtime_status(
        &self,
        request: Request<adminpb::RegionRuntimeStatusRequest>,
    ) -> Result<Response<adminpb::RegionRuntimeStatusResponse>, Status> {
        let request = request.into_inner();
        if request.region_id == 0 {
            return Err(Status::invalid_argument("region_id is required"));
        }
        let Some(service) = self.regions.get_region(request.region_id)? else {
            return Ok(Response::new(
                adminpb::RegionRuntimeStatusResponse::default(),
            ));
        };
        service.region_runtime_status(Request::new(request)).await
    }

    async fn execution_status(
        &self,
        _request: Request<adminpb::ExecutionStatusRequest>,
    ) -> Result<Response<adminpb::ExecutionStatusResponse>, Status> {
        let mut last_admission = None;
        let mut restart = adminpb::ExecutionRestartStatus {
            state: adminpb::ExecutionRestartState::Ready as i32,
            ..Default::default()
        };
        let mut topology = Vec::new();

        for service in self.regions.values()? {
            let status = service.status.apply_status();
            let runtime = service.status.raft_runtime_status();
            let hosted = status.region_id != 0 && runtime.hosted;
            if hosted {
                restart.region_count += 1;
                restart.raft_group_count += 1;
            } else {
                restart.state = adminpb::ExecutionRestartState::Degraded as i32;
            }
            merge_last_admission(&mut last_admission, Some(service.execution.snapshot()?));
            for status in service.execution.topology_snapshot()? {
                push_missing_topology_status(&mut topology, status);
            }
        }
        if self.regions.is_empty()? {
            restart.state = adminpb::ExecutionRestartState::Degraded as i32;
        }
        restart.pending_root_event_count = self.restart_diagnostics.pending_root_event_count()?;
        restart.blocked_root_event_count = self.restart_diagnostics.blocked_root_event_count()?;
        restart.pending_scheduler_operation_count = self
            .restart_diagnostics
            .pending_scheduler_operation_count()?;
        for blocked in self.restart_diagnostics.blocked_topology_statuses()? {
            push_missing_topology_status(&mut topology, blocked);
        }
        for pending in self.restart_diagnostics.pending_topology_statuses()? {
            push_missing_topology_status(&mut topology, pending);
        }
        Ok(Response::new(adminpb::ExecutionStatusResponse {
            last_admission,
            restart: Some(restart),
            topology,
        }))
    }
}

fn merge_last_admission(
    current: &mut Option<adminpb::ExecutionAdmissionStatus>,
    next: Option<adminpb::ExecutionAdmissionStatus>,
) {
    let Some(next) = next else {
        return;
    };
    if !next.observed {
        return;
    }
    let replace = current
        .as_ref()
        .map(|current| !current.observed || current.at_unix_nano <= next.at_unix_nano)
        .unwrap_or(true);
    if replace {
        *current = Some(next);
    }
}

pub async fn serve_with_metadata_region_services<E, S, D>(
    addr: SocketAddr,
    metadata_service: MultiRegionMetadataPlaneService<E>,
    admin_service: MultiRegionRaftAdminService<S, D>,
    transport: nokv_raftnode::TonicRaftTransportRegistry,
) -> Result<(), tonic::transport::Error>
where
    E: AppliedRegionDescriptorProvider
        + ApplyWatchProvider
        + MetadataCommandExecutor
        + MetadataReadExecutor
        + RaftRuntimeStatusProvider,
    S: AppliedRegionDescriptorProvider + RaftMembershipAdmin + RaftRuntimeStatusProvider,
    D: RegionDescriptorSink,
{
    tonic::transport::Server::builder()
        .add_service(crate::MetadataPlaneServer::new(metadata_service))
        .add_service(crate::RaftAdminServer::new(admin_service))
        .add_service(nokv_raftnode::RaftTransportServer::new(transport.service()))
        .serve(addr)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission_state::RegionAdmissionState;
    use crate::execution::ExecutionRuntime;
    use crate::{RaftRuntimeStatus, RegionAdmission};
    use nokv_metastore::MemoryMetadataStore;
    use nokv_proto::nokv::admin::v1::raft_admin_server::RaftAdmin;
    use nokv_proto::nokv::meta::v1 as metapb;
    use nokv_proto::nokv::metadata::v1::metadata_plane_server::MetadataPlane;
    use nokv_raftnode::{ApplyStatusProvider, BasicNode};

    #[derive(Debug, Clone)]
    struct FixedRuntimeEngine {
        inner: nokv_raftnode::AppliedMetadataEngine<MemoryMetadataStore>,
        runtime: RaftRuntimeStatus,
    }

    impl FixedRuntimeEngine {
        fn leader(region_id: u64, local_peer_id: u64) -> Self {
            Self {
                inner: nokv_raftnode::AppliedMetadataEngine::new(
                    region_id,
                    MemoryMetadataStore::new(),
                ),
                runtime: RaftRuntimeStatus {
                    local_peer_id,
                    leader_peer_id: local_peer_id,
                    leader: true,
                    hosted: true,
                },
            }
        }

        fn set_region_descriptor(&self, descriptor: metapb::RegionDescriptor) {
            self.inner.set_region_descriptor(descriptor).unwrap();
        }
    }

    impl MetadataReadExecutor for FixedRuntimeEngine {
        fn execute_metadata_get<'a>(
            &'a self,
            req: &'a metadatapb::MetadataGetRequest,
        ) -> impl std::future::Future<Output = nokv_metastore::Result<metadatapb::MetadataGetResponse>>
               + Send
               + 'a {
            self.inner.execute_metadata_get(req)
        }

        fn execute_metadata_batch_get<'a>(
            &'a self,
            req: &'a metadatapb::MetadataBatchGetRequest,
        ) -> impl std::future::Future<
            Output = nokv_metastore::Result<metadatapb::MetadataBatchGetResponse>,
        > + Send
               + 'a {
            self.inner.execute_metadata_batch_get(req)
        }

        fn execute_metadata_scan<'a>(
            &'a self,
            req: &'a metadatapb::MetadataScanRequest,
        ) -> impl std::future::Future<
            Output = nokv_metastore::Result<metadatapb::MetadataScanResponse>,
        > + Send
               + 'a {
            self.inner.execute_metadata_scan(req)
        }
    }

    impl MetadataCommandExecutor for FixedRuntimeEngine {
        fn execute_metadata_command<'a>(
            &'a self,
            req: &'a metadatapb::MetadataCommitRequest,
        ) -> impl std::future::Future<
            Output = nokv_metastore::Result<metadatapb::MetadataCommitResponse>,
        > + Send
               + 'a {
            self.inner.execute_metadata_command(req)
        }
    }

    impl ApplyStatusProvider for FixedRuntimeEngine {
        fn apply_status(&self) -> nokv_raftnode::ApplyStatus {
            self.inner.apply_status()
        }
    }

    impl ApplyWatchProvider for FixedRuntimeEngine {
        fn subscribe_apply(
            &self,
        ) -> tokio::sync::broadcast::Receiver<metadatapb::MetadataApplyWatchEvent> {
            self.inner.subscribe_apply()
        }

        fn replay_apply(
            &self,
            request: nokv_raftnode::ApplyWatchReplayRequest,
        ) -> nokv_metastore::Result<nokv_raftnode::ApplyWatchReplay> {
            self.inner.replay_apply(request)
        }
    }

    impl RaftRuntimeStatusProvider for FixedRuntimeEngine {
        fn raft_runtime_status(&self) -> RaftRuntimeStatus {
            self.runtime
        }
    }

    impl AppliedRegionDescriptorProvider for FixedRuntimeEngine {
        fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
            self.inner
                .region_descriptor()
                .map_err(|err| Status::internal(err.to_string()))
        }
    }

    #[derive(Clone, Default)]
    struct NoopAdminStatus;

    impl ApplyStatusProvider for NoopAdminStatus {
        fn apply_status(&self) -> nokv_raftnode::ApplyStatus {
            nokv_raftnode::ApplyStatus {
                region_id: 1,
                term: 1,
                applied_index: 1,
            }
        }
    }

    impl RaftRuntimeStatusProvider for NoopAdminStatus {
        fn raft_runtime_status(&self) -> RaftRuntimeStatus {
            RaftRuntimeStatus {
                local_peer_id: 1,
                leader_peer_id: 1,
                leader: true,
                hosted: true,
            }
        }
    }

    impl AppliedRegionDescriptorProvider for NoopAdminStatus {}

    #[tonic::async_trait]
    impl RaftMembershipAdmin for NoopAdminStatus {
        async fn add_voter(&self, _peer_id: u64, _node: BasicNode) -> Result<(), Status> {
            Ok(())
        }

        async fn remove_voter(&self, _peer_id: u64) -> Result<(), Status> {
            Ok(())
        }

        async fn transfer_leader(&self, _peer_id: u64) -> Result<(), Status> {
            Ok(())
        }

        async fn propose_region_descriptor(
            &self,
            _descriptor: &metapb::RegionDescriptor,
        ) -> Result<(), Status> {
            Ok(())
        }
    }

    fn metadata_context(admission: &RegionAdmission) -> metadatapb::MetadataContext {
        metadatapb::MetadataContext {
            region_id: admission.region_id,
            region_epoch: Some(metapb::RegionEpoch {
                version: admission.epoch_version,
                conf_version: admission.epoch_conf_version,
            }),
            peer: Some(metapb::RegionPeer {
                store_id: admission.store_id,
                peer_id: admission.peer_id,
            }),
            ..Default::default()
        }
    }

    fn metadata_service(
        engine: FixedRuntimeEngine,
        admission: RegionAdmission,
    ) -> MetadataPlaneService<FixedRuntimeEngine> {
        MetadataPlaneService::with_admission_state_and_execution(
            engine,
            RegionAdmissionState::new(admission),
            ExecutionRuntime::default(),
        )
    }

    fn test_region_descriptor(
        region_id: u64,
        store_id: u64,
        peer_id: u64,
        start_key: &[u8],
        end_key: &[u8],
    ) -> metapb::RegionDescriptor {
        metapb::RegionDescriptor {
            region_id,
            start_key: start_key.to_vec(),
            end_key: end_key.to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 1,
                conf_version: 1,
            }),
            peers: vec![metapb::RegionPeer { store_id, peer_id }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn metadata_plane_routes_by_context_region() {
        let descriptor1 = test_region_descriptor(1, 1, 10, b"", b"m");
        let descriptor2 = test_region_descriptor(2, 1, 20, b"m", b"");
        let admission1 = RegionAdmission::from_descriptor(&descriptor1, true).unwrap();
        let admission2 = RegionAdmission::from_descriptor(&descriptor2, true).unwrap();
        let engine1 = FixedRuntimeEngine::leader(1, 10);
        let engine2 = FixedRuntimeEngine::leader(2, 20);
        engine1.set_region_descriptor(descriptor1);
        engine2.set_region_descriptor(descriptor2);
        let service = MultiRegionMetadataPlaneService::new([
            (1, metadata_service(engine1, admission1.clone())),
            (2, metadata_service(engine2, admission2.clone())),
        ])
        .unwrap();

        let key = b"m-artifact".to_vec();
        let get = service
            .get(Request::new(metadatapb::MetadataGetRequest {
                context: Some(metadata_context(&admission2)),
                key: key.clone(),
                version: 11,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(get.region_error.is_none());
        assert!(get.not_found);

        let wrong_region = service
            .get(Request::new(metadatapb::MetadataGetRequest {
                context: Some(metadata_context(&RegionAdmission {
                    region_id: 99,
                    store_id: 1,
                    peer_id: 99,
                    ..admission1
                })),
                key,
                version: 11,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            wrong_region
                .region_error
                .unwrap()
                .region_not_found
                .unwrap()
                .region_id,
            99
        );
    }

    #[tokio::test]
    async fn metadata_plane_routes_region_inserted_after_construction() {
        let descriptor1 = test_region_descriptor(1, 1, 10, b"", b"m");
        let descriptor2 = test_region_descriptor(2, 1, 20, b"m", b"");
        let admission1 = RegionAdmission::from_descriptor(&descriptor1, true).unwrap();
        let admission2 = RegionAdmission::from_descriptor(&descriptor2, true).unwrap();
        let engine1 = FixedRuntimeEngine::leader(1, 10);
        let engine2 = FixedRuntimeEngine::leader(2, 20);
        engine1.set_region_descriptor(descriptor1);
        engine2.set_region_descriptor(descriptor2);
        let service =
            MultiRegionMetadataPlaneService::new([(1, metadata_service(engine1, admission1))])
                .unwrap();

        service
            .insert_region(2, metadata_service(engine2, admission2.clone()))
            .unwrap();

        let response = service
            .get(Request::new(metadatapb::MetadataGetRequest {
                context: Some(metadata_context(&admission2)),
                key: b"m-dynamic".to_vec(),
                version: 21,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.region_error.is_none());
        assert!(response.not_found);

        let duplicate = service.insert_region(
            2,
            metadata_service(FixedRuntimeEngine::leader(2, 20), admission2.clone()),
        );
        assert_eq!(duplicate.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn admin_routes_region_scoped_membership() {
        let admission1 =
            RegionAdmission::from_descriptor(&test_region_descriptor(1, 1, 10, b"", b"m"), true)
                .unwrap();
        let admission2 =
            RegionAdmission::from_descriptor(&test_region_descriptor(2, 1, 20, b"m", b""), true)
                .unwrap();
        let service = MultiRegionRaftAdminService::new([
            (
                1,
                RaftAdminService::with_admission(NoopAdminStatus, admission1),
            ),
            (
                2,
                RaftAdminService::with_admission(NoopAdminStatus, admission2),
            ),
        ])
        .unwrap();

        let added = service
            .add_peer(Request::new(adminpb::AddPeerRequest {
                region_id: 2,
                store_id: 3,
                peer_id: 30,
            }))
            .await
            .unwrap()
            .into_inner()
            .region
            .unwrap();
        assert_eq!(added.region_id, 2);
        assert_eq!(added.epoch.unwrap().conf_version, 2);
        assert!(added
            .peers
            .iter()
            .any(|peer| peer.store_id == 3 && peer.peer_id == 30));

        let missing = service
            .remove_peer(Request::new(adminpb::RemovePeerRequest {
                region_id: 3,
                peer_id: 30,
            }))
            .await
            .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::FailedPrecondition);
        assert!(missing.message().contains("region 3"));
    }

    #[tokio::test]
    async fn admin_counts_region_inserted_after_construction() {
        let admission1 =
            RegionAdmission::from_descriptor(&test_region_descriptor(1, 1, 10, b"", b"m"), true)
                .unwrap();
        let admission2 =
            RegionAdmission::from_descriptor(&test_region_descriptor(2, 1, 20, b"m", b""), true)
                .unwrap();
        let service = MultiRegionRaftAdminService::new([(
            1,
            RaftAdminService::with_admission(NoopAdminStatus, admission1),
        )])
        .unwrap();

        service
            .insert_region(
                2,
                RaftAdminService::with_admission(NoopAdminStatus, admission2),
            )
            .unwrap();

        let status = service
            .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(status.restart.unwrap().region_count, 2);
    }

    #[tokio::test]
    async fn admin_execution_status_counts_process_diagnostics_once() {
        let admission1 =
            RegionAdmission::from_descriptor(&test_region_descriptor(1, 1, 10, b"", b"m"), true)
                .unwrap();
        let admission2 =
            RegionAdmission::from_descriptor(&test_region_descriptor(2, 1, 20, b"m", b""), true)
                .unwrap();
        let diagnostics = nokv_holtstore::HoltMetadataStore::open_memory().unwrap();
        diagnostics
            .enqueue_pending_root_event(&metapb::RootEvent {
                kind: metapb::RootEventKind::PeerAdded as i32,
                payload: Some(metapb::root_event::Payload::PeerChange(
                    metapb::RootPeerChange {
                        region_id: 2,
                        store_id: 3,
                        peer_id: 30,
                        ..Default::default()
                    },
                )),
            })
            .unwrap();
        let service = MultiRegionRaftAdminService::new([
            (
                1,
                RaftAdminService::with_admission(NoopAdminStatus, admission1),
            ),
            (
                2,
                RaftAdminService::with_admission(NoopAdminStatus, admission2),
            ),
        ])
        .unwrap()
        .with_restart_diagnostics(Arc::new(diagnostics));

        let status = service
            .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
            .await
            .unwrap()
            .into_inner();
        let restart = status.restart.unwrap();
        assert_eq!(restart.region_count, 2);
        assert_eq!(restart.raft_group_count, 2);
        assert_eq!(restart.pending_root_event_count, 1);
        assert_eq!(status.topology.len(), 1);
        assert_eq!(status.topology[0].transition_id, "peer:2:add:3:30");
    }
}
