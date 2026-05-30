//! Tonic services for the Rust raftstore data plane.
//!
//! This crate owns the external gRPC boundary. It keeps the existing NoKV
//! protobuf contract intact while the Rust state-machine and replication layers
//! are brought up behind the service.

use std::net::SocketAddr;

use nokv_mvcc::{KvEngine, MvccStore};
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::kv::v1 as kvpb;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub use adminpb::raft_admin_server::RaftAdminServer;
pub use kvpb::store_kv_server::StoreKvServer;

#[derive(Debug, Clone, Default)]
pub struct StoreKvService<E = MvccStore> {
    engine: E,
}

impl<E> StoreKvService<E> {
    pub fn new(engine: E) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl<E> kvpb::store_kv_server::StoreKv for StoreKvService<E>
where
    E: KvEngine,
{
    async fn get(
        &self,
        request: Request<kvpb::KvGetRequest>,
    ) -> Result<Response<kvpb::KvGetResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self.engine.get(&inner).map_err(internal_error)?;
        Ok(Response::new(kvpb::KvGetResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn batch_get(
        &self,
        request: Request<kvpb::KvBatchGetRequest>,
    ) -> Result<Response<kvpb::KvBatchGetResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self.engine.batch_get(&inner).map_err(internal_error)?;
        Ok(Response::new(kvpb::KvBatchGetResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn scan(
        &self,
        request: Request<kvpb::KvScanRequest>,
    ) -> Result<Response<kvpb::KvScanResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self.engine.scan(&inner).map_err(internal_error)?;
        Ok(Response::new(kvpb::KvScanResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn prewrite(
        &self,
        request: Request<kvpb::KvPrewriteRequest>,
    ) -> Result<Response<kvpb::KvPrewriteResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self.engine.prewrite(&inner).map_err(internal_error)?;
        Ok(Response::new(kvpb::KvPrewriteResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn commit(
        &self,
        request: Request<kvpb::KvCommitRequest>,
    ) -> Result<Response<kvpb::KvCommitResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self.engine.commit(&inner).map_err(internal_error)?;
        Ok(Response::new(kvpb::KvCommitResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn batch_rollback(
        &self,
        request: Request<kvpb::KvBatchRollbackRequest>,
    ) -> Result<Response<kvpb::KvBatchRollbackResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self.engine.batch_rollback(&inner).map_err(internal_error)?;
        Ok(Response::new(kvpb::KvBatchRollbackResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn resolve_lock(
        &self,
        request: Request<kvpb::KvResolveLockRequest>,
    ) -> Result<Response<kvpb::KvResolveLockResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self.engine.resolve_lock(&inner).map_err(internal_error)?;
        Ok(Response::new(kvpb::KvResolveLockResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn check_txn_status(
        &self,
        request: Request<kvpb::KvCheckTxnStatusRequest>,
    ) -> Result<Response<kvpb::KvCheckTxnStatusResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self
            .engine
            .check_txn_status(&inner)
            .map_err(internal_error)?;
        Ok(Response::new(kvpb::KvCheckTxnStatusResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn txn_heart_beat(
        &self,
        request: Request<kvpb::KvTxnHeartBeatRequest>,
    ) -> Result<Response<kvpb::KvTxnHeartBeatResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self.engine.txn_heartbeat(&inner).map_err(internal_error)?;
        Ok(Response::new(kvpb::KvTxnHeartBeatResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn try_atomic_mutate(
        &self,
        request: Request<kvpb::KvTryAtomicMutateRequest>,
    ) -> Result<Response<kvpb::KvTryAtomicMutateResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self
            .engine
            .try_atomic_mutate(&inner)
            .map_err(internal_error)?;
        Ok(Response::new(kvpb::KvTryAtomicMutateResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    async fn install_prepared_mvcc_entries(
        &self,
        request: Request<kvpb::KvInstallPreparedMvccEntriesRequest>,
    ) -> Result<Response<kvpb::KvInstallPreparedMvccEntriesResponse>, Status> {
        let inner = request.into_inner().request.unwrap_or_default();
        let response = self
            .engine
            .install_prepared(&inner)
            .map_err(internal_error)?;
        Ok(Response::new(kvpb::KvInstallPreparedMvccEntriesResponse {
            response: Some(response),
            region_error: None,
        }))
    }

    type WatchApplyStream = ReceiverStream<Result<kvpb::ApplyWatchResponse, Status>>;

    async fn watch_apply(
        &self,
        _request: Request<kvpb::ApplyWatchRequest>,
    ) -> Result<Response<Self::WatchApplyStream>, Status> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Debug, Clone, Default)]
pub struct RaftAdminService;

#[tonic::async_trait]
impl adminpb::raft_admin_server::RaftAdmin for RaftAdminService {
    async fn add_peer(
        &self,
        _request: Request<adminpb::AddPeerRequest>,
    ) -> Result<Response<adminpb::AddPeerResponse>, Status> {
        Err(Status::unimplemented(
            "rust raft membership is not wired yet",
        ))
    }

    async fn remove_peer(
        &self,
        _request: Request<adminpb::RemovePeerRequest>,
    ) -> Result<Response<adminpb::RemovePeerResponse>, Status> {
        Err(Status::unimplemented(
            "rust raft membership is not wired yet",
        ))
    }

    async fn transfer_leader(
        &self,
        _request: Request<adminpb::TransferLeaderRequest>,
    ) -> Result<Response<adminpb::TransferLeaderResponse>, Status> {
        Err(Status::unimplemented(
            "rust raft leadership is not wired yet",
        ))
    }

    async fn region_runtime_status(
        &self,
        _request: Request<adminpb::RegionRuntimeStatusRequest>,
    ) -> Result<Response<adminpb::RegionRuntimeStatusResponse>, Status> {
        Ok(Response::new(
            adminpb::RegionRuntimeStatusResponse::default(),
        ))
    }

    async fn execution_status(
        &self,
        _request: Request<adminpb::ExecutionStatusRequest>,
    ) -> Result<Response<adminpb::ExecutionStatusResponse>, Status> {
        Ok(Response::new(adminpb::ExecutionStatusResponse::default()))
    }
}

pub async fn serve(addr: SocketAddr, mvcc: MvccStore) -> Result<(), tonic::transport::Error> {
    serve_with_engine(addr, mvcc).await
}

pub async fn serve_with_engine<E>(
    addr: SocketAddr,
    engine: E,
) -> Result<(), tonic::transport::Error>
where
    E: KvEngine,
{
    tonic::transport::Server::builder()
        .add_service(StoreKvServer::new(StoreKvService::new(engine)))
        .add_service(RaftAdminServer::new(RaftAdminService))
        .serve(addr)
        .await
}

fn internal_error(err: nokv_mvcc::Error) -> Status {
    Status::internal(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use adminpb::raft_admin_server::RaftAdmin;
    use kvpb::store_kv_server::StoreKv;

    #[tokio::test]
    async fn get_returns_not_found_from_empty_store() {
        let service = StoreKvService::new(MvccStore::new());
        let response = service
            .get(Request::new(kvpb::KvGetRequest {
                request: Some(kvpb::GetRequest {
                    key: b"missing".to_vec(),
                    version: 1,
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.response.unwrap().not_found);
    }

    #[tokio::test]
    async fn service_can_run_against_holt_mvcc_engine() {
        let service = StoreKvService::new(nokv_holtstore::HoltMvccStore::open_memory().unwrap());
        let response = service
            .try_atomic_mutate(Request::new(kvpb::KvTryAtomicMutateRequest {
                request: Some(kvpb::TryAtomicMutateRequest {
                    mutations: vec![kvpb::Mutation {
                        key: b"k".to_vec(),
                        value: b"v".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    }],
                    commit_version: 2,
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.response.unwrap().applied_keys, 1);
    }

    #[tokio::test]
    async fn admin_membership_is_explicitly_not_wired() {
        let service = RaftAdminService;
        let err = service
            .add_peer(Request::new(adminpb::AddPeerRequest {
                region_id: 1,
                store_id: 1,
                peer_id: 1,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }
}
